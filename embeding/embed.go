/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package embeding

import (
	"math"
	"os"
	"path"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/worker"
	"github.com/wisdomatom/stateless-graph/x"
)

type EmbedDgraph struct {
	closer *z.Closer
}

type DgraphServer struct {
	Url            string   `toml:"url" mapstructure:"url"`
	Host           string   `toml:"host" mapstructure:"host"`
	Grpc           string   `toml:"grpc" mapstructure:"grpc"`
	AuthToken      string   `toml:"auth_token" mapstructure:"auth_token"`
	User           string   `toml:"user" mapstructure:"user"`
	Password       string   `toml:"password" mapstructure:"password"`
	Embed          bool     `toml:"embed" mapstructure:"embed"`
	EmbedDataDir   string   `toml:"embed_data_dir" mapstructure:"embed_data_dir"`
	TiKVAddrs      []string `toml:"tikv_addrs" mapstructure:"tikv_addrs"`
	UseFlatStorage bool     `toml:"use_flat_storage" mapstructure:"use_flat_storage"`
}

var (
	badgerTuning = "compression=snappy; numgoroutines=32;"
)

// NewDgraphEmbed initializes the embedded native Flat Storage + TiKV/Badger engine
// with zero gRPC, zero bufconn, and zero DQL server overhead.
func NewDgraphEmbed(conf DgraphServer) (*EmbedDgraph, error) {
	if len(conf.TiKVAddrs) == 0 && conf.EmbedDataDir != "" {
		if err := os.MkdirAll(conf.EmbedDataDir, 0750); err != nil {
			return nil, err
		}
	}

	x.WorkerConfig.HardSync = false
	x.WorkerConfig.TiKVAddrs = conf.TiKVAddrs
	x.WorkerConfig.Embedded = true
	x.WorkerConfig.UseFlatStorage = true

	if len(conf.TiKVAddrs) == 0 {
		worker.Config.PostingDir = conf.EmbedDataDir
	}
	worker.Config.TypeFilterUidLimit = 100000

	if x.WorkerConfig.TmpDir == "" {
		x.WorkerConfig.TmpDir = path.Join(os.TempDir(), "stateless-graph-tmp")
	}

	x.WorkerConfig.Badger = badger.DefaultOptions("").FromSuperFlag(badgerTuning).
		WithSyncWrites(false).
		WithInMemory(false).
		WithLoggingLevel(badger.ERROR)

	if runtime.GOOS == "windows" {
		x.WorkerConfig.Badger.ValueLogFileSize = 1024 * 1024 * 64
	}
	x.Config.MaxRetries = 10
	x.Config.Limit = z.NewSuperFlag("max-pending-queries=1000000")
	x.Config.LimitNormalizeNode = 1
	x.Config.LimitMutationsNquad = 2000000
	x.Config.LimitQueryEdge = 10000000

	worker.InitServerState()
	worker.InitForLite(worker.State.Pstore)
	worker.SetMaxUID(math.MaxUint64)
	worker.InitTasks()
	worker.Init(worker.State.Pstore)
	schema.Init(worker.State.Pstore)
	worker.StartRaftNodes(false)
	x.UpdateHealthStatus(true)

	return &EmbedDgraph{}, nil
}

func (r *EmbedDgraph) Close() {
	if r.closer != nil {
		r.closer.Signal()
	}
}

// UidLeaseInfo combines global storage UID lease state with local in-memory allocator state.
type UidLeaseInfo struct {
	StorageMaxUid     uint64
	CurrentUid        uint64
	LocalMaxUid       uint64
	LocalRemaining    uint64
	HasPrefetchedNext bool
	PrefetchedStart   uint64
	PrefetchedEnd     uint64
}

// GetUidLeaseInfo returns the global leased max UID from TiKV/storage and the local allocator snapshot.
func GetUidLeaseInfo() (*UidLeaseInfo, error) {
	storageMax, err := worker.GetStorageMaxLeasedUid()
	if err != nil {
		return nil, err
	}
	snap := x.GetUidLeaseSnapshot()
	return &UidLeaseInfo{
		StorageMaxUid:     storageMax,
		CurrentUid:        snap.CurrentUid,
		LocalMaxUid:       snap.LocalMaxUid,
		LocalRemaining:    snap.LocalRemaining,
		HasPrefetchedNext: snap.HasPrefetchedNext,
		PrefetchedStart:   snap.PrefetchedStart,
		PrefetchedEnd:     snap.PrefetchedEnd,
	}, nil
}
