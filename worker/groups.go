/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/wisdomatom/stateless-graph/conn"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

type groupi struct {
	x.SafeMutex
	state        *pb.MembershipState
	Node         *node
	gid          uint32
	tablets      map[string]*pb.Tablet
	triggerCh    chan struct{} // Used to trigger membership sync
	blockDeletes *sync.Mutex   // Ensure that deletion won't happen when move is going on.
	closer       *z.Closer

	// Group checksum is used to determine if the tablets served by the groups have changed from
	// the membership information that the Alpha has. If so, Alpha cannot service a read.
	deltaChecksum      uint64 // Checksum received by OracleDelta.
	membershipChecksum uint64 // Checksum received by MembershipState.
}

var gr = &groupi{
	blockDeletes: new(sync.Mutex),
	tablets:      make(map[string]*pb.Tablet),
	closer:       z.NewCloser(3), // Match CLOSER:1 in this file.
}

func groups() *groupi {
	return gr
}

// StartRaftNodes will read the WAL dir, create the RAFT groups,
// and either start or restart RAFT nodes.
// This function triggers RAFT nodes to be created, and is the entrance to the RAFT
// world from main.go.
func StartRaftNodes(bindall bool) {
	if x.WorkerConfig.MyAddr == "" {
		x.WorkerConfig.MyAddr = fmt.Sprintf("localhost:%d", workerPort())
	}

	glog.Infof("Starting Alpha in Standalone Mode (No Raft/Zero)")

	// Initialize local group state
	g := groups()
	g.Lock()
	g.gid = 1
	g.state = &pb.MembershipState{
		Groups: map[uint32]*pb.Group{
			1: {
				Members: map[uint64]*pb.Member{
					1: {Id: 1, GroupId: 1, Addr: x.WorkerConfig.MyAddr},
				},
			},
		},
	}
	g.Node = &node{
		Node:   &conn.Node{Id: 1},
		closer: z.NewCloser(1),
		ops:    make(map[op]operation),
	}
	g.Node.Applied.Init(nil)
	g.Unlock()

	x.Checkf(schema.LoadFromDb(context.Background()), "Error while initializing schema")
	glog.Infof("Load schema from DB: OK")

	gr.applyInitialSchema()
	gr.applyInitialTypes()
	glog.Infof("Upserted Schema and Types: OK")

	x.UpdateHealthStatus(true)
	glog.Infof("Server is ready: OK")
}

func (g *groupi) Ctx() context.Context {
	return g.closer.Ctx()
}

func (g *groupi) IsClosed() bool {
	return g.closer.Ctx().Err() != nil
}

func (g *groupi) applyInitialTypes() {
	initialTypes := schema.InitialTypes(x.RootNamespace)
	ts := State.GetTimestamp(false)
	if ts == 0 {
		ts = 1
	}
	for _, t := range initialTypes {
		if _, ok := schema.State().GetType(t.TypeName); ok {
			continue
		}
		if err := updateType(t.GetTypeName(), t, ts); err != nil {
			glog.Errorf("Error while applying initial type: %s", err)
		}
	}
}

func (g *groupi) applyInitialSchema() {
	if g.groupId() != 1 {
		return
	}
	initialSchema := schema.InitialSchema(x.RootNamespace)
	ctx := g.Ctx()
	ts := State.GetTimestamp(false)
	if ts == 0 {
		ts = 1
	}

	apply := func(s *pb.SchemaUpdate) {
		// There are 2 cases: either the alpha is fresh or it restarted.
		if err := applySchema(s, ts); err != nil {
			glog.Errorf("Error while applying initial schema: %s", err)
		}
	}

	for _, s := range initialSchema {
		if gid, err := g.BelongsToReadOnly(s.Predicate, 0); err != nil {
			glog.Errorf("Error getting tablet for predicate %s. Will force schema proposal.",
				s.Predicate)
			apply(s)
		} else if gid == 0 {
			// The tablet is not being served currently.
			apply(s)
		} else if curr, _ := schema.State().Get(ctx, s.Predicate); gid == g.groupId() && !proto.Equal(s, &curr) {
			// If this tablet is served to the group, do not upsert the schema unless the
			// stored schema and the proposed one are different.
			apply(s)
		} else {
			// The schema for this predicate has already been proposed.
			glog.V(1).Infof("Schema found for predicate %s: %+v", s.Predicate, curr)
			continue
		}
	}
}

func applySchema(s *pb.SchemaUpdate, ts uint64) error {
	if err := updateSchema(s, ts); err != nil {
		return err
	}
	if servesTablet, err := groups().ServesTablet(s.Predicate); err != nil {
		return err
	} else if !servesTablet {
		return errors.Errorf("group 1 should always serve reserved predicate %s", s.Predicate)
	}
	return nil
}

// No locks are acquired while accessing this function.
// Don't acquire RW lock during this, otherwise we might deadlock.
func (g *groupi) groupId() uint32 {
	return atomic.LoadUint32(&g.gid)
}

// MaxLeaseId returns the maximum UID that has been leased.
func MaxLeaseId() uint64 {
	g := groups()
	g.RLock()
	defer g.RUnlock()
	if g.state == nil {
		return 0
	}
	return g.state.MaxUID
}

// GetMembershipState returns the current membership state.
func GetMembershipState() *pb.MembershipState {
	g := groups()
	g.RLock()
	defer g.RUnlock()
	return proto.Clone(g.state).(*pb.MembershipState)
}

// UpdateMembershipState is a no-op in stateless mode.
func UpdateMembershipState(ctx context.Context) error {
	return nil
}

func (g *groupi) applyState(myId uint64, state *pb.MembershipState) {
	x.AssertTrue(state != nil)
	g.Lock()
	defer g.Unlock()
	// We don't update state if we get any old state. Counter stores the raftindex of
	// last update. For leader changes at zero since we don't propose, state can get
	// updated at same counter value. So ignore only if counter is less.
	if g.state != nil && g.state.Counter > state.Counter {
		return
	}

	oldState := g.state
	g.state = state

	// Sometimes this can cause us to lose latest tablet info, but that shouldn't cause any issues.
	var foundSelf bool
	g.tablets = make(map[string]*pb.Tablet)
	for gid, group := range g.state.Groups {
		for _, member := range group.Members {
			if myId == member.Id {
				foundSelf = true
				atomic.StoreUint32(&g.gid, gid)
			}
			if x.WorkerConfig.MyAddr != member.Addr {
				conn.GetPools().Connect(member.Addr, x.WorkerConfig.TLSClientConfig)
			}
		}
		for _, tablet := range group.Tablets {
			g.tablets[tablet.Predicate] = tablet
		}
		if gid == g.groupId() {
			glog.V(3).Infof("group %d checksum: %d", g.groupId(), group.Checksum)
			atomic.StoreUint64(&g.membershipChecksum, group.Checksum)
		}
	}
	for id, member := range g.state.Zeros {
		if x.WorkerConfig.MyAddr != member.Addr {
			conn.GetPools().Connect(member.Addr, x.WorkerConfig.TLSClientConfig)
		}
		// Remove stale pool if this Zero changed address.
		if oldState != nil {
			if old, ok := oldState.Zeros[id]; ok && old.Addr != member.Addr {
				conn.GetPools().Remove(old.Addr)
			}
		}
	}
	if !foundSelf {
		// I'm not part of this cluster. I should crash myself.
		glog.Fatalf("Unable to find myself [id:%d group:%d] in membership state: %+v. Goodbye!",
			myId, g.groupId(), state)
	}

	// While restarting we fill Node information after retrieving initial state.
	if g.Node != nil {
		conn.GetPools().RemoveInvalid(g.state)
	}
}

func (g *groupi) ServesGroup(gid uint32) bool {
	return g.groupId() == gid
}

func (g *groupi) ChecksumsMatch(ctx context.Context) error {
	if atomic.LoadUint64(&g.deltaChecksum) == atomic.LoadUint64(&g.membershipChecksum) {
		return nil
	}
	t := time.Tick(100 * time.Millisecond)

	for {
		select {
		case <-t:
			if atomic.LoadUint64(&g.deltaChecksum) == atomic.LoadUint64(&g.membershipChecksum) {
				return nil
			}
		case <-ctx.Done():
			return errors.Errorf("Group checksum mismatch for id: %d", g.groupId())
		}
	}
}

func (g *groupi) BelongsTo(key string) (uint32, error) {
	return 1, nil
}

// BelongsToReadOnly acts like BelongsTo except it does not ask zero to serve
// the tablet for key if no group is currently serving it.
func (g *groupi) BelongsToReadOnly(key string, ts uint64) (uint32, error) {
	return 1, nil
}

func (g *groupi) ServesTablet(key string) (bool, error) {
	return true, nil
}

func (g *groupi) Tablet(key string) (*pb.Tablet, error) {
	return &pb.Tablet{GroupId: 1, Predicate: key}, nil
}

func (g *groupi) Inform(preds []string) ([]*pb.Tablet, error) {
	return nil, nil
}

func (g *groupi) ForceTablet(key string) (*pb.Tablet, error) {
	return &pb.Tablet{GroupId: 1, Predicate: key}, nil
}

func (g *groupi) HasMeInState() bool {
	g.RLock()
	defer g.RUnlock()
	if g.state == nil {
		return false
	}

	group, has := g.state.Groups[g.groupId()]
	if !has {
		return false
	}
	_, has = group.Members[g.Node.Id]
	return has
}

// Returns 0, 1, or 2 valid server addrs.
func (g *groupi) AnyTwoServers(gid uint32) []string {
	g.RLock()
	defer g.RUnlock()

	if g.state == nil {
		return []string{}
	}
	group, has := g.state.Groups[gid]
	if !has {
		return []string{}
	}
	var res []string
	for _, m := range group.Members {
		// map iteration gives us members in no particular order.
		res = append(res, m.Addr)
		if len(res) >= 2 {
			break
		}
	}
	return res
}

func (g *groupi) members(gid uint32) map[uint64]*pb.Member {
	g.RLock()
	defer g.RUnlock()

	if g.state == nil {
		return nil
	}
	if gid == 0 {
		return g.state.Zeros
	}
	group, has := g.state.Groups[gid]
	if !has {
		return nil
	}
	return group.Members
}

func (g *groupi) AnyServer(gid uint32) *conn.Pool {
	members := g.members(gid)
	for _, m := range members {
		pl, err := conn.GetPools().Get(m.Addr)
		if err == nil {
			return pl
		}
	}
	return nil
}

func (g *groupi) MyPeer() (uint64, bool) {
	members := g.members(g.groupId())
	for _, m := range members {
		if m.Id != g.Node.Id {
			return m.Id, true
		}
	}
	return 0, false
}

// Leader will try to return the leader of a given group, based on membership information.
// There is currently no guarantee that the returned server is the leader of the group.
func (g *groupi) Leader(gid uint32) *conn.Pool {
	members := g.members(gid)
	if members == nil {
		return nil
	}
	for _, m := range members {
		if m.Leader {
			if pl, err := conn.GetPools().Get(m.Addr); err == nil {
				return pl
			}
		}
	}
	return nil
}

func (g *groupi) KnownGroups() (gids []uint32) {
	g.RLock()
	defer g.RUnlock()
	if g.state == nil || len(g.state.Groups) == 0 {
		if x.WorkerConfig.Embedded {
			return []uint32{1}
		}
		return
	}
	for gid := range g.state.Groups {
		gids = append(gids, gid)
	}
	return
}

// KnownGroups returns the known groups using the global groupi instance.
func KnownGroups() []uint32 {
	return groups().KnownGroups()
}

// GroupId returns the group to which this worker belongs to.
func GroupId() uint32 {
	return groups().groupId()
}

// NodeId returns the raft id of the node.
func NodeId() uint64 {
	return groups().Node.Id
}

func (g *groupi) triggerMembershipSync() {
	// It's ok if we miss the trigger, periodic membership sync runs every minute.
	select {
	case g.triggerCh <- struct{}{}:
	// It's ok to ignore it, since we would be sending update of a later state
	default:
	}
}

// GetFeaturesList returns a list of Dgraph features that are available.
func GetFeaturesList() []string {
	var ee []string
	if Config.AclSecretKey != nil {
		ee = append(ee, "acl")
		ee = append(ee, "multi_tenancy")
	}
	if x.WorkerConfig.EncryptionKey != nil {
		ee = append(ee, "encryption_at_rest", "encrypted_backup_restore", "encrypted_export")
	} else {
		ee = append(ee, "backup_restore")
	}
	if x.WorkerConfig.Audit {
		ee = append(ee, "audit")
	}
	if Config.ChangeDataConf != "" {
		ee = append(ee, "cdc")
	}
	return ee
}

// SubscribeForUpdates will listen for updates for the given group.
func SubscribeForUpdates(prefixes [][]byte, ignore string, cb func(kvs *badgerpb.KVList),
	group uint32, closer *z.Closer) {

	var prefix []byte
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}
	defer func() {
		glog.Infof("SubscribeForUpdates closing for prefix: %q\n", prefix)
		closer.Done()
	}()

	listen := func() error {
		// Connect to any of the group 1 nodes.
		members := groups().AnyTwoServers(group)
		// There may be a lag while starting so keep retrying.
		if len(members) == 0 {
			return fmt.Errorf("unable to find any servers for group: %d", group)
		}
		pool := conn.GetPools().Connect(members[0], x.WorkerConfig.TLSClientConfig)
		client := pb.NewWorkerClient(pool.Get())

		// Get Subscriber stream.
		stream, err := client.Subscribe(closer.Ctx(),
			&pb.SubscriptionRequest{Matches: x.PrefixesToMatches(prefixes, ignore)})
		if err != nil {
			return errors.Wrapf(err, "error from client.subscribe")
		}
		for {
			// Listen for updates.
			kvs, err := stream.Recv()
			if err != nil {
				return errors.Wrapf(err, "while receiving from stream")
			}
			cb(kvs)
		}
	}

	for {
		if err := listen(); err != nil {
			glog.Errorf("Error during SubscribeForUpdates for prefix %q: %v. closer err: %v\n",
				prefix, err, closer.Ctx().Err())
		}
		if closer.Ctx().Err() != nil {
			return
		}
		time.Sleep(time.Second)
	}
}
