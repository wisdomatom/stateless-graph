/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	ostats "go.opencensus.io/stats"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dgraph-io/badger/v4"
	"github.com/wisdomatom/stateless-graph/conn"
	"github.com/wisdomatom/stateless-graph/protos/pb"
	"github.com/wisdomatom/stateless-graph/schema"
	"github.com/wisdomatom/stateless-graph/types"
	"github.com/wisdomatom/stateless-graph/x"
	"github.com/dgraph-io/ristretto/v2/z"
)

type operation struct {
	*z.Closer
	ts uint64
}

type node struct {
	// This needs to be 64 bit aligned for atomics to work on 32 bit machine.
	pendingSize int64

	// embedded struct
	*conn.Node

	// Fields which are never changed after init.
	ctx     context.Context
	gid     uint32
	closer  *z.Closer

	checkpointTs uint64 // Timestamp corresponding to checkpoint.
	streaming    int32  // Used to avoid calculating snapshot

	// Used to track the ops going on in the system.
	ops         map[op]operation
	opsLock     sync.Mutex
	cdcTracker  *CDC
	canCampaign bool
}

type op int

func (id op) String() string {
	switch id {
	case opRollup:
		return "opRollup"
	case opSnapshot:
		return "opSnapshot"
	case opIndexing:
		return "opIndexing"
	case opRestore:
		return "opRestore"
	case opBackup:
		return "opBackup"
	case opPredMove:
		return "opPredMove"
	default:
		return "opUnknown"
	}
}

const (
	opRollup op = iota + 1
	opSnapshot
	opIndexing
	opRestore
	opBackup
	opPredMove
)

// startTask is used for the tasks that do not require tracking of timestamp.
// Currently, only the timestamps for backup and indexing needs to be tracked because they can
// run concurrently.
func (n *node) startTask(id op) (*z.Closer, error) {
	return n.startTaskAtTs(id, 0)
}

// startTaskAtTs is used to check whether an op is already running. If a rollup is running,
// it is canceled and startTask will wait until it completes before returning.
// If the same task is already running, this method returns an error.
// Restore operations have preference and cancel all other operations, not just rollups.
// You should only call Done() on the returned closer. Calling other functions (such as
// SignalAndWait) for closer could result in panics. For more details, see GitHub issue #5034.
func (n *node) startTaskAtTs(id op, ts uint64) (*z.Closer, error) {
	n.opsLock.Lock()
	defer n.opsLock.Unlock()

	stopTask := func(id op) {
		n.opsLock.Lock()
		delete(n.ops, id)
		n.opsLock.Unlock()
		glog.Infof("Operation completed with id: %s", id)

		// Resume rollups if another operation is being stopped.
		if id != opRollup {
			time.Sleep(10 * time.Second) // Wait for 10s to start rollup operation.
			// If any other operation is running, this would error out. This error can
			// be safely ignored because rollups will resume once that other task is done.
			_, _ = n.startTask(opRollup)
		}
	}

	closer := z.NewCloser(1)
	switch id {
	case opRollup:
		if len(n.ops) > 0 {
			return nil, errors.Errorf("another operation is already running")
		}
		go IncrRollup.Process(closer, State.GetTimestamp)
	case opRestore:
		// Restores cancel all other operations, except for other restores since
		// only one restore operation should be active any given moment.
		for otherId, otherOp := range n.ops {
			if otherId == opRestore {
				return nil, errors.Errorf("another restore operation is already running")
			}
			// Remove from map and signal the closer to cancel the operation.
			delete(n.ops, otherId)
			otherOp.SignalAndWait()
		}
	case opBackup:
		// Backup cancels all other operations, except for other backups since
		// only one backup operation should be active any given moment. Also, indexing at higher
		// timestamp can also run concurrently with backup.
		for otherId, otherOp := range n.ops {
			if otherId == opBackup {
				return nil, errors.Errorf("another backup operation is already running")
			}
			// Remove from map and signal the closer to cancel the operation.
			delete(n.ops, otherId)
			otherOp.SignalAndWait()
		}
	case opIndexing:
		for otherId, otherOp := range n.ops {
			switch otherId {
			case opBackup:
				if otherOp.ts < ts {
					// If backup is running at higher timestamp, then indexing can't be executed.
					continue
				}
				return nil, errors.Errorf("operation %s is already running", otherId)
			case opRollup:
				// Remove from map and signal the closer to cancel the operation.
				delete(n.ops, otherId)
				otherOp.SignalAndWait()
			default:
				return nil, errors.Errorf("operation %s is already running", otherId)
			}
		}
	case opSnapshot, opPredMove:
		for otherId, otherOp := range n.ops {
			if otherId == opRollup {
				// Remove from map and signal the closer to cancel the operation.
				delete(n.ops, otherId)
				otherOp.SignalAndWait()
			} else {
				return nil, errors.Errorf("operation %s is already running", otherId)
			}
		}
	default:
		glog.Errorf("Got an unhandled operation %s. Ignoring...", id)
		return nil, nil
	}

	n.ops[id] = operation{Closer: closer, ts: ts}
	glog.Infof("Operation started with id: %s", id)
	go func(id op, closer *z.Closer) {
		closer.Wait()
		stopTask(id)
	}(id, closer)
	return closer, nil
}

func (n *node) waitForTask(id op) {
	n.opsLock.Lock()
	closer, ok := n.ops[id]
	n.opsLock.Unlock()
	if !ok {
		return
	}
	closer.Wait()
}

func (n *node) isRunningTask(id op) bool {
	n.opsLock.Lock()
	_, ok := n.ops[id]
	n.opsLock.Unlock()
	return ok
}

func (n *node) stopAllTasks() {
	defer n.closer.Done() // CLOSER:1
	<-n.closer.HasBeenClosed()

	glog.Infof("Stopping all ongoing registered tasks...")
	n.opsLock.Lock()
	defer n.opsLock.Unlock()
	for op, closer := range n.ops {
		glog.Infof("Stopping op: %s...\n", op)
		closer.SignalAndWait()
	}
	glog.Infof("Stopped all ongoing registered tasks.")
}

// GetOngoingTasks returns the list of ongoing tasks.
func GetOngoingTasks() []string {
	n := groups().Node
	if n == nil {
		return []string{}
	}

	n.opsLock.Lock()
	defer n.opsLock.Unlock()
	var tasks []string
	for id := range n.ops {
		tasks = append(tasks, id.String())
	}
	return tasks
}

// Now that we apply txn updates via Raft, waiting based on Txn timestamps is
// sufficient. We don't need to wait for proposals to be applied.

func (n *node) Ctx(key uint64) context.Context {
	if pctx := n.Proposals.Get(key); pctx != nil {
		return pctx.Ctx
	}
	return context.Background()
}

var errHasPendingTxns = errors.New("Pending transactions found. Please retry operation")

// We must not wait here. Previously, we used to block until we have aborted the
// transactions. We're now applying all updates serially, so blocking for one
// operation is not an option.
func detectPendingTxns(attr string) error {
	tctxs := Oracle().IterateTxns(func(key []byte) bool {
		pk, err := x.Parse(key)
		if err != nil {
			glog.Errorf("error %v while parsing key %v", err, hex.EncodeToString(key))
			return false
		}
		return pk.Attr == attr
	})
	if len(tctxs) == 0 {
		return nil
	}
	go tryAbortTransactions(tctxs)
	return errHasPendingTxns
}

// We don't support schema mutations across nodes in a transaction.
// Wait for all transactions to either abort or complete and all write transactions
// involving the predicate are aborted until schema mutations are done.
func (n *node) applyMutations(ctx context.Context, proposal *pb.Proposal) (rerr error) {
	span := trace.SpanFromContext(ctx)

	if proposal.Mutations.DropOp == pb.Mutations_ALL_IN_NS {
		ns, err := strconv.ParseUint(proposal.Mutations.DropValue, 0, 64)
		if err != nil {
			return err
		}
		// Ensures nothing get written to disk due to commit proposals.
		Oracle().ResetTxnsForNs(ns)
		if err := DeleteAllForNs(ns); err != nil {
			return err
		}

		// TODO: What about multi shard cluster?
		// It should be okay to set the schema at timestamp 1 after drop all operation.
		if groups().groupId() == 1 {
			initialSchema := schema.InitialSchema(ns)
			for _, s := range initialSchema {
				if err := applySchema(s, 1); err != nil {
					return err
				}
			}
		}

		// Propose initial types as well after a drop all as they would have been cleared.
		initialTypes := schema.InitialTypes(ns)
		for _, t := range initialTypes {
			if err := updateType(t.GetTypeName(), t, 1); err != nil {
				return err
			}
		}

		// TODO: Revisit this when we work on posting cache. Don't clear entire cache.
		// We don't want to drop entire cache, just due to one namespace.
		ResetCache()
		return nil
	}

	if proposal.Mutations.DropOp == pb.Mutations_DATA {
		ns, err := strconv.ParseUint(proposal.Mutations.DropValue, 0, 64)
		if err != nil {
			return err
		}
		// Ensures nothing get written to disk due to commit proposals.
		Oracle().ResetTxnsForNs(ns)
		if err := DeleteData(ns); err != nil {
			return err
		}

		// TODO: Revisit this when we work on posting cache. Don't clear entire cache.
		// We don't want to drop entire cache, just due to one namespace.
		ResetCache()
		return nil
	}

	if proposal.Mutations.DropOp == pb.Mutations_ALL {
		// Ensures nothing get written to disk due to commit proposals.
		Oracle().ResetTxns()
		schema.State().DeleteAll()

		if err := DeleteAll(); err != nil {
			return err
		}

		// Clear entire cache.
		ResetCache()

		ts := State.GetTimestamp(false)
		if ts == 0 {
			ts = 1
		}

		if groups().groupId() == 1 {
			initialSchema := schema.InitialSchema(x.RootNamespace)
			for _, s := range initialSchema {
				if err := applySchema(s, ts); err != nil {
					return err
				}
			}
		}

		// Propose initial types as well after a drop all as they would have been cleared.
		initialTypes := schema.InitialTypes(x.RootNamespace)
		for _, t := range initialTypes {
			if err := updateType(t.GetTypeName(), t, ts); err != nil {
				return err
			}
		}

		return nil
	}

	if proposal.Mutations.DropOp == pb.Mutations_TYPE {
		return schema.State().DeleteType(proposal.Mutations.DropValue, proposal.StartTs)
	}

	if proposal.Mutations.StartTs == 0 {
		return errors.New("StartTs must be provided")
	}

	if len(proposal.Mutations.Schema) > 0 || len(proposal.Mutations.Types) > 0 {
		// MaxAssigned would ensure that everything that's committed up until this point
		// would be picked up in building indexes. Any uncommitted txns would be cancelled
		// by detectPendingTxns below.
		startTs := Oracle().MaxAssigned()

		span.AddEvent("Applying schema and types")
		for _, supdate := range proposal.Mutations.Schema {
			// We should not need to check for predicate move here.
			if err := detectPendingTxns(supdate.Predicate); err != nil {
				return err
			}
		}

		if err := runSchemaMutation(ctx, proposal.Mutations.Schema, startTs); err != nil {
			return err
		}

		// Clear the entire cache if there is a schema update because the index rebuild
		// will invalidate the state.
		if len(proposal.Mutations.Schema) > 0 {
			ResetCache()
		}

		for _, tupdate := range proposal.Mutations.Types {
			if err := runTypeMutation(ctx, tupdate, startTs); err != nil {
				return err
			}
		}
		return nil
	}

	// Scheduler tracks tasks at subject, predicate level, so doing
	// schema stuff here simplifies the design and we needn't worry about
	// serializing the mutations per predicate or schema mutations
	// We derive the schema here if it's not present
	// Since raft committed logs are serialized, we can derive
	// schema here without any locking

	// Stores a map of predicate and type of first mutation for each predicate.
	schemaMap := make(map[string]types.TypeID)
	for _, edge := range proposal.Mutations.Edges {
		if edge.Entity == 0 && bytes.Equal(edge.Value, []byte(x.Star)) {
			// We should only drop the predicate if there is no pending
			// transaction.
			if err := detectPendingTxns(edge.Attr); err != nil {
				span.AddEvent("Found pending transactions. Retry later.")
				return err
			}
			span.AddEvent("Deleting predicate")
			return DeletePredicate(ctx, edge.Attr, proposal.StartTs)
		}
		// Don't derive schema when doing deletion.
		if edge.Op == pb.DirectedEdge_DEL {
			continue
		}
		if _, ok := schemaMap[edge.Attr]; !ok {
			schemaMap[edge.Attr] = TypeID(edge)
		}
	}

	total := len(proposal.Mutations.Edges)

	// TODO: Active mutations values can go up or down but with
	// OpenCensus stats bucket boundaries start from 0, hence
	// recording negative and positive values skews up values.
	ostats.Record(ctx, x.ActiveMutations.M(int64(total)))
	defer func() {
		ostats.Record(ctx, x.ActiveMutations.M(int64(-total)))
	}()

	// Go through all the predicates and their first observed schema type. If we are unable to find
	// these predicates in the current schema state, add them to the schema state. Note that the
	// schema deduction is done by RDF/JSON chunker.
	for attr, storageType := range schemaMap {
		if _, err := schema.State().TypeOf(attr); err != nil {
			hint := pb.Metadata_DEFAULT
			if mutHint, ok := proposal.GetMutations().GetMetadata().GetPredHints()[attr]; ok {
				hint = mutHint
			}
			if err = createSchema(attr, storageType, hint, proposal.StartTs); err != nil {
				return err
			}
		}
	}

	m := proposal.Mutations

	// It is possible that the user gives us multiple versions of the same edge, one with no facets
	// and another with facets. In that case, use stable sort to maintain the ordering given to us
	// by the user.
	sort.SliceStable(m.Edges, func(i, j int) bool {
		ei := m.Edges[i]
		ej := m.Edges[j]
		if ei.GetAttr() != ej.GetAttr() {
			return ei.GetAttr() < ej.GetAttr()
		}
		return ei.GetEntity() < ej.GetEntity()
	})

	txn := Oracle().RegisterStartTs(m.StartTs)
	if txn.ShouldAbort() {
		span.AddEvent("Txn should abort.", trace.WithAttributes(
			attribute.Int64("start_ts", int64(m.StartTs)),
		))
		return x.ErrConflict
	}
	// Discard the posting lists from cache to release memory at the end.
	defer txn.Update()

	process := func(edges []*pb.DirectedEdge) error {
		var retries int
		for _, edge := range edges {
			for {
				err := runMutation(ctx, edge, txn)
				if err == nil {
					break
				}
				if err != ErrRetry {
					return err
				}
				retries++
			}
		}
		if retries > 0 {
			span.AddEvent("retries=true num=%d", trace.WithAttributes(
				attribute.Int("retries", retries)))
		}
		return nil
	}
	numGo, width := x.DivideAndRule(len(m.Edges))
	span.AddEvent("To apply: %d edges. NumGo: %d. Width: %d", trace.WithAttributes(
		attribute.Int("num_edges", len(m.Edges)),
		attribute.Int("num_go", numGo),
		attribute.Int("width", width)))

	if numGo == 1 {
		return process(m.Edges)
	}

	// We need to create batches such that no two batches contains the same entry (<Entity> + <Predicate>)
	// combination. We have already sorted the edges, all we need to do is increase the batch size until we reach
	// the next different entry. New number of chans would ne less than NumGo. So we can create the chan with
	// numGo.
	sameAttrAndUid := func(i, j int) bool {
		ei := m.Edges[i]
		ej := m.Edges[j]
		if ei.GetAttr() != ej.GetAttr() {
			return false
		}
		return ei.GetEntity() == ej.GetEntity()
	}
	numChanCreated := 0

	errCh := make(chan error, numGo)
	for i := 0; i < len(m.Edges); {
		end := i + width
		if end > len(m.Edges) {
			end = len(m.Edges)
		}

		for end < len(m.Edges) && sameAttrAndUid(end, end-1) {
			end++
		}

		numChanCreated += 1
		go func(start, end int) {
			errCh <- process(m.Edges[start:end])
		}(i, end)
		i = end
	}
	// Earlier we were returning after even if one thread had an error. We should wait for
	// all the transactions to finish. We call txn.Update() when this function exists. This could cause
	// a deadlock with runMutation.
	var errs error
	for range numChanCreated {
		if err := <-errCh; err != nil {
			if errs == nil {
				errs = errors.New("Got error while running mutation")
			}
			errs = errors.Wrap(err, errs.Error())
		}
	}
	return errs
}

// TODO(Anurag - 4 May 2020): Are we using pkey? Remove if unused.
func (n *node) commitOrAbort(pkey uint64, delta *pb.OracleDelta) error {
	x.PrintOracleDelta(delta)
	// First let's commit all mutations to disk.
	writer := NewTxnWriter(pstore)
	toDisk := func(start, commit uint64) {
		txn := Oracle().GetTxn(start)
		if txn == nil || commit == 0 {
			return
		}
		// If the transaction has failed, we dont need to update it.
		if commit != 0 {
			txn.Update()
		}
		// We start with 20 ms, so that we end up waiting 5 mins by the end.
		// If there is any transient issue, it should get fixed within that timeframe.
		err := x.ExponentialRetry(int(x.Config.MaxRetries),
			20*time.Millisecond, func() error {
				err := txn.CommitToDisk(writer, commit)
				if err == badger.ErrBannedKey {
					glog.Errorf("Error while writing to banned namespace.")
					return nil
				}
				return err
			})

		if err != nil {
			glog.Errorf("Error while applying txn status to disk (%d -> %d): %v",
				start, commit, err)
			panic(err)
		}
	}

	for _, status := range delta.Txns {
		toDisk(status.StartTs, status.CommitTs)
	}
	if err := writer.Flush(); err != nil {
		return errors.Wrapf(err, "while flushing to disk")
	}

	if x.WorkerConfig.HardSync {
		if err := pstore.Sync(); err != nil {
			glog.Errorf("Error while calling Sync while commitOrAbort: %v", err)
		}
	}

	g := groups()
	if delta.GroupChecksums != nil && delta.GroupChecksums[g.groupId()] > 0 {
		atomic.StoreUint64(&g.deltaChecksum, delta.GroupChecksums[g.groupId()])
	}

	// Clear all the cached lists that were touched by this transaction.
	for _, status := range delta.Txns {
		txn := Oracle().GetTxn(status.StartTs)
		txn.UpdateCachedKeys(status.CommitTs)
	}

	// Now advance Oracle(), so we can service waiting reads.
	Oracle().ProcessDelta(delta)
	return nil
}


func (n *node) updateRaftProgress() error {
	return nil
}

func (n *node) blockingAbort(req *pb.TxnTimestamps) error {
	for _, ts := range req.Ts {
		if txn := Oracle().GetTxn(ts); txn != nil {
			if txn.KVTxn != nil {
				txn.KVTxn.Discard()
			}
		}
	}
	return nil
}

func (n *node) AmLeader() bool {
	return true
}

func (n *node) proposeSnapshot() error {
	return nil
}

func (n *node) proposeCDCState(ts uint64) error {
	return nil
}
