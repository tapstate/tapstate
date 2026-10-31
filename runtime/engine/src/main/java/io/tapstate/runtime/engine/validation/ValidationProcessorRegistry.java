package io.tapstate.runtime.engine.validation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.io.Serial;
import java.io.Serializable;
import java.util.*;

/**
 * Member-local, single-execution processor ownership. Only primitive coordinates travel with a
 * supplier; actual run authorization and exit facts stay in this original registry incarnation.
 * Sealing rejects late supplier/processor entry. A supplier exit is not native connector drain or
 * business completion: readers retain their separately admitted resources until their actual exit.
 */
public final class ValidationProcessorRegistry implements AutoCloseable {
    private static final String CONTEXT = "tapstate.validation-processors";
    private static final long MAX_BYTES = 4L * 1024 * 1024;
    private static final int MAX_RUNS = 128, MAX_VERTICES = 64;
    private final HazelcastInstance member;
    private final UUID incarnation = UUID.randomUUID();
    private final UUID memberId;
    private final Runnable requireLocal;
    private final Map<UUID, Run> runs = new HashMap<>();
    private long bytes;
    private volatile boolean closed;

    public ValidationProcessorRegistry(HazelcastInstance member, Runnable requireLocal) {
        this.member = Objects.requireNonNull(member); this.memberId = member.getCluster().getLocalMember().getUuid();
        this.requireLocal = Objects.requireNonNull(requireLocal);
        if (member.getUserContext().putIfAbsent(CONTEXT, this) != null) throw new IllegalStateException("validation processor registry already bound");
    }

    /** Invoke only after actual CPU/I/O/metadata reservation for every vertex. Guards are pure local checks. */
    public Run register(ValidationRunIdentity identity, Set<String> vertices, Runnable requireCurrent) {
        Objects.requireNonNull(identity); Objects.requireNonNull(vertices); Objects.requireNonNull(requireCurrent);
        if (vertices.isEmpty() || vertices.size() > MAX_VERTICES) throw invalid("processors.vertices");
        long charge = 1024L + 2L * (identity.owner().clusterId().length() + identity.owner().pipelineId().length()
                + identity.owner().nodeId().length() + identity.owner().bootId().length() + identity.taskId().length()
                + identity.runId().length() + identity.specHash().length() + identity.comparisonRulesVersion().length());
        for (String vertex : vertices) { if (vertex == null || vertex.isBlank() || vertex.length() > 512) throw invalid("processors.vertex"); charge += 256L + 2L * vertex.length(); }
        requireLocal.run(); requireCurrent.run();
        synchronized (this) {
            if (closed) throw invalid("processors.registryClosed");
            if (runs.size() == MAX_RUNS || charge > MAX_BYTES - bytes) throw limit();
            for (var existing : runs.values()) if (existing.identity.equals(identity)) throw invalid("processors.originalAlreadyRegistered");
            var run = new Run(UUID.randomUUID(), identity, Set.copyOf(vertices), requireCurrent, charge);
            runs.put(run.id, run); bytes += charge; return run;
        }
    }

    public final class Run {
        private final UUID id;
        private final ValidationRunIdentity identity;
        private final Runnable requireCurrent;
        private final Map<String, Phase> vertices = new HashMap<>();
        private final Map<String, ProcessorSupplier> factories = new HashMap<>();
        private final long charge;
        private boolean sealed;
        private int active;
        private Run(UUID id, ValidationRunIdentity identity, Set<String> vertices, Runnable requireCurrent, long charge) {
            this.id = id; this.identity = identity; this.requireCurrent = requireCurrent; this.charge = charge;
            vertices.forEach(vertex -> this.vertices.put(vertex, Phase.NEW));
        }
        public ValidationRunIdentity identity() { return identity; }
        public synchronized void requireRoster(Set<String> actual) { if (!vertices.keySet().equals(actual)) throw invalid("processors.actualRoster"); }
        public ProcessorSupplier wrap(String vertex, ProcessorSupplier delegate) {
            Objects.requireNonNull(delegate);
            boolean initCooperative = delegate.initIsCooperative(), closeCooperative = delegate.closeIsCooperative();
            var declared = delegate.permissions(); var permissions = declared == null ? null : List.copyOf(declared);
            long permissionBytes = 0;
            if (permissions != null && permissions.size() > 64) throw invalid("processors.permissions");
            for (var permission : permissions == null ? List.<java.security.Permission>of() : permissions) {
                permissionBytes += 256L + 2L * (permission.getName().length() + permission.getActions().length());
                if (permissionBytes > 8192) throw invalid("processors.permissions");
            }
            synchronized (this) {
                if (!vertices.containsKey(vertex) || sealed || factories.putIfAbsent(vertex, delegate) != null) throw invalid("processors.originalVertex");
            }
            // The actual native factory and its resources stay on this original member. Only its
            // bounded lookup token and declared scheduling/security metadata travel in the DAG.
            return new Tracked(new Token(incarnation, id, memberId, vertex), initCooperative, closeCooperative, permissions);
        }
        /** Close admission before cancelling Jet. Queued suppliers cannot create new actual processors. */
        public synchronized void seal() { sealed = true; }
        /** Passive actual supplier exit facts, including suppliers that never entered before sealing. */
        public synchronized boolean processorsExited() { return sealed && active == 0; }
        /** Known control retirement and actual exits are caller prerequisites; no authority is borrowed. */
        public void forget() {
            synchronized (ValidationProcessorRegistry.this) {
                synchronized (this) { if (!sealed || active != 0) throw invalid("processors.notExited"); }
                if (runs.remove(id, this)) { synchronized (this) { factories.clear(); } bytes -= charge; }
            }
        }
        private void require() {
            synchronized (this) { if (sealed || closed) throw invalid("processors.runClosed"); }
            requireLocal.run(); requireCurrent.run();
            synchronized (this) { if (sealed || closed) throw invalid("processors.runClosed"); }
        }
        private void enter(String vertex) {
            require();
            synchronized (this) {
                if (sealed || closed || vertices.get(vertex) != Phase.NEW) throw invalid("processors.executionReentry");
                vertices.put(vertex, Phase.ENTERED); active++;
            }
        }
        private synchronized void exit(String vertex) {
            if (vertices.get(vertex) != Phase.ENTERED) throw new IllegalStateException("validation supplier exit without its original entry");
            vertices.put(vertex, Phase.EXITED); active--;
        }
        private synchronized ProcessorSupplier factory(String vertex) { return Objects.requireNonNull(factories.get(vertex), "original processor factory"); }
    }
    private enum Phase { NEW, ENTERED, EXITED }
    private record Token(UUID incarnation, UUID run, UUID member, String vertex) implements Serializable { @Serial private static final long serialVersionUID = 1L; }

    private static Run resolve(HazelcastInstance member, Token token) {
        var candidate = member.getUserContext().get(CONTEXT);
        if (!(candidate instanceof ValidationProcessorRegistry registry) || !registry.incarnation.equals(token.incarnation())
                || !registry.memberId.equals(token.member()) || !member.getCluster().getLocalMember().getUuid().equals(token.member())) throw invalid("processors.originalRegistry");
        synchronized (registry) {
            var run = registry.runs.get(token.run()); if (run == null) throw invalid("processors.originalRun"); return run;
        }
    }
    private static final class Tracked implements ProcessorSupplier {
        @Serial private static final long serialVersionUID = 1L;
        private final Token token;
        private final boolean initCooperative, closeCooperative;
        private final List<java.security.Permission> permissions;
        private transient ProcessorSupplier delegate;
        private transient Run run;
        private transient boolean entered, touched, exited;
        private Tracked(Token token, boolean initCooperative, boolean closeCooperative, List<java.security.Permission> permissions) {
            this.token = token; this.initCooperative = initCooperative; this.closeCooperative = closeCooperative; this.permissions = permissions;
        }
        @Override public void init(Context context) throws Exception {
            run = resolve(context.hazelcastInstance(), token); run.enter(token.vertex()); entered = true;
            run.require(); delegate = run.factory(token.vertex()); touched = true; delegate.init(context);
        }
        @Override public Collection<? extends Processor> get(int count) {
            if (!entered || count != 1) throw invalid("processors.localParallelism"); run.require();
            var actual = Objects.requireNonNull(delegate.get(count));
            if (actual.size() != 1) throw new IllegalStateException("validation supplier returned the wrong number of processors");
            return actual.stream().map(processor -> new Guarded(Objects.requireNonNull(processor), run)).toList();
        }
        @Override public boolean initIsCooperative() { return initCooperative; }
        @Override public boolean closeIsCooperative() { return closeCooperative; }
        @Override public List<java.security.Permission> permissions() { return permissions; }
        @Override public void close(Throwable error) throws Exception {
            try { if (touched) delegate.close(error); }
            finally { if (entered && !exited) { exited = true; run.exit(token.vertex()); } }
        }
    }
    private static final class Guarded implements io.tapstate.runtime.engine.TransparentProcessor {
        private final Processor delegate;
        private final Run run;
        private Guarded(Processor delegate, Run run) { this.delegate = delegate; this.run = run; }
        @Override public boolean isCooperative() { return delegate.isCooperative(); }
        @Override public void init(Outbox outbox, Context context) throws Exception { run.require(); delegate.init(outbox, context); }
        @Override public void process(int ordinal, Inbox inbox) { run.require(); delegate.process(ordinal, inbox); }
        @Override public boolean tryProcessWatermark(Watermark watermark) { run.require(); return delegate.tryProcessWatermark(watermark); }
        @Override public boolean tryProcessWatermark(int ordinal, Watermark watermark) { run.require(); return delegate.tryProcessWatermark(ordinal, watermark); }
        @Override public boolean tryProcess() { run.require(); return delegate.tryProcess(); }
        @Override public boolean completeEdge(int ordinal) { run.require(); return delegate.completeEdge(ordinal); }
        @Override public boolean complete() { run.require(); return delegate.complete(); }
        @Override public boolean saveToSnapshot() { throw invalid("processors.snapshotsDisabled"); }
        @Override public boolean snapshotCommitPrepare() { throw invalid("processors.snapshotsDisabled"); }
        @Override public boolean snapshotCommitFinish(boolean success) { throw invalid("processors.snapshotsDisabled"); }
        @Override public void restoreFromSnapshot(Inbox inbox) { throw invalid("processors.snapshotsDisabled"); }
        @Override public boolean finishSnapshotRestore() { throw invalid("processors.snapshotsDisabled"); }
        @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
        @Override public void close() throws Exception { delegate.close(); }
    }
    @Override public void close() {
        List<Run> original;
        synchronized (this) { if (closed) return; closed = true; original = List.copyOf(runs.values()); }
        member.getUserContext().remove(CONTEXT, this); original.forEach(Run::seal);
    }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit() { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.processorRegistry"), null); }
}
