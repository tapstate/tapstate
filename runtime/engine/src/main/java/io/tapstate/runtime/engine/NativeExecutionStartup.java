package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.map.EntryProcessor;
import io.tapstate.core.lifecycle.ProcessorRuntimeContext;

import java.io.Serializable;
import java.security.Permission;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Bounded latest native initialization evidence for the actual fenced execution, shared by endpoints. */
public final class NativeExecutionStartup {
    public static final String MAP_NAME = "tapstate.execution-startup";
    public static final String CLAIM_ARGUMENT = "tapstate.claim-generation";
    public static final String EXECUTION_ARGUMENT = "tapstate.execution-generation";
    public static final String PROFILE_ARGUMENT = "tapstate.profile-generation";
    static final String STAND_IN_PROCESSOR_TYPE = InitializedStandIn.class.getSimpleName();

    private NativeExecutionStartup() { }

    public static void install(DAG dag, HazelcastInstance member, String pipeline,
            long claimGeneration, long executionGeneration, long profileGeneration) {
        if (claimGeneration < 1 || executionGeneration < 1 || profileGeneration < 1) {
            throw new IllegalArgumentException("native startup requires an actual fenced execution");
        }
        Set<String> vertices = new java.util.LinkedHashSet<>();
        dag.forEach(vertex -> vertices.add(vertex.getName()));
        Evidence expected = new Evidence(claimGeneration, executionGeneration, profileGeneration,
                null, null, vertices, Map.of(), Map.of());
        member.<String, Evidence>getMap(MAP_NAME).executeOnKey(pipeline, entry -> {
            Evidence previous = entry.getValue();
            if (previous == null || previous.executionGeneration() < executionGeneration) {
                entry.setValue(expected);
            }
            return null;
        });
        dag.forEach(vertex -> vertex.updateMetaSupplier(delegate -> new Metas(delegate, pipeline,
                claimGeneration, executionGeneration, profileGeneration)));
    }

    public static Optional<Evidence> read(HazelcastInstance member, String pipeline) {
        return Optional.ofNullable(member.<String, Evidence>getMap(MAP_NAME).get(pipeline));
    }

    public static void forget(HazelcastInstance member, String pipeline) {
        member.getMap(MAP_NAME).delete(pipeline);
    }

    public record Evidence(long claimGeneration, long executionGeneration, long profileGeneration,
            String jobId, String runtimeExecutionId, Set<String> vertices,
            Map<String, Integer> expectedProcessors, Map<String, ProcessorRuntimeContext> processors)
            implements Serializable {
        private static final long serialVersionUID = 1L;
        public Evidence {
            vertices = Set.copyOf(vertices);
            expectedProcessors = Map.copyOf(expectedProcessors);
            processors = Map.copyOf(processors);
        }

        /** All actual native processors must initialize; job state alone never supplies this evidence. */
        public boolean initialized() {
            return jobId != null && !vertices.isEmpty() && expectedProcessors.keySet().equals(vertices)
                    && vertices.stream().allMatch(vertex -> processors.values().stream()
                            .filter(context -> context.vertex().equals(vertex)).count() == expectedProcessors.get(vertex));
        }
    }

    private static void mutate(HazelcastInstance member, String pipeline, long claim, long execution,
            long profile, String job, String runtimeExecution, String vertex, int total, ProcessorRuntimeContext processor) {
        member.<String, Evidence>getMap(MAP_NAME).executeOnKey(pipeline, (EntryProcessor<String, Evidence, Boolean>) entry -> {
            Evidence old = entry.getValue();
            if (old == null || old.claimGeneration() != claim || old.executionGeneration() != execution
                    || old.profileGeneration() != profile || !old.vertices().contains(vertex)
                    || (old.jobId() != null && (!old.jobId().equals(job)
                            || !old.runtimeExecutionId().equals(runtimeExecution)))) {
                return false;
            }
            Map<String, Integer> totals = new LinkedHashMap<>(old.expectedProcessors());
            Integer previous = totals.putIfAbsent(vertex, total);
            if (previous != null && previous != total) {
                throw new IllegalStateException("one native vertex reported incompatible processor counts");
            }
            Map<String, ProcessorRuntimeContext> contexts = new LinkedHashMap<>(old.processors());
            if (processor != null) {
                String key = vertex + ":" + processor.globalProcessorIndex();
                ProcessorRuntimeContext existing = contexts.putIfAbsent(key, processor);
                if (existing != null && (!existing.memberUuid().equals(processor.memberUuid())
                        || existing.localProcessorIndex() != processor.localProcessorIndex())) {
                    throw new IllegalStateException("one native processor index initialized on two members");
                }
            }
            entry.setValue(new Evidence(claim, execution, profile, job, runtimeExecution, old.vertices(), totals, contexts));
            return true;
        });
    }

    private static class Metas implements ProcessorMetaSupplier {
        private static final long serialVersionUID = 1L;
        private final ProcessorMetaSupplier delegate;
        private final String pipeline;
        private final long claim;
        private final long execution;
        private final long profile;

        Metas(ProcessorMetaSupplier delegate, String pipeline, long claim, long execution, long profile) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.pipeline = pipeline;
            this.claim = claim;
            this.execution = execution;
            this.profile = profile;
        }

        @Override
        public void init(Context context) throws Exception {
            delegate.init(context);
            mutate(context.hazelcastInstance(), pipeline, claim, execution, profile,
                    Long.toUnsignedString(context.jobId()), Long.toUnsignedString(context.executionId()),
                    context.vertexName(), context.totalParallelism(), null);
        }

        @Override
        public java.util.function.Function<? super com.hazelcast.cluster.Address, ? extends ProcessorSupplier> get(
                java.util.List<com.hazelcast.cluster.Address> addresses) {
            var suppliers = delegate.get(addresses);
            return address -> new Suppliers(suppliers.apply(address), pipeline, claim, execution, profile);
        }

        @Override public int preferredLocalParallelism() { return delegate.preferredLocalParallelism(); }
        @Override public Permission getRequiredPermission() { return delegate.getRequiredPermission(); }
        @Override public Map<String, String> getTags() { return delegate.getTags(); }
        @Override public boolean initIsCooperative() { return delegate.initIsCooperative(); }
        @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
        @Override public boolean isReusable() { return delegate.isReusable(); }
        @Override public void close(Throwable error) throws Exception { delegate.close(error); }
    }

    private static final class Suppliers implements ProcessorSupplier {
        private static final long serialVersionUID = 1L;
        private final ProcessorSupplier delegate;
        private final String pipeline;
        private final long claim;
        private final long execution;
        private final long profile;

        Suppliers(ProcessorSupplier delegate, String pipeline, long claim, long execution, long profile) {
            this.delegate = delegate;
            this.pipeline = pipeline;
            this.claim = claim;
            this.execution = execution;
            this.profile = profile;
        }

        @Override public void init(Context context) throws Exception { delegate.init(context); }
        @Override public boolean initIsCooperative() { return delegate.initIsCooperative(); }
        @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
        @Override public void close(Throwable error) throws Exception { delegate.close(error); }
        @Override public Collection<? extends Processor> get(int count) {
            return delegate.get(count).stream()
                    .map(processor -> PinnedStandIns.isStandIn(processor)
                            ? new InitializedStandIn(processor, pipeline, claim, execution, profile)
                            : new Initialized(processor, pipeline, claim, execution, profile)).toList();
        }
    }

    /** Publishes only initialization; all processing and its stage timer remain with the delegate. */
    private static class Initialized implements Processor {
        private final Processor delegate;
        private final String pipeline;
        private final long claim;
        private final long execution;
        private final long profile;

        Initialized(Processor delegate, String pipeline, long claim, long execution, long profile) {
            this.delegate = delegate;
            this.pipeline = pipeline;
            this.claim = claim;
            this.execution = execution;
            this.profile = profile;
        }

        @Override public boolean isCooperative() { return delegate.isCooperative(); }
        @Override public void init(Outbox outbox, Context context) throws Exception {
            delegate.init(outbox, context);
            var local = context.hazelcastInstance().getCluster().getLocalMember();
            ProcessorRuntimeContext observed = new ProcessorRuntimeContext(pipeline, context.vertexName(),
                    Long.toUnsignedString(context.jobId()), Long.toUnsignedString(context.executionId()), claim, execution,
                    profile, local.getAttribute("tapstate.node-id"), local.getAttribute("tapstate.boot-id"),
                    local.getUuid().toString(), local.getAddress().toString(), context.memberIndex(),
                    context.localProcessorIndex(), context.globalProcessorIndex(), context.localParallelism(),
                    context.totalParallelism(), context.memberCount(), Instant.now());
            mutate(context.hazelcastInstance(), pipeline, claim, execution, profile, observed.jobId(),
                    observed.runtimeExecutionId(), observed.vertex(), observed.totalParallelism(), observed);
        }
        @Override public void process(int ordinal, Inbox inbox) { delegate.process(ordinal, inbox); }
        @Override public boolean tryProcess() { return delegate.tryProcess(); }
        @Override public boolean tryProcessWatermark(Watermark watermark) { return delegate.tryProcessWatermark(watermark); }
        @Override public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            return delegate.tryProcessWatermark(ordinal, watermark);
        }
        @Override public boolean completeEdge(int ordinal) { return delegate.completeEdge(ordinal); }
        @Override public boolean complete() { return delegate.complete(); }
        @Override public boolean saveToSnapshot() { return delegate.saveToSnapshot(); }
        @Override public boolean snapshotCommitPrepare() { return delegate.snapshotCommitPrepare(); }
        @Override public boolean snapshotCommitFinish(boolean success) { return delegate.snapshotCommitFinish(success); }
        @Override public void restoreFromSnapshot(Inbox inbox) { delegate.restoreFromSnapshot(inbox); }
        @Override public boolean finishSnapshotRestore() { return delegate.finishSnapshotRestore(); }
        @Override public void close() throws Exception { delegate.close(); }
        @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }
    }

    private static final class InitializedStandIn extends Initialized {
        InitializedStandIn(Processor delegate, String pipeline, long claim, long execution, long profile) {
            super(delegate, pipeline, claim, execution, profile);
        }
    }
}
