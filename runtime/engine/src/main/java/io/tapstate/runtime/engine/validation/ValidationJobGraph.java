package io.tapstate.runtime.engine.validation;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.io.Serial;
import java.net.UnknownHostException;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

/**
 * A single-use validation graph whose real processors and data edges stay on one fixed member.
 * The controller admits that member and all I/O/byte resources before construction. Each processor
 * still guards the full owner/run authority, and blocking connector work belongs on admitted I/O
 * workers. This graph provides placement and job isolation, not resource admission, durable result
 * sealing or business completion. A normal Jet completion alone never completes a validation run.
 */
public final class ValidationJobGraph {
    public record Limits(int vertices, int edges, int edgeQueueSize) {
        public Limits {
            if (vertices < 1 || vertices > 1024 || edges < 0 || edges > 4096
                    || edgeQueueSize < 1 || edgeQueueSize > 1024) throw invalid("job.limits");
        }
    }

    private final ValidationRunIdentity run;
    private final String host;
    private final int port;
    private final String memberId;
    private final Limits limits;
    private final DAG dag = new DAG();
    private int edges;
    private boolean submitted;

    public ValidationJobGraph(ValidationRunIdentity run, Address selectedAddress, UUID selectedMember, Limits limits) {
        this.run = Objects.requireNonNull(run, "run");
        Objects.requireNonNull(selectedAddress, "selectedAddress");
        this.host = selectedAddress.getHost();
        this.port = selectedAddress.getPort();
        this.memberId = Objects.requireNonNull(selectedMember, "selectedMember").toString();
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    public synchronized void vertex(String name, ProcessorSupplier processors) {
        mutable();
        Objects.requireNonNull(processors, "processors");
        if (name == null || name.isBlank() || name.length() > 512) throw invalid("vertex.name");
        if (dag.vertices().size() == limits.vertices()) throw limit("jobVertices");
        dag.newVertex(name, new Pinned(processors, host, port, memberId, run.owner().pipelineId())).localParallelism(1);
    }

    public synchronized void edge(String source, int sourceOrdinal, String target, int targetOrdinal) {
        mutable();
        if (sourceOrdinal < 0 || sourceOrdinal > 1023 || targetOrdinal < 0 || targetOrdinal > 1023)
            throw invalid("edge.ordinal");
        if (edges == limits.edges()) throw limit("jobEdges");
        var from = Objects.requireNonNull(dag.getVertex(source), "source vertex");
        var to = Objects.requireNonNull(dag.getVertex(target), "target vertex");
        // Every real vertex is on this member. Local edges cannot route a row to a remote placeholder
        // or independently choose a destination by hashing a vertex name.
        dag.edge(Edge.from(from, sourceOrdinal).to(to, targetOrdinal).local()
                .setConfig(new EdgeConfig().setQueueSize(limits.edgeQueueSize())));
        edges++;
    }

    /** Run on admitted I/O; a failed or unknown submission must be reconciled, never blindly retried. */
    public synchronized Job submit(JetService jet, Runnable requireAuthorized) {
        mutable();
        Objects.requireNonNull(jet, "jet");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (dag.vertices().isEmpty()) throw new IllegalStateException("validation graph has no processors");
        submitted = true;
        requireAuthorized.run();
        dag.lock();
        // No name-based borrowing of an existing job, production identity or initial snapshot.
        return jet.newJob(dag, config());
    }

    private JobConfig config() {
        var owner = run.owner();
        return new JobConfig().setName("validation:" + run.runId())
                .setProcessingGuarantee(ProcessingGuarantee.NONE)
                .setAutoScaling(false).setSuspendOnFailure(false).setSplitBrainProtection(false)
                .setStoreMetricsAfterJobCompletion(false)
                .setArgument("validation.cluster", owner.clusterId())
                .setArgument("validation.pipeline", owner.pipelineId())
                .setArgument("validation.task", run.taskId()).setArgument("validation.run", run.runId())
                .setArgument("validation.mode", run.mode().name()).setArgument("validation.spec", run.specHash())
                .setArgument("validation.keyMappingRevision", run.keyMappingRevision())
                .setArgument("validation.rules", run.comparisonRulesVersion())
                .setArgument("validation.owner.node", owner.nodeId()).setArgument("validation.owner.boot", owner.bootId())
                .setArgument("validation.owner.claimGeneration", owner.claimGeneration())
                .setArgument("validation.owner.executionGeneration", owner.executionGeneration())
                .setArgument("validation.owner.topologyRevision", owner.topologyRevision())
                .setArgument("validation.owner.member", memberId);
    }

    private void mutable() {
        if (submitted) throw new IllegalStateException("validation graph submission has already been attempted");
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }
    private static TapstateException ownerUnavailable(String pipeline, Throwable cause) {
        return new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", pipeline), cause);
    }

    /** Plain serializable coordinates; Address itself cannot be captured by arbitrary Java suppliers. */
    private static final class Pinned implements ProcessorMetaSupplier {
        @Serial private static final long serialVersionUID = 1L;
        private final ProcessorSupplier supplier;
        private final String host;
        private final int port;
        private final String memberId;
        private final String pipeline;
        private transient ProcessorMetaSupplier delegate;
        private Pinned(ProcessorSupplier supplier, String host, int port, String memberId, String pipeline) {
            this.supplier = supplier;
            this.host = host;
            this.port = port;
            this.memberId = memberId;
            this.pipeline = pipeline;
        }
        private Address address() {
            try { return new Address(host, port); }
            catch (UnknownHostException missing) { throw ownerUnavailable(pipeline, missing); }
        }
        private ProcessorMetaSupplier delegate() {
            if (delegate == null) delegate = ProcessorMetaSupplier.forceTotalParallelismOne(supplier, address());
            return delegate;
        }
        @Override public int preferredLocalParallelism() { return 1; }
        @Override public void init(Context context) throws Exception {
            Address target = address();
            boolean present = context.hazelcastInstance().getCluster().getMembers().stream()
                    .anyMatch(member -> !member.isLiteMember() && member.getAddress().equals(target)
                            && member.getUuid().toString().equals(memberId));
            if (!present) throw ownerUnavailable(pipeline, null);
            delegate().init(context);
        }
        @Override public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            return delegate().get(addresses);
        }
        @Override public void close(Throwable error) throws Exception { delegate().close(error); }
        @Override public boolean isReusable() { return false; }
        @Override public boolean initIsCooperative() { return delegate().initIsCooperative(); }
        @Override public boolean closeIsCooperative() { return delegate().closeIsCooperative(); }
        @Override public Permission getRequiredPermission() { return delegate().getRequiredPermission(); }
        @Override public Map<String, String> getTags() { return delegate().getTags(); }
    }
}
