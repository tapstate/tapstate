package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Opt-in, same-thread timings; no command bodies, credentials, positions or exception messages. */
final class PipelineDiagnostics {
    static final String LOGGER_NAME = "io.tapstate.diagnostics.pipeline";
    private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);
    private static final ThreadLocal<Span> CURRENT = new ThreadLocal<>();
    private static final Span DISABLED = new Span();

    private PipelineDiagnostics() {}

    private static String identifier(String value) {
        return value == null || value.length() <= 128 ? value : "sha256:" + CanonicalHash.ofText(value);
    }

    static Span phase(Phase phase) { return open(null, null, phase, null); }

    static <T> T measure(String pipeline, Phase phase, Supplier<T> action) {
        try (var span = open(pipeline, null, phase, null)) {
            try { return action.get(); }
            catch (RuntimeException | Error failed) { span.failed(failed); throw failed; }
        }
    }

    static void retention(String capture, Runnable action) {
        try (var span = open(null, capture, Phase.CAPTURE_RETENTION, null)) {
            try { action.run(); }
            catch (RuntimeException | Error failed) { span.failed(failed); throw failed; }
        }
    }

    static Attempt attempt(String pipeline) { return new Attempt(pipeline, LOG.isDebugEnabled()); }

    static void intent(PipelineIntentSnapshot intent) {
        var current = CURRENT.get();
        if (current == null) return;
        var facts = current.root.facts;
        facts.put("pipeline_id", identifier(intent.scope().pipelineId()));
        facts.put("cluster_id", identifier(intent.scope().clusterId()));
        facts.put("binding_version", intent.scope().bindingVersion());
        facts.put("cache_incarnation", identifier(intent.scope().cacheIncarnation()));
        facts.put("state_generation", intent.scope().stateGeneration());
        facts.put("intent_revision", intent.intentRevision());
        if (!intent.deleted()) {
            facts.put("desired_state", intent.desired().targetState().name());
            facts.put("artifact_revision", identifier(intent.desired().revision()));
        }
        event("intent_loaded");
    }

    static void execution(ExecutionFence fence) {
        var current = CURRENT.get();
        if (current == null) return;
        var facts = current.root.facts;
        if (fence.scope() != null) {
            facts.put("cluster_id", identifier(fence.scope().clusterId()));
            facts.put("binding_version", fence.scope().bindingVersion());
            facts.put("cache_incarnation", identifier(fence.scope().cacheIncarnation()));
            facts.put("state_generation", fence.scope().stateGeneration());
        }
        facts.put("claim_generation", fence.claimGeneration());
        facts.put("execution_generation", fence.executionGeneration());
        facts.put("owner_node", identifier(fence.ownerNodeId()));
        facts.put("owner_boot", identifier(fence.ownerBootId()));
        facts.put("topology_revision", fence.topologyRevision());
        event("execution_bound");
    }

    static void source(String source) {
        var current = CURRENT.get();
        if (current != null) { current.details.put("source_id", identifier(source)); event("source_selected"); }
    }

    static void job(long id) {
        var current = CURRENT.get();
        if (current != null) current.root.facts.put("jet_job_id", Long.toUnsignedString(id, 16));
    }

    static void event(String outcome) {
        var current = CURRENT.get();
        if (current != null) current.write("decision", outcome);
    }

    static void failed(Throwable failure) {
        var current = CURRENT.get();
        if (current != null) { current.failed(failure); current.write("decision", "pass_failed"); }
    }

    private static Span open(String pipeline, String capture, Phase phase, Attempt attempt) {
        if (!LOG.isDebugEnabled()) return DISABLED;
        return new Span(CURRENT.get(), pipeline, capture, phase, attempt);
    }

    static final class Attempt {
        private final String pipeline;
        private final String id;
        private final long askedAt;
        private Attempt(String pipeline, boolean enabled) {
            this.pipeline = enabled ? identifier(pipeline) : pipeline;
            this.id = enabled ? UUID.randomUUID().toString() : null;
            this.askedAt = enabled ? System.nanoTime() : 0;
        }
        void admission(String outcome, Set<String> active) {
            if (id == null || !LOG.isDebugEnabled()) return;
            var snapshot = Set.copyOf(active);
            var value = base();
            value.put("edge", "admission"); value.put("outcome", outcome);
            value.put("active_count", snapshot.size());
            value.put("active_pipelines", snapshot.stream().sorted().limit(16).map(PipelineDiagnostics::identifier).toList());
            LOG.debug("{}", JsonWriter.write(value));
        }
        private Map<String, Object> base() {
            var value = new LinkedHashMap<String, Object>();
            value.put("event", "pipeline_diag"); value.put("at", Instant.now().toString());
            value.put("pipeline_id", pipeline); value.put("pass_id", id);
            value.put("thread", identifier(Thread.currentThread().getName()));
            value.put("thread_id", Thread.currentThread().threadId());
            return value;
        }
        void run(Runnable action) {
            try (var span = open(pipeline, null, Phase.PIPELINE_PASS, this)) {
                try { action.run(); }
                catch (RuntimeException | Error failed) { span.failed(failed); throw failed; }
            }
        }
    }

    static final class Span implements AutoCloseable {
        private final Span parent;
        private final Span root;
        private final Phase phase;
        private final long started;
        private final MongoHttpDiagnosticsScope measurement;
        private final MongoHttpDiagnosticsScope.Stage stage;
        private final Map<String, Object> facts;
        private final Map<String, Object> details;
        private final int sequence;
        private int nextSequence;
        private Throwable failure;
        private boolean closed;

        private Span() {
            parent = null; root = this; phase = null; started = 0; measurement = null;
            stage = null; facts = Map.of(); details = Map.of(); sequence = 0;
        }
        private Span(Span parent, String pipeline, String capture, Phase phase, Attempt attempt) {
            this.parent = parent; this.root = parent == null ? this : parent.root;
            this.phase = phase; this.started = System.nanoTime();
            this.measurement = parent == null ? MongoHttpDiagnosticsScope.open() : null;
            this.facts = parent == null ? new LinkedHashMap<>() : root.facts;
            this.details = parent == null ? new LinkedHashMap<>() : new LinkedHashMap<>(parent.details);
            if (parent == null) {
                facts.put("pass_id", attempt != null && attempt.id != null ? attempt.id : UUID.randomUUID().toString());
                if (pipeline != null) facts.put("pipeline_id", identifier(pipeline));
                if (capture != null) facts.put("capture_id", identifier(capture));
                if (attempt != null && attempt.id != null)
                    facts.put("dispatch_wait_ms", TimeUnit.NANOSECONDS.toMillis(started - attempt.askedAt));
            }
            sequence = ++root.nextSequence;
            stage = MongoHttpDiagnosticsScope.phase(phase);
            CURRENT.set(this);
            write("begin", "entered");
        }
        void failed(Throwable failed) { if (phase != null) failure = failed; }
        private void write(String edge, String outcome) {
            var value = new LinkedHashMap<>(facts);
            value.putAll(details);
            value.put("event", "pipeline_diag"); value.put("at", Instant.now().toString());
            value.put("thread", identifier(Thread.currentThread().getName()));
            value.put("thread_id", Thread.currentThread().threadId());
            value.put("phase", phase.name()); value.put("span_id", sequence);
            value.put("parent_span_id", parent == null ? 0 : parent.sequence);
            value.put("edge", edge); value.put("outcome", identifier(outcome));
            value.put("elapsed_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
            if (failure != null) {
                value.put("exception_type", failure.getClass().getSimpleName());
                if (failure instanceof TapstateException coded) value.put("error_code", coded.code().code());
            }
            if (edge.equals("end") && measurement != null) {
                value.put("measurement_scope", "synchronous_thread");
                value.putAll(measurement.snapshot());
            }
            LOG.debug("{}", JsonWriter.write(value));
        }
        @Override public void close() {
            if (phase == null || closed) return;
            closed = true;
            try { stage.close(); write("end", failure == null ? "closed" : "failed"); }
            finally {
                if (parent == null) CURRENT.remove(); else CURRENT.set(parent);
                if (measurement != null) measurement.close();
            }
        }
    }
}
