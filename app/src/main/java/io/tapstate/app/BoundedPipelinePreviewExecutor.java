package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.map.IMap;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkBoundedSnapshotQueryPort;
import io.tapstate.adapters.pdk.PdkTargetPreviewRenderer;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.EventJsonValues;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.runtime.engine.DagTraceBinding;
import io.tapstate.runtime.engine.FiniteEnvelopeSourceProcessor;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.probe.PipelinePreviewEvent;
import io.tapstate.runtime.probe.PipelinePreviewProbe;
import io.tapstate.runtime.probe.PipelinePreviewRequest;
import io.tapstate.runtime.probe.PipelinePreviewStream;
import io.tapstate.spi.capture.BoundedQueryCancellation;
import io.tapstate.spi.store.StorePort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Runs candidate Pipelines over exact bounded reads and request-private Jet state. */
final class BoundedPipelinePreviewExecutor implements PipelinePreviewProbe, AutoCloseable {

    static final String LEASE_MAP = "__preview.leases";
    private static final Logger LOG = LoggerFactory.getLogger(BoundedPipelinePreviewExecutor.class);
    private static final int MAX_WORKERS = 8;
    private static final int MAX_QUEUED_RUNS = 32;
    private static final int MAX_EVENTS = 32;
    private static final Duration LEASE_TTL = Duration.ofMinutes(5);
    private static final Duration CANCEL_JOIN = Duration.ofSeconds(2);
    private static final long MAX_RESULT_BYTES = 8L * 1024L * 1024L;

    private final StorePort storePort;
    private final ConnectorProvisioner connectors;
    private final HazelcastInstance member;
    private final NestSettings nestSettings;
    private final Clock clock;
    private final PreviewSelectionPlanner planner;
    private final PdkTargetPreviewRenderer targetPreviewRenderer;
    private final ThreadPoolExecutor workers;

    BoundedPipelinePreviewExecutor(StorePort storePort, ConnectorProvisioner connectors,
            HazelcastInstance member, NestSettings nestSettings, Clock clock) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.connectors = Objects.requireNonNull(connectors, "connectors");
        this.member = Objects.requireNonNull(member, "member");
        this.nestSettings = Objects.requireNonNull(nestSettings, "nestSettings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.planner = new PreviewSelectionPlanner(new PdkBoundedSnapshotQueryPort(connectors),
                new PreviewSampleCache(member), clock);
        this.targetPreviewRenderer = new PdkTargetPreviewRenderer(connectors);
        ThreadFactory threads = task -> {
            Thread thread = new Thread(task, "tapstate-preview-" + UUID.randomUUID());
            thread.setDaemon(true);
            return thread;
        };
        this.workers = new ThreadPoolExecutor(
                2, MAX_WORKERS, 30, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(MAX_QUEUED_RUNS), threads,
                new ThreadPoolExecutor.AbortPolicy());
        this.workers.allowCoreThreadTimeOut(true);
    }

    @Override
    public PipelinePreviewStream preview(PipelinePreviewRequest request) {
        Objects.requireNonNull(request, "request");
        PreviewStream stream = new PreviewStream(request);
        stream.start();
        return stream;
    }

    @Override
    public void close() {
        workers.shutdownNow();
    }

    private final class PreviewStream implements PipelinePreviewStream {

        private final PipelinePreviewRequest request;
        private final ArrayBlockingQueue<PipelinePreviewEvent> events = new ArrayBlockingQueue<>(MAX_EVENTS);
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicReference<Job> job = new AtomicReference<>();
        private final BoundedQueryCancellation queryCancellation = new BoundedQueryCancellation();
        private final String executionId;
        private final String inputMapName;
        private final String resultMapName;
        private final String traceMapName;
        private final String leaseBase;
        private volatile String leaseKey;
        private volatile Future<?> worker;
        private volatile boolean cacheHit;
        private boolean workerStarted;

        PreviewStream(PipelinePreviewRequest request) {
            this.request = request;
            this.executionId = "preview_" + UUID.randomUUID();
            this.inputMapName = "__preview.inputs." + executionId;
            this.resultMapName = "__preview.results." + executionId;
            this.traceMapName = "__preview.trace." + executionId;
            this.leaseBase = "pipeline:" + digest(request.principal() + "\n" + request.pipelineId());
        }

        void start() {
            leaseKey = acquireLease(leaseBase, request.runId());
            try {
                worker = workers.submit(this::execute);
            } catch (RejectedExecutionException overloaded) {
                releaseLease();
                throw refused("the preview service is at its bounded concurrency limit");
            }
        }

        @Override
        public boolean sampleCacheHit() {
            return cacheHit;
        }

        @Override
        public PipelinePreviewEvent next() throws InterruptedException {
            if (finished.get() && events.isEmpty()) {
                return null;
            }
            long remaining = Duration.between(clock.instant(), request.deadline()).toNanos();
            if (remaining <= 0) {
                cancel();
                return event("run.failed", failure(ActuationError.PREVIEW_REFUSED.code(),
                        "the preview exceeded its 15 second execution deadline"));
            }
            PipelinePreviewEvent next = events.poll(remaining, TimeUnit.NANOSECONDS);
            if (next != null) {
                return next;
            }
            if (finished.get()) {
                return null;
            }
            cancel();
            return event("run.failed", failure(ActuationError.PREVIEW_REFUSED.code(),
                    "the preview exceeded its 15 second execution deadline"));
        }

        @Override
        public void cancel() {
            boolean releaseBeforeStart;
            synchronized (this) {
                if (!cancelled.compareAndSet(false, true)) {
                    return;
                }
                releaseBeforeStart = !workerStarted;
            }
            queryCancellation.cancel();
            Job active = job.get();
            if (active != null) {
                active.cancel();
            }
            if (!releaseBeforeStart) {
                StatelessTransforms.cancelPreviewJs(executionId);
            }
            Future<?> task = worker;
            if (task != null) {
                task.cancel(true);
            }
            if (releaseBeforeStart) {
                releaseLease();
            }
        }

        private void execute() {
            synchronized (this) {
                if (cancelled.get()) {
                    finished.set(true);
                    releaseLease();
                    return;
                }
                workerStarted = true;
            }
            PipelineResource executionPipeline = null;
            Throwable failure = null;
            List<Map<String, Object>> documents = List.of();
            List<Map<String, Object>> trace = List.of();
            String resultFormat = PdkTargetPreviewRenderer.LOGICAL_JSON;
            PreviewSelectionPlanner.Sample sample = null;
            Set<String> stateMaps = Set.of();
            boolean jobJoined = true;
            boolean retainLease = false;
            try {
                checkActive();
                List<Resource> resources = parseCandidate();
                ReadOnlyArtifactSnapshot snapshot = ReadOnlyArtifactSnapshot.overlay(
                        storePort.artifacts(), resources);
                Resource candidate = snapshot.get(request.pipelineId()).orElse(null);
                if (!(candidate instanceof PipelineResource original)) {
                    throw refused("pipelineId does not identify a compiled Pipeline resource");
                }
                PipelineResource inlined = PipelineInlining.inline(original, snapshot);
                PipelineResource selected = selectOutput(inlined, request.outputId());
                executionPipeline = isolatedPipeline(pruneToOutput(selected));
                StoreBackedDagSource dagSource = new StoreBackedDagSource(storePort, nestSettings, snapshot);
                String sampleId = request.sampleId() == null ? request.runId() : request.sampleId();
                sample = planner.load(request.principal(), request.pipelineId(), sampleId,
                        executionPipeline, dagSource, request.rootLimit(),
                        request.deadline(), queryCancellation);
                stageSampleInputs(sample.rowsBySourceKey());
                checkActive();
                stateMaps = dagSource.previewStateMapNames(executionPipeline);
                cacheHit = sample.cacheHit();
                emit("sample.completed", Map.ofEntries(
                        Map.entry("sampleId", sampleId),
                        Map.entry("complete", true),
                        Map.entry("cacheHit", sample.cacheHit()),
                        Map.entry("cachedReads", sample.cachedReads()),
                        Map.entry("rootRows", sample.rootRows()),
                        Map.entry("inputRows", sample.inputRows()),
                        Map.entry("inputBytes", sample.inputBytes()),
                        Map.entry("queryCount", sample.queryCount()),
                        Map.entry("rootSourceKeys", sample.rootSourceKeys()),
                        Map.entry("rootTruncated", sample.rootTruncated()),
                        Map.entry("repeatable", sample.repeatable())));

                DagTraceBinding traceBinding = new DagTraceBinding(
                        nodeId -> PreviewTraceProcessor.metaSupplier(
                                "preview.trace." + digest(nodeId), nodeId, traceMapName),
                        (nodeId, alias) -> PreviewTraceProcessor.inputMetaSupplier(
                                "preview.trace.input." + digest(nodeId + "\u0000" + alias),
                                nodeId, alias, traceMapName));
                DAG dag = dagSource.previewDag(
                        executionPipeline, inputMapName, resultMapName, traceBinding, executionId);
                checkActive();
                JobConfig config = new JobConfig()
                        .setName(executionId)
                        .setProcessingGuarantee(com.hazelcast.jet.config.ProcessingGuarantee.NONE)
                        .setSplitBrainProtection(true)
                        .setAutoScaling(false);
                Job submitted = member.getJet().newJob(dag, config);
                job.set(submitted);
                waitForJob(submitted);
                documents = readDocuments();
                PdkTargetPreviewRenderer.Result rendered = targetPreviewRenderer.render(
                        selectedTarget(executionPipeline, snapshot), documents, this::checkActive);
                documents = rendered.documents();
                resultFormat = rendered.format();
                if (EventJsonValues.encodedSize(documents, MAX_RESULT_BYTES) > MAX_RESULT_BYTES) {
                    throw refused("the final preview result exceeds the 8 MiB response limit");
                }
                trace = readTrace(executionPipeline);
            } catch (Exception problem) {
                if (!(problem instanceof InterruptedException) || !cancelled.get()) {
                    failure = problem;
                    if (!(problem instanceof TapstateException)) {
                        LOG.warn("Preview run {} failed unexpectedly (exception types: {})",
                                request.runId(), exceptionTypes(problem));
                    }
                }
                if (problem instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                Job active = job.get();
                if (active != null && !active.getFuture().isDone()) {
                    active.cancel();
                    try {
                        active.getFuture().get(CANCEL_JOIN.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception stillEnding) {
                        jobJoined = active.getFuture().isDone();
                    }
                }
                if (jobJoined) {
                    boolean cleanupFailed = !cleanupTemporaryState(stateMaps);
                    try {
                        StatelessTransforms.finishPreviewJs(executionId);
                    } catch (RuntimeException cleanupFailure) {
                        LOG.warn("Could not close preview JavaScript contexts for {}",
                                executionId, cleanupFailure);
                        cleanupFailed = true;
                    }
                    if (cleanupFailed && failure == null) {
                        failure = refused("temporary preview state could not be cleaned up");
                    }
                    retainLease = cleanupFailed;
                } else {
                    retainLease = true;
                    if (failure == null) {
                        failure = refused("the preview job did not stop before temporary state cleanup");
                    }
                    Set<String> cleanupMaps = stateMaps;
                    active.getFuture().whenComplete((ignored, jobFailure) -> cleanupAfterUnjoinedJob(cleanupMaps));
                }
            }
            try {
                if (cancelled.get()) {
                    return;
                }
                if (failure != null) {
                    emit("run.failed", failurePayload(failure));
                    return;
                }
                for (Map<String, Object> node : trace) {
                    emit("step.completed", node);
                }
                emit("result.completed", Map.of(
                        "format", resultFormat,
                        "documents", documents,
                        "complete", true,
                        "rowCount", documents.size(),
                        "rootRows", sample == null ? 0 : sample.rootRows(),
                        "truncated", false));
                emit("run.completed", Map.of("rowCount", documents.size()));
            } finally {
                if (!retainLease) {
                    releaseLease();
                }
                finished.set(true);
            }
        }

        private List<Resource> parseCandidate() {
            DslParser parser = new DslParser();
            List<Resource> parsed = new ArrayList<>(request.canonicalResources().size());
            for (String canonical : request.canonicalResources()) {
                checkActive();
                parsed.add(parser.parse(canonical));
            }
            return List.copyOf(parsed);
        }

        private PipelineResource selectOutput(PipelineResource pipeline, String outputId) {
            ViewBlock selectedView = null;
            if (pipeline.view() instanceof ViewBlock.Inline view && view.id().equals(outputId)) {
                selectedView = view;
            }
            ServeBlock selectedServe = null;
            if (pipeline.serve() instanceof ServeBlock.Inline serve && serve.sync() != null) {
                for (int index = 0; index < serve.sync().size(); index++) {
                    SyncElement sync = serve.sync().get(index);
                    String id = sync.id() == null ? "sync_" + index : sync.id();
                    if (id.equals(outputId)) {
                        selectedServe = new ServeBlock.Inline(serve.id(), serve.from(), List.of(sync),
                                List.of(), List.of());
                        break;
                    }
                }
            }
            if (selectedView == null && selectedServe == null) {
                throw refused("outputId does not identify an executable preview terminal");
            }
            return new PipelineResource(pipeline.id(), pipeline.metadata(), pipeline.sources(),
                    pipeline.transforms(), selectedView, selectedServe,
                    boundedSettings(pipeline.settings()), pipeline.experimental());
        }

        private void stageSampleInputs(Map<String, List<Envelope>> rowsBySourceKey) {
            if (rowsBySourceKey.isEmpty()) {
                throw refused("the selected pipeline has no bounded source samples");
            }
            Map<String, FiniteEnvelopeSourceProcessor.Sample> batches = new LinkedHashMap<>();
            rowsBySourceKey.forEach((sourceKey, rows) ->
                    batches.put(sourceKey, new FiniteEnvelopeSourceProcessor.Sample(rows)));
            member.<String, FiniteEnvelopeSourceProcessor.Sample>getMap(inputMapName).putAll(batches);
        }

        private PipelineResource pruneToOutput(PipelineResource pipeline) {
            if (pipeline.transforms() == null || pipeline.transforms().isEmpty()) {
                return pipeline;
            }
            Map<String, io.tapstate.core.model.Step> byId = new LinkedHashMap<>();
            pipeline.transforms().forEach(step -> byId.put(step.id(), step));
            Set<String> required = new java.util.LinkedHashSet<>();
            List<io.tapstate.core.model.FromRef> pending = new ArrayList<>();
            if (pipeline.view() instanceof ViewBlock.Inline view) {
                pending.add(view.from());
            }
            if (pipeline.serve() instanceof ServeBlock.Inline serve) {
                pending.addAll(serve.from() instanceof io.tapstate.core.model.FromClause.Flow flow
                        ? flow.refs() : ((io.tapstate.core.model.FromClause.Aliases) serve.from())
                                .aliases().values());
            }
            while (!pending.isEmpty()) {
                io.tapstate.core.model.FromRef reference = pending.removeLast();
                if (!(reference instanceof io.tapstate.core.model.FromRef.Literal literal)) {
                    continue;
                }
                io.tapstate.core.model.Step step = byId.get(literal.ref());
                if (step == null || !required.add(step.id())) {
                    continue;
                }
                if (step.from() instanceof io.tapstate.core.model.FromClause.Flow flow) {
                    pending.addAll(flow.refs());
                } else {
                    pending.addAll(((io.tapstate.core.model.FromClause.Aliases) step.from())
                            .aliases().values());
                }
            }
            List<io.tapstate.core.model.Step> transforms = pipeline.transforms().stream()
                    .filter(step -> required.contains(step.id())).toList();
            return new PipelineResource(pipeline.id(), pipeline.metadata(), pipeline.sources(), transforms,
                    pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental());
        }

        private PipelineResource isolatedPipeline(PipelineResource pipeline) {
            // A leading dot is forbidden in DSL ids and keeps preview state out of user map namespaces.
            return new PipelineResource("." + executionId, pipeline.metadata(), pipeline.sources(),
                    pipeline.transforms(), pipeline.view(), pipeline.serve(),
                    boundedSettings(pipeline.settings()), pipeline.experimental());
        }

        private io.tapstate.core.model.Settings boundedSettings(io.tapstate.core.model.Settings settings) {
            if (settings == null) {
                return new io.tapstate.core.model.Settings(null, 256, 1, null, null, null);
            }
            int batch = settings.batchSize() == null ? 256 : Math.max(1, Math.min(settings.batchSize(), 256));
            return new io.tapstate.core.model.Settings(settings.errorPolicy(), batch, 1,
                    null, settings.readMode(), settings.startFrom());
        }

        private void waitForJob(Job submitted) throws Exception {
            long remaining = Duration.between(clock.instant(), request.deadline()).toNanos();
            if (remaining <= 0) {
                throw new TimeoutException("preview deadline elapsed before Jet execution");
            }
            try {
                submitted.getFuture().get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException timeout) {
                submitted.cancel();
                throw refused("the preview exceeded its 15 second execution deadline");
            } catch (java.util.concurrent.ExecutionException failed) {
                Throwable cause = failed.getCause();
                while (cause instanceof java.util.concurrent.ExecutionException
                        || cause instanceof java.util.concurrent.CompletionException) {
                    cause = cause.getCause();
                }
                if (cause instanceof TapstateException coded) {
                    throw coded;
                }
                throw refused("the isolated preview pipeline failed during execution");
            }
        }

        private String exceptionTypes(Throwable failureCause) {
            List<String> types = new ArrayList<>();
            Set<Throwable> seen = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
            for (Throwable current = failureCause;
                    current != null && types.size() < 8 && seen.add(current);
                    current = current.getCause()) {
                types.add(current.getClass().getName());
            }
            return String.join(" <- ", types);
        }

        private List<Map<String, Object>> readDocuments() {
            IMap<String, String> results = member.getMap(resultMapName);
            List<Map.Entry<String, String>> entries = new ArrayList<>(results.entrySet());
            entries.sort(Map.Entry.comparingByKey());
            List<Map<String, Object>> docs = new ArrayList<>(entries.size());
            for (Map.Entry<String, String> entry : entries) {
                String json = entry.getValue();
                if (json == null) {
                    continue;
                }
                docs.add(Collections.unmodifiableMap(PreviewDocumentStorage.decode(json)));
            }
            return List.copyOf(docs);
        }

        private SourceResource selectedTarget(
                PipelineResource pipeline, ReadOnlyArtifactSnapshot snapshot) {
            if (!(pipeline.serve() instanceof ServeBlock.Inline serve)
                    || serve.sync() == null || serve.sync().isEmpty()) {
                return null;
            }
            Resource target = snapshot.get(serve.sync().getFirst().source()).orElse(null);
            return target instanceof SourceResource source ? source : null;
        }

        private List<Map<String, Object>> readTrace(PipelineResource pipeline) {
            IMap<String, String> traces = member.getMap(traceMapName);
            List<String> keys = new ArrayList<>(traces.keySet());
            keys.sort(String::compareTo);
            Map<String, TraceNode> byNode = new LinkedHashMap<>();
            for (String key : keys) {
                Object parsed = JsonReader.parse(traces.get(key));
                if (parsed instanceof Map<?, ?> raw) {
                    String nodeId = raw.get("nodeId") instanceof String id ? id : null;
                    if (nodeId == null) {
                        continue;
                    }
                    TraceNode node = byNode.computeIfAbsent(nodeId, ignored -> new TraceNode());
                    if (raw.get("inputAlias") instanceof String alias) {
                        long count = number(raw.get("rowsSeen"));
                        node.inputRowsByAlias.put(alias, count);
                        node.inputPointerPatternsByAlias.put(alias, strings(raw.get("pointerPatterns")));
                    } else if (raw.containsKey("rowsSeen")) {
                        node.outputRows = number(raw.get("rowsSeen"));
                        node.sampleBytes = number(raw.get("sampleBytes"));
                        node.truncated = Boolean.TRUE.equals(raw.get("truncated"));
                        node.pointerPatterns = strings(raw.get("pointerPatterns"));
                        node.pointerPatternsComplete = Boolean.TRUE.equals(raw.get("pointerPatternsComplete"));
                    } else {
                        Map<String, Object> sample = new LinkedHashMap<>();
                        raw.forEach((name, value) -> sample.put((String) name, value));
                        node.samples.add(Collections.unmodifiableMap(sample));
                    }
                }
            }
            List<Map<String, Object>> nodes = new ArrayList<>(byNode.size());
            Map<String, String> stepTypes = new LinkedHashMap<>();
            if (pipeline.transforms() != null) {
                for (var step : pipeline.transforms()) {
                    if (step instanceof io.tapstate.core.model.Step.Inline inline) {
                        stepTypes.put(step.id(), inline.body().type());
                    }
                }
            }
            byNode.forEach((nodeId, traceNode) -> {
                long inputRows = traceNode.inputRowsByAlias.values().stream().mapToLong(Long::longValue).sum();
                Map<String, Object> node = new LinkedHashMap<>();
                node.put("nodeId", nodeId);
                node.put("inputRows", inputRows);
                node.put("inputRowsByAlias", Map.copyOf(traceNode.inputRowsByAlias));
                node.put("inputPointerPatternsByAlias", Map.copyOf(traceNode.inputPointerPatternsByAlias));
                node.put("outputRows", traceNode.outputRows);
                node.put("countComplete", true);
                node.put("sampleCount", traceNode.samples.size());
                node.put("sampleBytes", traceNode.sampleBytes);
                node.put("truncated", traceNode.truncated);
                node.put("samples", List.copyOf(traceNode.samples));
                node.put("pointerPatterns", traceNode.pointerPatterns);
                node.put("pointerPatternsComplete", traceNode.pointerPatternsComplete);
                String type = stepTypes.get(nodeId);
                if ("filter".equals(type)) {
                    node.put("droppedRows", Math.max(0, inputRows - traceNode.outputRows));
                } else if ("unwind".equals(type)) {
                    node.put("fanOutRows", Math.max(0, traceNode.outputRows - inputRows));
                }
                if ("js".equals(type)) {
                    node.put("determinism", "unknown");
                }
                nodes.add(Collections.unmodifiableMap(node));
            });
            return List.copyOf(nodes);
        }

        private long number(Object value) {
            return value instanceof Number number ? number.longValue() : 0;
        }

        private List<String> strings(Object value) {
            if (!(value instanceof List<?> values)) {
                return List.of();
            }
            return values.stream().filter(String.class::isInstance).map(String.class::cast).toList();
        }

        private final class TraceNode {
            private final Map<String, Long> inputRowsByAlias = new LinkedHashMap<>();
            private final Map<String, List<String>> inputPointerPatternsByAlias = new LinkedHashMap<>();
            private final List<Map<String, Object>> samples = new ArrayList<>();
            private List<String> pointerPatterns = List.of();
            private long outputRows;
            private long sampleBytes;
            private boolean truncated;
            private boolean pointerPatternsComplete;
        }

        private void emit(String kind, Map<String, Object> payload) {
            if (cancelled.get()) {
                return;
            }
            long remaining = Duration.between(clock.instant(), request.deadline()).toNanos();
            if (remaining <= 0) {
                throw refused("the preview exceeded its 15 second execution deadline");
            }
            try {
                if (!events.offer(new PipelinePreviewEvent(request.runId(), request.candidateHash(), 0,
                        kind, clock.instant(), payload), remaining, TimeUnit.NANOSECONDS)) {
                    throw refused("the preview event stream could not keep up before the deadline");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException("preview stream interrupted");
            }
        }

        private void checkActive() {
            queryCancellation.throwIfCancelled();
            if (cancelled.get()) {
                throw new java.util.concurrent.CancellationException("preview was cancelled");
            }
            if (!clock.instant().isBefore(request.deadline())) {
                throw refused("the preview exceeded its 15 second execution deadline");
            }
        }

        private Map<String, Object> failurePayload(Throwable problem) {
            if (problem instanceof TapstateException coded) {
                Object reason = coded.args().get("reason");
                return failure(coded.code().code(), reason instanceof String text
                        ? text : "the preview could not be completed safely");
            }
            return failure(ActuationError.PREVIEW_REFUSED.code(),
                    "the preview could not be completed safely");
        }

        private Map<String, Object> failure(String code, String reason) {
            return Map.of("code", code, "reason", reason);
        }

        private PipelinePreviewEvent event(String kind, Map<String, Object> payload) {
            return new PipelinePreviewEvent(request.runId(), request.candidateHash(), 0, kind,
                    clock.instant(), payload);
        }

        private String acquireLease(String base, String owner) {
            IMap<String, String> leases = member.getMap(LEASE_MAP);
            for (int slot = 0; slot < 2; slot++) {
                String key = base + ":" + slot;
                if (leases.putIfAbsent(key, owner, LEASE_TTL.toMillis(), TimeUnit.MILLISECONDS) == null) {
                    return key;
                }
            }
            throw refused("two previews are already running for this Pipeline");
        }

        private void releaseLease() {
            String key = leaseKey;
            if (key != null) {
                try {
                    member.getMap(LEASE_MAP).remove(key, request.runId());
                } catch (RuntimeException stopping) {
                    // Lease expiry is the fallback when the member itself is stopping.
                } finally {
                    leaseKey = null;
                }
            }
        }

        private void destroyMap(String name) {
            try {
                member.getMap(name).destroy();
            } catch (HazelcastInstanceNotActiveException stopping) {
                if (!cancelled.get()) {
                    throw stopping;
                }
            }
        }

        private boolean cleanupTemporaryState(Set<String> stateMaps) {
            List<String> cleanupTargets = new ArrayList<>();
            cleanupTargets.add(inputMapName);
            cleanupTargets.add(resultMapName);
            cleanupTargets.add(traceMapName);
            cleanupTargets.addAll(stateMaps);
            boolean cleaned = true;
            for (String target : cleanupTargets) {
                try {
                    destroyMap(target);
                } catch (RuntimeException cleanupFailure) {
                    LOG.warn("Could not destroy preview temporary map '{}'", target, cleanupFailure);
                    cleaned = false;
                }
            }
            return cleaned;
        }

        private void cleanupAfterUnjoinedJob(Set<String> stateMaps) {
            boolean cleaned = true;
            try {
                StatelessTransforms.finishPreviewJs(executionId);
            } catch (RuntimeException cleanupFailure) {
                LOG.warn("Could not close preview JavaScript contexts for {}",
                        executionId, cleanupFailure);
                cleaned = false;
            }
            if (!cleanupTemporaryState(stateMaps)) {
                cleaned = false;
            }
            if (cleaned) {
                releaseLease();
            } else {
                LOG.warn("Retaining preview lease for {} until its TTL expires after cleanup failure",
                        executionId);
            }
        }
    }

    private static TapstateException refused(String reason) {
        return new TapstateException(ActuationError.PREVIEW_REFUSED, Map.of("reason", reason), null);
    }

    private static Map<String, Object> failure(String code, String reason) {
        return Map.of("code", code, "reason", reason);
    }

    private static String digest(String value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
