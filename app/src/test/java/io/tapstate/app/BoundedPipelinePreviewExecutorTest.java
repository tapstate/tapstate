package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.WriteMode;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.probe.PipelinePreviewEvent;
import io.tapstate.runtime.probe.PipelinePreviewRequest;
import io.tapstate.spi.store.StorePort;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class BoundedPipelinePreviewExecutorTest {

    @Test
    void interruptedCleanupWaitPreservesTheWorkerInterruptAndDefersMapCleanup() throws Exception {
        HazelcastInstance member = member("preview-interrupted-cleanup");
        BoundedPipelinePreviewExecutor executor = executor(member);
        CompletableFuture<Void> completion = new CompletableFuture<>() {
            @Override
            public Void get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
                Thread.currentThread().interrupt();
                return super.get(timeout, unit);
            }
        };
        try {
            Object stream = stream(executor);
            Job active = mock(Job.class);
            when(active.getFuture()).thenReturn(completion);
            AtomicReference<Job> runningJob = getField(stream, "job");
            runningJob.set(active);
            String inputMap = getField(stream, "inputMapName");
            member.<String, String>getMap(inputMap).put("sentinel", "still-owned-by-the-job");

            try {
                invoke(stream, "execute", new Class<?>[0]);
            } catch (CancellationException interruptedEmission) {
                // Restoring interruption also prevents the event queue from blocking this worker.
            }

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(active).cancel();
            Thread.interrupted();
            Map<String, String> waitingRows = member.getMap(inputMap);
            assertThat(waitingRows).containsEntry("sentinel", "still-owned-by-the-job");
            completion.complete(null);
            Map<String, String> cleanedRows = member.getMap(inputMap);
            assertThat(cleanedRows).isEmpty();
        } finally {
            Thread.interrupted();
            completion.complete(null);
            executor.close();
            member.shutdown();
        }
    }

    @Test
    void invalidCandidateEmitsSafeFailureAndReleasesItsPipelineLease() throws Exception {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("preview-executor-test-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        BoundedPipelinePreviewExecutor executor = new BoundedPipelinePreviewExecutor(
                unusedStore(), (ConnectorProvisioner) connectorId -> null, member, NestSettings.defaults(),
                java.time.Clock.systemUTC());
        try {
            var stream = executor.preview(new PipelinePreviewRequest(
                    "run-invalid", "author", "pipeline", "output", 3, "sample", "candidate-hash",
                    List.of("not: a tapstate resource"), Instant.now().plusSeconds(10)));
            PipelinePreviewEvent failed = stream.next();
            assertThat(failed.kind()).isEqualTo("run.failed");
            assertThat(failed.payload()).containsKeys("code", "reason");
            assertThat(failed.payload().get("code")).isEqualTo("dsl.unsupported-version");
            assertThat(member.<String, String>getMap(BoundedPipelinePreviewExecutor.LEASE_MAP).size()).isZero();
            assertThat(stream.next()).isNull();
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    @Test
    void validCandidateWithoutBoundedSourceFailsBeforeJetAndCleansTemporaryState() throws Exception {
        HazelcastInstance member = member("preview-executor-no-source");
        InMemoryStorePort store = new InMemoryStorePort();
        BoundedPipelinePreviewExecutor executor = new BoundedPipelinePreviewExecutor(
                store, (ConnectorProvisioner) connectorId -> null, member, NestSettings.defaults(),
                java.time.Clock.systemUTC());
        store.artifacts().save(new io.tapstate.core.model.SourceResource("missing_source", null, "test", Map.of(),
                io.tapstate.core.model.SourceMode.CDC, null, null, null));
        PipelineResource pipeline = new PipelineResource("pipeline", null,
                List.of(SourceRef.spec("missing_source", true)), List.of(),
                new ViewBlock.Inline("view", FromRef.literal("missing_source.orders"), "id", null),
                null, null, null);
        String canonical = new io.tapstate.core.model.canonical.CanonicalWriter().write(pipeline);
        try {
            var stream = executor.preview(new PipelinePreviewRequest("run-no-source", "author", "pipeline", "view",
                    1, "sample", "candidate-hash", List.of(canonical), Instant.now().plusSeconds(10)));

            PipelinePreviewEvent failed = stream.next();
            assertThat(failed.kind()).isEqualTo("run.failed");
            assertThat(failed.payload()).containsKeys("code", "reason");
            assertThat(member.<String, String>getMap(BoundedPipelinePreviewExecutor.LEASE_MAP).size()).isZero();
            assertThat(stream.next()).isNull();
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    @Test
    void boundsAndPrunesTheChosenOutputAndBuildsTraceDiagnostics() throws Exception {
        HazelcastInstance member = member("preview-executor-helpers");
        BoundedPipelinePreviewExecutor executor = executor(member);
        try {
            Object stream = stream(executor);
            PipelineResource pipeline = pipeline();
            PipelineResource selected = invoke(stream, "selectOutput", new Class<?>[] {PipelineResource.class,
                    String.class}, pipeline, "sync_out");
            assertThat(selected.view()).isNull();
            assertThat(((ServeBlock.Inline) selected.serve()).sync()).hasSize(1);
            assertThatThrownBy(() -> invoke(stream, "selectOutput", new Class<?>[] {PipelineResource.class,
                    String.class}, pipeline, "unknown")).isInstanceOf(TapstateException.class);

            PipelineResource pruned = invoke(stream, "pruneToOutput", new Class<?>[] {PipelineResource.class},
                    pipeline);
            assertThat(pruned.transforms()).extracting(Step::id).containsExactly("needed");
            PipelineResource isolated = invoke(stream, "isolatedPipeline", new Class<?>[] {PipelineResource.class},
                    pipeline);
            assertThat(isolated.id()).startsWith(".preview_");
            Settings bounded = invoke(stream, "boundedSettings", new Class<?>[] {Settings.class},
                    new Settings(null, 2_000, 8, "daily", null, "latest"));
            assertThat(bounded.batchSize()).isEqualTo(256);
            assertThat(bounded.parallelism()).isEqualTo(1);
            assertThat(bounded.schedule()).isNull();
            assertThat(((Settings) invoke(stream, "boundedSettings", new Class<?>[] {Settings.class}, (Object) null))
                    .batchSize()).isEqualTo(256);

            String traceMap = getField(stream, "traceMapName");
            member.<String, String>getMap(traceMap).put("1", JsonWriter.write(Map.of(
                    "nodeId", "needed", "inputAlias", "root", "rowsSeen", 4, "pointerPatterns", List.of("/id"))));
            member.<String, String>getMap(traceMap).put("2", JsonWriter.write(Map.of(
                    "nodeId", "needed", "rowsSeen", 3, "sampleBytes", 18, "truncated", false,
                    "pointerPatterns", List.of("/total"), "pointerPatternsComplete", true)));
            member.<String, String>getMap(traceMap).put("3", JsonWriter.write(Map.of(
                    "nodeId", "needed", "sample", Map.of("id", "one"))));
            List<Map<String, Object>> trace = invoke(stream, "readTrace", new Class<?>[] {PipelineResource.class},
                    pruned);
            assertThat(trace).singleElement().satisfies(node -> {
                assertThat(node).containsEntry("inputRows", 4L).containsEntry("outputRows", 3L)
                        .containsEntry("droppedRows", 1L).containsEntry("sampleCount", 1)
                        .containsEntry("pointerPatterns", List.of("/total"));
            });
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    @Test
    void stagesBoundedInputsReadsOrderedDocumentsAndBuildsSafeFailurePayloads() throws Exception {
        HazelcastInstance member = member("preview-executor-data-helpers");
        BoundedPipelinePreviewExecutor executor = executor(member);
        try {
            Object stream = stream(executor);
            String resultMapName = getField(stream, "resultMapName");
            member.<String, String>getMap(resultMapName).put("b", PreviewDocumentStorage.encode(Map.of("id", 2)));
            member.<String, String>getMap(resultMapName).put("a", PreviewDocumentStorage.encode(Map.of("id", 1)));
            assertThat((List<Map<String, Object>>) invoke(stream, "readDocuments", new Class<?>[0]))
                    .containsExactly(Map.of("id", 1L), Map.of("id", 2L));

            assertThatThrownBy(() -> invoke(stream, "stageSampleInputs", new Class<?>[] {Map.class}, Map.of()))
                    .isInstanceOf(TapstateException.class);
            Envelope row = Envelope.read(1L, "orders", Map.of("id", 1), Map.of());
            invoke(stream, "stageSampleInputs", new Class<?>[] {Map.class}, Map.of("orders", List.of(row)));
            String inputMapName = getField(stream, "inputMapName");
            assertThat(member.getMap(inputMapName).get("orders")).isInstanceOf(
                    io.tapstate.runtime.engine.FiniteEnvelopeSourceProcessor.Sample.class);

            Map<String, Object> coded = invoke(stream, "failurePayload", new Class<?>[] {Throwable.class},
                    new TapstateException(ActuationError.PREVIEW_REFUSED, Map.of("reason", "coded reason"), null));
            assertThat(coded).containsEntry("reason", "coded reason");
            Map<String, Object> generic = invoke(stream, "failurePayload", new Class<?>[] {Throwable.class},
                    new IllegalArgumentException("internal detail"));
            assertThat(generic).containsEntry("reason", "the preview could not be completed safely")
                    .doesNotContainValue("internal detail");
            assertThat((String) invoke(stream, "exceptionTypes", new Class<?>[] {Throwable.class},
                    new IllegalStateException("outer", new IllegalArgumentException("inner"))))
                    .contains("IllegalStateException", "IllegalArgumentException");
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    @Test
    void decodesTraceForFilterUnwindAndJsAndIgnoresMalformedFields() throws Exception {
        HazelcastInstance member = member("preview-executor-trace-shapes");
        BoundedPipelinePreviewExecutor executor = executor(member);
        try {
            Object stream = stream(executor);
            String traceMap = getField(stream, "traceMapName");
            member.<String, String>getMap(traceMap).put("00", JsonWriter.write(Map.of("sample", Map.of())));
            member.<String, String>getMap(traceMap).put("10", JsonWriter.write(Map.of(
                    "nodeId", "filtered", "inputAlias", "root", "rowsSeen", "bad",
                    "pointerPatterns", List.of("/id", 1))));
            member.<String, String>getMap(traceMap).put("11", JsonWriter.write(Map.of(
                    "nodeId", "filtered", "rowsSeen", "bad", "sampleBytes", 12,
                    "truncated", true, "pointerPatterns", "bad", "pointerPatternsComplete", true)));
            member.<String, String>getMap(traceMap).put("12", JsonWriter.write(Map.of(
                    "nodeId", "filtered", "sample", Map.of("id", 1))));
            member.<String, String>getMap(traceMap).put("20", JsonWriter.write(Map.of(
                    "nodeId", "expanded", "inputAlias", "root", "rowsSeen", 2,
                    "pointerPatterns", "bad")));
            member.<String, String>getMap(traceMap).put("21", JsonWriter.write(Map.of(
                    "nodeId", "expanded", "rowsSeen", 4, "sampleBytes", "bad",
                    "pointerPatterns", List.of(), "pointerPatternsComplete", false)));
            member.<String, String>getMap(traceMap).put("30", JsonWriter.write(Map.of(
                    "nodeId", "scripted", "inputAlias", "root", "rowsSeen", 4,
                    "pointerPatterns", List.of("/id"))));
            member.<String, String>getMap(traceMap).put("31", JsonWriter.write(Map.of(
                    "nodeId", "scripted", "rowsSeen", 4, "sampleBytes", 24,
                    "pointerPatterns", List.of("/id"))));

            FromClause input = FromClause.list(FromRef.literal("orders"));
            PipelineResource pipeline = new PipelineResource("pipeline", null,
                    List.of(SourceRef.spec("source", true)), List.of(
                            Step.inline("filtered", input, new TransformBody.Filter("id > 0"), null),
                            Step.inline("expanded", input,
                                    new TransformBody.Unwind("items", null, false, null, null), null),
                            Step.inline("scripted", input, new TransformBody.Js("emit(record)"), null)),
                    new ViewBlock.Inline("view", FromRef.literal("filtered"), "id", null), null, null, null);

            List<Map<String, Object>> trace = invoke(stream, "readTrace", new Class<?>[] {PipelineResource.class},
                    pipeline);
            assertThat(trace).hasSize(3);
            assertThat(trace.get(0)).containsEntry("nodeId", "filtered")
                    .containsEntry("inputRows", 0L).containsEntry("outputRows", 0L)
                    .containsEntry("droppedRows", 0L).containsEntry("sampleBytes", 12L)
                    .containsEntry("truncated", true).containsEntry("pointerPatterns", List.of());
            assertThat(trace.get(1)).containsEntry("nodeId", "expanded")
                    .containsEntry("inputRows", 2L).containsEntry("fanOutRows", 2L)
                    .containsEntry("sampleBytes", 0L);
            assertThat(trace.get(2)).containsEntry("nodeId", "scripted")
                    .containsEntry("determinism", "unknown").containsEntry("inputRows", 4L);
        } finally {
            executor.close();
            member.shutdown();
        }
    }

    private static BoundedPipelinePreviewExecutor executor(HazelcastInstance member) {
        return new BoundedPipelinePreviewExecutor(unusedStore(), (ConnectorProvisioner) connectorId -> null,
                member, NestSettings.defaults(), java.time.Clock.systemUTC());
    }

    private static HazelcastInstance member(String name) {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName(name + "-" + UUID.randomUUID());
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static Object stream(BoundedPipelinePreviewExecutor executor) throws Exception {
        Class<?> streamType = java.util.Arrays.stream(BoundedPipelinePreviewExecutor.class.getDeclaredClasses())
                .filter(type -> type.getSimpleName().equals("PreviewStream")).findFirst().orElseThrow();
        Constructor<?> constructor = streamType.getDeclaredConstructor(
                BoundedPipelinePreviewExecutor.class, PipelinePreviewRequest.class);
        constructor.setAccessible(true);
        return constructor.newInstance(executor, new PipelinePreviewRequest("run-helper", "author", "pipeline",
                "view_out", 3, "sample", "hash", List.of("pipeline"), Instant.now().plusSeconds(30)));
    }

    @SuppressWarnings("unchecked")
    private static <T> T invoke(Object target, String name, Class<?>[] parameterTypes, Object... arguments)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        try {
            return (T) method.invoke(target, arguments);
        } catch (java.lang.reflect.InvocationTargetException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw failure;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    private static PipelineResource pipeline() {
        Step dead = Step.inline("dead", FromClause.list(FromRef.literal("orders")),
                new TransformBody.Filter("true"), null);
        Step needed = Step.inline("needed", FromClause.list(FromRef.literal("orders")),
                new TransformBody.Filter("true"), null);
        ServeBlock.Inline serve = new ServeBlock.Inline("serve", FromRef.literal("needed"),
                List.of(new SyncElement("sync_out", "target", WriteMode.UPSERT, null, null)), null, null);
        return new PipelineResource("pipeline", null, List.of(SourceRef.spec("source", true)), List.of(dead, needed),
                new ViewBlock.Inline("view_out", FromRef.literal("needed"), "id", null), serve, null, null);
    }

    private static StorePort unusedStore() {
        return (StorePort) Proxy.newProxyInstance(StorePort.class.getClassLoader(), new Class<?>[] {StorePort.class},
                (proxy, method, args) -> {
                    throw new AssertionError("invalid candidate must fail before accessing store port");
                });
    }
}
