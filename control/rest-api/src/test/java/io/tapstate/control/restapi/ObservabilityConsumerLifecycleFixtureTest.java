package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ControlApiSchema;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CurrentObservationReader;
import io.tapstate.control.core.EventsCursorCodec;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PipelineEventsQueryService;
import io.tapstate.control.core.PipelineExplainService;
import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.model.Resource;
import io.tapstate.messages.EventCatalog;
import io.tapstate.messages.ExplanationCatalog;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.WorkloadClaim;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP projections and an executable request-order trace for consumers of those projections. */
class ObservabilityConsumerLifecycleFixtureTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PIPELINE = "orders";
    private static final String CLUSTER = "consumer-fixture";
    private static final Instant NOW = Instant.parse("2026-09-20T10:10:00Z");
    private static final Instant START = NOW.minusSeconds(600);
    private static final List<String> CURRENT_READS = List.of("status", "metrics", "snapshot", "explain");
    private static ConfigurableApplicationContext context;
    private static RestClient client;
    private Fixture fixture;
    private String bearer;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class).properties("server.port=0").run();
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        client = RestClient.create("http://127.0.0.1:" + port);
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    void seedStoppedPipeline() {
        fixture = context.getBean(Fixture.class);
        fixture.reset();
        bearer = "Bearer " + context.getBean(TokenService.class).create(Scope.READ);
        fixture.publish(PipelineState.STOPPED, 42, START, NOW.minusSeconds(1));
    }

    @Test
    void acceptedStartWithoutACurrentPublicationIs404WhileCurrentPendingAndStaleRemain200() throws Exception {
        for (String face : CURRENT_READS) {
            assertThat(read(face).status()).as("stopped pipeline " + face).isEqualTo(200);
        }
        assertSchema("pipeline.explain", read("explain").body());
        fixture.pending = new Pending(PendingReason.START_CAPACITY);
        fixture.generations.current = 2;

        // Retain the old latest row: the scoped reader, rather than fixture cleanup, must reject it.
        assertThat(fixture.observations.readStored(PIPELINE)).isPresent();
        for (String face : CURRENT_READS) {
            assertError(read(face), 404, "monitor.no-observation", Map.of("pipeline", PIPELINE));
        }
        assertSchema("pipeline.metrics.history", get(historyPath(100, null)).body());
        assertSchema("pipeline.events", get(eventsPath(100, null)).body());

        fixture.publish(PipelineState.NEW, 1, NOW.minusSeconds(1), NOW.minusSeconds(1));
        HttpResult waitingToStart = read("explain");
        assertThat(waitingToStart.status()).isEqualTo(200);
        assertSchema("pipeline.explain", waitingToStart.body());
        assertThat(waitingToStart.body()).containsEntry("state", "NEW")
                .containsEntry("pending", Map.of("reason", "START_CAPACITY"));
        assertThat(waitingToStart.body().keySet()).containsExactlyInAnyOrderElementsOf(
                golden("explain-start-pending.golden.json").keySet());

        fixture.pending = new Pending(PendingReason.STOP_CAPACITY);
        fixture.publish(PipelineState.RUNNING, 2, NOW.minusSeconds(1), NOW);
        HttpResult waitingToStop = read("explain");
        assertThat(waitingToStop.status()).isEqualTo(200);
        assertSchema("pipeline.explain", waitingToStop.body());
        assertThat(waitingToStop.body()).containsEntry("state", "RUNNING")
                .containsEntry("pending", Map.of("reason", "STOP_CAPACITY"));

        fixture.pending = null;
        fixture.generations.current = 3;
        fixture.publish(PipelineState.RUNNING, 2, START, NOW.minusSeconds(45));
        HttpResult stale = read("explain");
        assertThat(stale.status()).isEqualTo(200);
        assertSchema("pipeline.explain", stale.body());
        assertThat(stale.body()).containsEntry("kind", "OBSERVATION_STALE")
                .containsEntry("freshness", "STALE").doesNotContainKey("pending");
        assertThat(stale.body().keySet()).containsExactlyInAnyOrderElementsOf(
                golden("explain-stale.golden.json").keySet());
    }

    @Test
    void theExistingSixStatesAndPublicRequestSelectorsRemainClosed() {
        assertThat(PipelineState.values()).extracting(Enum::name).containsExactly(
                "NEW", "RUNNING", "PAUSED", "STOPPED", "COMPLETED", "FAILED");
        for (PipelineState state : PipelineState.values()) {
            fixture.publish(state, 42, START, NOW.minusSeconds(1));
            HttpResult status = read("status");
            HttpResult explain = read("explain");
            assertThat(status.status()).isEqualTo(200);
            assertThat(status.body()).containsEntry("state", state.name());
            assertThat(explain.status()).isEqualTo(200);
            assertThat(explain.body()).containsEntry("state", state.name());
            assertSchema("pipeline.explain", explain.body());
        }
        assertThat(requestProperties("pipeline.logs")).containsExactlyInAnyOrder("id", "limit", "scope");
        for (String face : CURRENT_READS) {
            assertThat(requestProperties("pipeline." + face)).as(face + " request selectors").containsExactly("id");
        }
        assertThat(requestProperties("pipeline.metrics.history"))
                .containsExactlyInAnyOrder("id", "from", "to", "resolution", "limit", "table", "cursor");
        assertThat(requestProperties("pipeline.events"))
                .containsExactlyInAnyOrder("id", "from", "to", "limit", "cursor");
    }

    @Test
    void seededResumeContinuityAndStopStartResetProjectAsDifferentHistoryBoundaries() throws Exception {
        // The runtime's continuity tests own counter production. This fixture owns its consumer projection.
        fixture.history.add(START, 100, START, "inc-old", 1);
        fixture.history.add(START.plusSeconds(60), 160, START, "inc-old", 1);
        fixture.history.add(START.plusSeconds(120), 220, START, "inc-old", 2);
        fixture.history.add(START.plusSeconds(180), 280, START, "inc-old", 2);
        HttpResult resumed = get(historyPath(100, null));
        assertThat(resumed.status()).isEqualTo(200);
        assertSchema("pipeline.metrics.history", resumed.body());
        assertThat(segmentReasons(resumed.body())).containsExactly("WINDOW_START", "CONTINUATION")
                .doesNotContain("COUNTER_RESET");
        List<Map<String, Object>> resumedSegments = objects(resumed.body().get("segments"));
        Map<String, Object> continuedPoint = objects(resumedSegments.getLast().get("points")).getLast();
        assertThat(((Number) object(continuedPoint.get("recordsOut")).get("delta")).longValue())
                .isEqualTo(60);

        fixture.history.add(START.plusSeconds(240), 1, START.plusSeconds(240), "inc-old", 3);
        fixture.history.add(START.plusSeconds(300), 61, START.plusSeconds(240), "inc-old", 3);
        HttpResult restarted = get(historyPath(100, null));
        assertSchema("pipeline.metrics.history", restarted.body());
        assertThat(segmentReasons(restarted.body()))
                .containsExactly("WINDOW_START", "CONTINUATION", "COUNTER_RESET");
        assertThat(restarted.body().keySet()).containsExactlyInAnyOrderElementsOf(
                golden("history-aggregate-boundaries.golden.json").keySet());
    }

    @Test
    void recreatedIdExcludesRetainedOldRowsAndBothLogScopesKeepTheirOriginalShape() throws Exception {
        seedHistoryAndEvents();
        fixture.logs.append(PIPELINE, new LogSink.Scope("inc-old", 1), line("old run"));
        fixture.logs.append(PIPELINE, new LogSink.Scope("inc-old", 2), line("resumed old run"));
        fixture.generations.current = 2;
        assertLogs("current", "resumed old run");
        assertLogs("incarnation", "old run", "resumed old run");
        String oldHistoryCursor = (String) get(historyPath(1, null)).body().get("nextCursor");
        String oldEventsCursor = (String) get(eventsPath(1, null)).body().get("nextCursor");
        assertThat(oldHistoryCursor).isNotBlank();
        assertThat(oldEventsCursor).isNotBlank();

        fixture.artifacts.remove();
        assertError(get(eventsPath(1, null)), 404, "lifecycle.unknown-pipeline", Map.of("pipeline", PIPELINE));
        assertLogs("current");
        assertLogs("incarnation");
        fixture.artifacts.put("inc-new");
        fixture.generations.current = 3;
        for (String face : CURRENT_READS) {
            assertError(read(face), 404, "monitor.no-observation", Map.of("pipeline", PIPELINE));
        }
        HttpResult emptyHistory = get(historyPath(100, null));
        assertSchema("pipeline.metrics.history", emptyHistory.body());
        assertThat(emptyHistory.body()).containsEntry("segments", List.of())
                .containsEntry("nextCursor", null);
        assertThat(emptyHistory.body().keySet()).containsExactlyInAnyOrderElementsOf(
                golden("history-empty.golden.json").keySet());
        HttpResult emptyEvents = get(eventsPath(100, null));
        assertSchema("pipeline.events", emptyEvents.body());
        assertThat(emptyEvents.body()).containsEntry("events", List.of())
                .containsEntry("knownGaps", List.of()).containsEntry("completeness", "BEST_EFFORT");
        assertLogs("current");
        assertLogs("incarnation");

        fixture.history.add(START.plusSeconds(300), 7, START.plusSeconds(300), "inc-new", 3);
        fixture.events.append(event("new-event", "inc-new", 3, START.plusSeconds(300)));
        fixture.logs.append(PIPELINE, new LogSink.Scope("inc-new", 3), line("new run"));
        fixture.logs.append(PIPELINE, new LogSink.Scope("inc-new", 4), line("resumed new run"));
        fixture.publish(PipelineState.RUNNING, 7, START.plusSeconds(300), NOW);
        assertThat(objects(get(eventsPath(100, null)).body().get("events")))
                .extracting(event -> event.get("id")).containsExactly("new-event");
        assertThat(objects(get(historyPath(100, null)).body().get("segments")))
                .flatExtracting(segment -> objects(segment.get("points")))
                .extracting(point -> point.get("intervalEnd"))
                .containsExactly(START.plusSeconds(300).toString());
        assertLogs("current", "new run");
        assertLogs("incarnation", "new run", "resumed new run");
        assertError(get(historyPath(1, oldHistoryCursor)), 400, "monitor.invalid-cursor",
                Map.of("operation", "pipeline.metrics.history", "reason", "QUERY_MISMATCH"));
        assertError(get(eventsPath(1, oldEventsCursor)), 400, "monitor.invalid-cursor",
                Map.of("operation", "pipeline.events", "reason", "QUERY_MISMATCH"));
    }

    @Test
    void clientTraceInvalidatesSameIdViewsAndCursorsAndIgnoresOlderRequestResponses() throws Exception {
        seedHistoryAndEvents();
        fixture.logs.append(PIPELINE, new LogSink.Scope("inc-old", 1), line("old run"));
        Consumer consumer = new Consumer();
        List<Map<String, Object>> trace = new ArrayList<>();
        for (String view : List.of("latest", "history", "events", "logs")) {
            String path = switch (view) {
                case "latest" -> path("metrics");
                case "history" -> historyPath(1, null);
                case "events" -> eventsPath(1, null);
                case "logs" -> path("logs?scope=current");
                default -> throw new IllegalStateException("unknown fixture view");
            };
            Request request = consumer.request(view, path);
            HttpResult result = get(path);
            trace.add(consumer.receive("initial " + view, request, result));
        }
        Request delayed = consumer.request("latest", path("metrics"));
        HttpResult oldLatest = read("metrics");
        fixture.generations.current = 2;
        fixture.pending = new Pending(PendingReason.START_PENDING);
        trace.add(consumer.invalidate("accepted start"));
        Request unpublished = consumer.request("latest", path("metrics"));
        trace.add(consumer.receive("await current publication", unpublished, read("metrics")));
        trace.add(consumer.receive("late pre-start response", delayed, oldLatest));
        fixture.pending = null;
        fixture.publish(PipelineState.RUNNING, 1, NOW.minusSeconds(1), NOW);
        Request published = consumer.request("latest", path("metrics"));
        HttpResult beforeRecreation = read("metrics");
        trace.add(consumer.receive("new execution publication", published, beforeRecreation));

        Request oldWindow = consumer.request("history", historyPath(1, null));
        HttpResult oldWindowPage = get(oldWindow.path());
        String changedWindow = historyPath(START.plusSeconds(60), 1, null);
        Request newWindow = consumer.request("history", changedWindow);
        trace.add(consumer.receive("changed history window", newWindow, get(changedWindow)));
        trace.add(consumer.receive("late old history window", oldWindow, oldWindowPage));

        fixture.artifacts.remove();
        trace.add(consumer.invalidate("deleted pipeline"));
        Request deleted = consumer.request("latest", path("metrics"));
        trace.add(consumer.receive("deleted read", deleted, read("metrics")));
        fixture.artifacts.put("inc-new");
        fixture.generations.current = 3;
        trace.add(consumer.invalidate("same id recreated"));
        Request recreated = consumer.request("latest", path("metrics"));
        trace.add(consumer.receive("recreated without publication", recreated, read("metrics")));
        trace.add(consumer.receive("late previous resource", published, beforeRecreation));
        fixture.publish(PipelineState.RUNNING, 7, NOW.minusSeconds(1), NOW);
        Request current = consumer.request("latest", path("metrics"));
        trace.add(consumer.receive("recreated publication", current, read("metrics")));
        trace.add(consumer.receive("late previous resource history", oldWindow, oldWindowPage));

        Map<String, Object> artifact = Map.of("contractVersion", "v1", "pipelineId", PIPELINE, "trace", trace);
        assertThat(JSON.readTree(JSON.writeValueAsString(artifact)))
                .isEqualTo(JSON.readTree(goldenText("consumer-lifecycle-trace.golden.json")));
    }

    private void seedHistoryAndEvents() {
        fixture.history.add(START, 100, START, "inc-old", 1);
        fixture.history.add(START.plusSeconds(60), 160, START, "inc-old", 1);
        fixture.history.add(START.plusSeconds(120), 220, START, "inc-old", 1);
        fixture.events.append(event("old-event-1", "inc-old", 1, START));
        fixture.events.append(event("old-event-2", "inc-old", 1, START.plusSeconds(60)));
    }

    private static PipelineEvent event(String id, String incarnation, long generation, Instant at) {
        return new PipelineEvent(id, PIPELINE, incarnation, generation, PipelineEvent.Kind.STATE_CHANGED,
                at, PipelineState.NEW, PipelineState.RUNNING, null, null, null);
    }

    private static LogLine line(String message) {
        return new LogLine(NOW.toEpochMilli(), "INFO", message);
    }

    private void assertLogs(String scope, String... messages) {
        HttpResult result = read("logs?scope=" + scope);
        assertThat(result.status()).isEqualTo(200);
        assertThat(result.body().keySet()).containsExactlyInAnyOrder("pipelineId", "lines");
        assertThat(result.body()).containsEntry("pipelineId", PIPELINE);
        List<Map<String, Object>> lines = objects(result.body().get("lines"));
        assertThat(lines).extracting(line -> line.get("message")).containsExactly((Object[]) messages);
        for (Map<String, Object> line : lines) {
            assertThat(line.keySet()).containsExactlyInAnyOrder("timestampMillis", "level", "message");
            assertThat(line.get("timestampMillis")).isEqualTo(NOW.toEpochMilli());
        }
    }

    private HttpResult read(String face) {
        return get(path(face));
    }

    private HttpResult get(String path) {
        return client.get().uri(path).header("Authorization", bearer).exchange((request, response) -> {
            assertThat(response.getHeaders().getCacheControl()).as(path + " cache policy").contains("no-store");
            Map<String, Object> body = response.bodyTo(new ParameterizedTypeReference<>() {});
            assertThat(body).as(path + " JSON object").isNotNull();
            return new HttpResult(response.getStatusCode().value(), body);
        });
    }

    private static void assertError(HttpResult result, int status, String code, Map<String, String> params) {
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.body()).containsEntry("code", code).containsEntry("params", params);
    }

    private static String path(String face) {
        return "/api/pipelines/" + PIPELINE + "/" + face;
    }

    private static String historyPath(int limit, String cursor) {
        return historyPath(START, limit, cursor);
    }

    private static String historyPath(Instant from, int limit, String cursor) {
        return path("metrics/history?from=" + from + "&to=" + NOW + "&resolution=raw&limit=" + limit)
                + (cursor == null ? "" : "&cursor=" + cursor);
    }

    private static String eventsPath(int limit, String cursor) {
        return path("events?from=" + START + "&to=" + NOW + "&limit=" + limit)
                + (cursor == null ? "" : "&cursor=" + cursor);
    }

    private static List<String> segmentReasons(Map<String, Object> page) {
        return objects(page.get("segments")).stream().map(segment -> (String) segment.get("startReason"))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> objects(Object value) {
        return (List<Map<String, Object>>) value;
    }

    private static java.util.Set<String> requestProperties(String operation) {
        return object(ControlApiSchema.resolve(ControlApiSchema.ref(operation).params()).get("properties"))
                .keySet();
    }

    private static void assertSchema(String operation, Map<String, Object> result) {
        conformsTo(ControlApiSchema.resolve(ControlApiSchema.ref(operation).result()), result, operation);
    }

    /** Validate only the schema features used by these closed result contracts; unknown features fail. */
    private static void conformsTo(Map<?, ?> schema, Object value, String path) {
        for (Object feature : schema.keySet()) {
            assertThat(List.of("type", "description", "properties", "additionalProperties", "required",
                    "oneOf", "enum", "items", "maxItems", "uniqueItems", "minLength", "format", "pattern",
                    "minimum", "maximum").contains(feature)).as(path + " supported schema feature").isTrue();
        }
        if (schema.get("oneOf") instanceof List<?> alternatives) {
            List<?> matching = alternatives.stream().filter(branch -> typeMatches((Map<?, ?>) branch, value))
                    .toList();
            assertThat(matching).as(path + " matches one schema branch").hasSize(1);
            conformsTo((Map<?, ?>) matching.getFirst(), value, path);
            return;
        }
        if (schema.get("enum") instanceof List<?> values) {
            assertThat(values.contains(value)).as(path + " enum").isTrue();
        }
        String type = (String) schema.get("type");
        if (type == null) {
            assertThat(schema.keySet()).as(path + " unconstrained evidence")
                    .isEqualTo(java.util.Set.of("description"));
            return;
        }
        assertThat(typeMatches(schema, value)).as(path + " type " + type).isTrue();
        switch (type) {
            case "object" -> {
                Map<?, ?> found = (Map<?, ?>) value;
                Map<?, ?> properties = (Map<?, ?>) schema.get("properties");
                if (schema.get("required") instanceof List<?> required) {
                    for (Object name : required) {
                        assertThat(found.containsKey(name)).as(path + "." + name + " required").isTrue();
                    }
                }
                for (var entry : found.entrySet()) {
                    Object property = properties == null ? null : properties.get(entry.getKey());
                    if (property instanceof Map<?, ?> nested) {
                        conformsTo(nested, entry.getValue(), path + "." + entry.getKey());
                    } else {
                        assertThat(schema.get("additionalProperties")).as(path + " undeclared field " + entry.getKey())
                                .isEqualTo(true);
                    }
                }
            }
            case "array" -> {
                List<?> items = (List<?>) value;
                if (schema.get("maxItems") instanceof Number max) {
                    assertThat(items.size()).as(path + " bounded array").isLessThanOrEqualTo(max.intValue());
                }
                if (Boolean.TRUE.equals(schema.get("uniqueItems"))) {
                    assertThat(items).as(path + " unique items").doesNotHaveDuplicates();
                }
                for (int index = 0; index < items.size(); index++) {
                    conformsTo((Map<?, ?>) schema.get("items"), items.get(index), path + "[" + index + "]");
                }
            }
            case "string" -> {
                String text = (String) value;
                if (schema.get("minLength") instanceof Number min) {
                    assertThat(text.length()).as(path + " length").isGreaterThanOrEqualTo(min.intValue());
                }
                if ("date-time".equals(schema.get("format"))) {
                    OffsetDateTime.parse(text);
                }
                if (schema.get("pattern") instanceof String pattern) {
                    assertThat(text).as(path + " pattern").containsPattern(pattern);
                }
            }
            case "number", "integer" -> {
                BigDecimal number = new BigDecimal(value.toString());
                if (schema.get("minimum") instanceof Number minimum) {
                    assertThat(number).as(path + " minimum").isGreaterThanOrEqualTo(
                            new BigDecimal(minimum.toString()));
                }
                if (schema.get("maximum") instanceof Number maximum) {
                    assertThat(number).as(path + " maximum").isLessThanOrEqualTo(
                            new BigDecimal(maximum.toString()));
                }
            }
            case "null" -> { }
            default -> throw new IllegalStateException("unsupported fixture schema type: " + type);
        }
    }

    private static boolean typeMatches(Map<?, ?> schema, Object value) {
        return switch ((String) schema.get("type")) {
            case "object" -> value instanceof Map;
            case "array" -> value instanceof List;
            case "string" -> value instanceof String;
            case "number" -> value instanceof Number;
            case "integer" -> value instanceof Number number
                    && new BigDecimal(number.toString()).stripTrailingZeros().scale() <= 0;
            case "null" -> value == null;
            default -> throw new IllegalStateException("unsupported fixture schema type");
        };
    }

    private static Map<String, Object> golden(String name) throws IOException {
        return JSON.readValue(goldenText(name), Map.class);
    }

    private static String goldenText(String name) throws IOException {
        try (var input = ObservabilityConsumerLifecycleFixtureTest.class.getResourceAsStream(
                "/golden/observability/" + name)) {
            if (input == null) {
                throw new IOException("missing consumer fixture: " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private record HttpResult(int status, Map<String, Object> body) { }

    /** Versions belong to this consumer; they never depend on private server identity fields. */
    private record Request(long version, long resourceVersion, String view, String path) { }

    private static final class Consumer {
        private long sequence;
        private long resourceVersion;
        private final Map<String, Request> requests = new TreeMap<>();
        private final Map<String, Map<String, Object>> views = new TreeMap<>();
        private final Map<String, String> cursors = new TreeMap<>();

        Request request(String view, String path) {
            Request previous = requests.get(view);
            if (previous != null && !previous.path().equals(path)) {
                views.remove(view);
                cursors.remove(view);
            }
            Request next = new Request(++sequence, resourceVersion, view, path);
            requests.put(view, next);
            return next;
        }

        Map<String, Object> receive(String action, Request request, HttpResult response) {
            Request current = requests.get(request.view());
            String disposition;
            if (request.resourceVersion() != resourceVersion || !request.equals(current)) {
                disposition = "IGNORED";
            } else if (response.status() == 200) {
                disposition = "ACCEPTED";
                views.put(request.view(), response.body());
                if (response.body().get("nextCursor") instanceof String cursor) {
                    cursors.put(request.view(), cursor);
                } else {
                    cursors.remove(request.view());
                }
            } else {
                disposition = "monitor.no-observation".equals(response.body().get("code"))
                        ? "PENDING" : "NOT_FOUND";
                views.remove(request.view());
                cursors.remove(request.view());
            }
            return step(action, request.version(), response.status(), disposition);
        }

        Map<String, Object> invalidate(String action) {
            resourceVersion++;
            requests.clear();
            views.clear();
            cursors.clear();
            return step(action, null, null, "INVALIDATED");
        }

        private Map<String, Object> step(String action, Long requestVersion, Integer status, String disposition) {
            Map<String, Object> step = new LinkedHashMap<>();
            step.put("action", action);
            step.put("requestVersion", requestVersion);
            step.put("resourceVersion", resourceVersion);
            step.put("responseStatus", status);
            step.put("disposition", disposition);
            step.put("cachedViews", List.copyOf(views.keySet()));
            step.put("cursorViews", List.copyOf(cursors.keySet()));
            Map<String, Object> latest = views.get("latest");
            step.put("latestCount", latest == null ? null : object(latest.get("metrics")).get("recordCount"));
            return step;
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class,
            PipelineObservationController.class, PipelineObservabilityController.class,
            PipelineEventsController.class, PipelineLogsController.class, ApiExceptionHandler.class})
    static class TestApp {
        @Bean Clock clock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
        @Bean Fixture fixture() { return new Fixture(); }
        @Bean JsonMapperBuilderCustomizer strictRequestShape() { return new ControlHttpFace().sourceJsonContract(); }
        @Bean ArtifactQueryService artifactQueryService(Fixture fixture) {
            return new ArtifactQueryService(fixture.artifacts);
        }
        @Bean CurrentObservationReader currentObservationReader(Fixture fixture) {
            return new CurrentObservationReader(fixture.artifacts, fixture.generations, fixture.observations, CLUSTER);
        }
        @Bean PipelineObservationQueryService pipelineObservationQueryService(
                ArtifactQueryService artifacts, CurrentObservationReader observations) {
            return new PipelineObservationQueryService(artifacts, observations);
        }
        @Bean PipelineHistoryQueryService pipelineHistoryQueryService(
                ArtifactQueryService artifacts, Fixture fixture, Clock clock) {
            return new PipelineHistoryQueryService(artifacts, fixture.history, Duration.ofMinutes(1), clock,
                    new HistoryCursorCodec("consumer-history-secret".getBytes(StandardCharsets.UTF_8), clock));
        }
        @Bean PipelineEventsQueryService pipelineEventsQueryService(
                ArtifactQueryService artifacts, Fixture fixture, Clock clock) {
            return new PipelineEventsQueryService(artifacts, fixture.events,
                    new EventsCursorCodec("consumer-events-secret".getBytes(StandardCharsets.UTF_8), clock),
                    clock, EventCatalog.bundled()::render);
        }
        @Bean PipelineExplainService pipelineExplainService(
                ArtifactQueryService artifacts, CurrentObservationReader observations, Fixture fixture, Clock clock) {
            return new PipelineExplainService(artifacts, observations, clock,
                    ExplanationCatalog.bundled()::render, id -> Optional.ofNullable(fixture.pending));
        }
        @Bean PipelineLogQueryService pipelineLogQueryService(Fixture fixture) {
            return new PipelineLogQueryService(fixture.logs, fixture.artifacts, fixture.generations, CLUSTER);
        }
        @Bean TokenStore tokenStore() { return new PipelineObservationApiTest.FakeTokenStore(); }
        @Bean TokenSecrets tokenSecrets() { return new PipelineObservationApiTest.FakeTokenSecrets(); }
        @Bean TokenSigner tokenSigner() { return new PipelineObservationApiTest.FakeSigner(); }
        @Bean TokenService tokenService(TokenStore store, TokenSecrets secrets, Clock clock) {
            return new TokenService(store, secrets, clock);
        }
        @Bean OperationRegistry operationRegistry() { return ControlOperations.registry(); }
    }

    private static final class Fixture {
        private final Artifacts artifacts = new Artifacts();
        private final Generations generations = new Generations();
        private final Observations observations = new Observations();
        private final History history = new History();
        private final PipelineObservationApiTest.FakePipelineEventStore events =
                new PipelineObservationApiTest.FakePipelineEventStore();
        private final RingBufferLogSink logs = new RingBufferLogSink(2, 20);
        private volatile Pending pending;

        void reset() {
            artifacts.put("inc-old");
            generations.current = 1;
            observations.stored = null;
            history.rows.clear();
            events.clear();
            logs.clearIncarnation(PIPELINE, "inc-old");
            logs.clearIncarnation(PIPELINE, "inc-new");
            pending = null;
        }

        void publish(PipelineState state, long count, Instant countingSince, Instant observedAt) {
            MetricFact counter = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                    List.of(MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                            MetricAttributes.TABLE_ID, "orders", MetricAttributes.DIRECTION, "out",
                            MetricAttributes.OP, "insert"),
                            countingSince, observedAt, count)));
            Observation observation = new Observation(PIPELINE, state,
                    Map.of("recordCount", count, "records.out", count, "reconcileFailuresInARow", 0L),
                    Map.of(), Map.of(), null, observedAt, List.of(counter));
            observations.saveScoped(observation, new ObservationStore.Scope(
                    artifacts.incarnation, generations.current));
        }
    }

    private static final class Artifacts implements ArtifactStore {
        private final Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                """);
        private volatile String incarnation;
        void put(String id) { incarnation = id; }
        void remove() { incarnation = null; }
        @Override public void saveAll(List<Resource> resources) { throw new UnsupportedOperationException(); }
        @Override public Optional<Resource> get(String id) {
            return PIPELINE.equals(id) && incarnation != null ? Optional.of(pipeline) : Optional.empty();
        }
        @Override public List<Resource> list() { return incarnation == null ? List.of() : List.of(pipeline); }
        @Override public Optional<String> pipelineIncarnationId(String id) {
            return get(id).map(resource -> incarnation);
        }
        @Override public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
            return get(id).map(resource -> new HistoryOwner(new RateHistoryStore.Visibility(incarnation, false)));
        }
    }

    private static final class Generations implements ExecutionGenerationStore {
        private volatile long current;
        @Override public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim claim, long revision) {
            throw new UnsupportedOperationException();
        }
        @Override public OptionalLong advanceStandalone(String clusterId, String pipelineId) {
            throw new UnsupportedOperationException();
        }
        @Override public OptionalLong currentGeneration(String clusterId, String pipelineId) {
            return OptionalLong.of(current);
        }
    }

    private static final class Observations implements ObservationStore {
        private volatile Stored stored;
        @Override public void save(Observation observation) { throw new UnsupportedOperationException(); }
        @Override public boolean saveScoped(Observation observation, Scope scope) {
            stored = new Stored(observation, Optional.of(scope));
            return true;
        }
        @Override public Optional<Observation> read(String pipelineId) {
            return readStored(pipelineId).map(Stored::observation);
        }
        @Override public Optional<Stored> readStored(String pipelineId) {
            return PIPELINE.equals(pipelineId) ? Optional.ofNullable(stored) : Optional.empty();
        }
        @Override public void delete(String pipelineId) { throw new UnsupportedOperationException(); }
    }

    private static final class History implements RateHistoryStore {
        private static final Comparator<Key> KEYS = Comparator.comparing(Key::observedAt)
                .thenComparing(Key::internalKey);
        private static final Comparator<Entry> ORDER = Comparator.comparing(Entry::key, KEYS);
        private final List<Entry> rows = new ArrayList<>();
        void add(Instant at, long count, Instant countingSince, String incarnation, long generation) {
            RateSample sample = new RateSample(PIPELINE, at, Map.of("records.out", count), Map.of(), countingSince);
            rows.add(new Entry(new Key(at, "row-" + rows.size()), sample,
                    Optional.of(new ObservationStore.Scope(incarnation, generation))));
        }
        @Override public void append(RateSample sample) { throw new UnsupportedOperationException(); }
        @Override public Page readPage(String id, Instant from, Instant to, Key after, int limit) {
            throw new UnsupportedOperationException("this fixture requires scoped history reads");
        }
        @Override public Page readPageVisible(String id, Visibility visibility,
                Instant from, Instant to, Key after, int limit) {
            List<Entry> matches = visible(id, visibility).stream()
                    .filter(row -> !row.key().observedAt().isBefore(from) && row.key().observedAt().isBefore(to))
                    .filter(row -> after == null || KEYS.compare(row.key(), after) > 0)
                    .sorted(ORDER).limit(limit + 1L).toList();
            return new Page(matches.size() > limit ? matches.subList(0, limit) : matches, matches.size() > limit);
        }
        @Override public Optional<Entry> read(String id, Key key) { throw new UnsupportedOperationException(); }
        @Override public Optional<Entry> readVisible(String id, Visibility visibility, Key key) {
            return visible(id, visibility).stream().filter(row -> row.key().equals(key)).findFirst();
        }
        @Override public Optional<Entry> predecessor(String id, Instant at) { throw new UnsupportedOperationException(); }
        @Override public Optional<Entry> predecessorVisible(String id, Visibility visibility, Instant at) {
            return visible(id, visibility).stream().filter(row -> row.key().observedAt().isBefore(at))
                    .max(ORDER);
        }
        @Override public Optional<Entry> successor(String id, Instant at) { throw new UnsupportedOperationException(); }
        @Override public Optional<Entry> successorVisible(String id, Visibility visibility, Instant at) {
            return visible(id, visibility).stream().filter(row -> !row.key().observedAt().isBefore(at))
                    .min(ORDER);
        }
        @Override public void deleteAll(String id) { throw new UnsupportedOperationException(); }
        @Override public Duration retention() { return Duration.ofDays(15); }
        private List<Entry> visible(String id, Visibility visibility) {
            return rows.stream().filter(row -> row.sample().pipelineId().equals(id))
                    .filter(row -> row.scope().map(scope -> scope.pipelineIncarnationId()
                            .equals(visibility.incarnationId())).orElse(visibility.includeLegacy())).toList();
        }
    }
}
