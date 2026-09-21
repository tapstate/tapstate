package io.tapstate.control.restapi;

import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenService;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Key;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Full HTTP contract for bounded history pages, cursor refusals, retention and authorization. */
class PipelineHistoryApiTest {

    private static final Instant NOW = Instant.parse("2026-07-12T12:00:00Z");
    private static final Instant COUNTING_SINCE = Instant.parse("2026-07-12T10:00:00Z");
    private static final byte[] CURSOR_SECRET =
            "test-history-secret".getBytes(StandardCharsets.UTF_8);

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(PipelineObservationApiTest.TestApp.class)
                .properties("server.port=0")
                .run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @BeforeEach
    void resetHistory() {
        history().clear();
    }

    @Test
    void rawPagesKeepDuplicateTimesAndBindEveryContinuationArgument() {
        history().append(sample("2026-07-12T11:57:30Z", 0));
        history().append(sample("2026-07-12T11:58:30Z", 60));
        history().append(sample("2026-07-12T11:58:30Z", 65));
        history().append(sample("2026-07-12T11:59:30Z", 125));

        Map<String, Object> first = page(null, List.of());
        Map<String, Object> second = page((String) first.get("nextCursor"), List.of());
        Map<String, Object> third = page((String) second.get("nextCursor"), List.of());

        assertThat(first).containsEntry("consistency", "EVENTUAL").containsKey("nextCursor");
        assertThat(second).containsKey("nextCursor");
        assertThat(third.get("nextCursor")).isNull();
        List<String> walked = new ArrayList<>();
        walked.addAll(intervalEnds(first));
        walked.addAll(intervalEnds(second));
        walked.addAll(intervalEnds(third));
        assertThat(walked).containsExactly(
                "2026-07-12T11:58:30Z",
                "2026-07-12T11:58:30Z",
                "2026-07-12T11:59:30Z");

        ApiError changed = historyError((String) first.get("nextCursor"), List.of("orders"),
                HttpStatus.BAD_REQUEST);
        assertThat(changed.code()).isEqualTo("monitor.invalid-cursor");
        assertThat(changed.params()).containsEntry("reason", "QUERY_MISMATCH");

        ApiError anotherPipeline = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl2/metrics/history")
                        .queryParam("from", "2026-07-12T11:58:00Z")
                        .queryParam("to", "2026-07-12T12:00:00Z")
                        .queryParam("resolution", "raw")
                        .queryParam("limit", 1)
                        .queryParam("cursor", first.get("nextCursor"))
                        .build())
                .header("Authorization", "Bearer " + token())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
        assertThat(anotherPipeline.code()).isEqualTo("monitor.invalid-cursor");
        assertThat(anotherPipeline.params()).containsEntry("reason", "QUERY_MISMATCH");

        String cursor = (String) first.get("nextCursor");
        char replacement = cursor.endsWith("A") ? 'B' : 'A';
        ApiError tampered = historyError(cursor.substring(0, cursor.length() - 1) + replacement,
                List.of(), HttpStatus.BAD_REQUEST);
        assertThat(tampered.code()).isEqualTo("monitor.invalid-cursor");
        assertThat(tampered.params()).containsEntry("reason", "TAMPERED");
    }

    @Test
    void expiredCursorLimitSelectorAndAuthenticationRefusalsKeepTheirCodesAndNoStore() {
        Instant from = Instant.parse("2026-07-12T11:58:00Z");
        Instant to = NOW;
        HistoryCursorCodec old = new HistoryCursorCodec(CURSOR_SECRET,
                Clock.fixed(NOW.minus(Duration.ofMinutes(11)), ZoneOffset.UTC));
        HistoryCursorCodec.QueryBinding binding = new HistoryCursorCodec.QueryBinding(
                "pl1", from, to, HistoryResolution.RAW, 1, List.of());
        String expired = old.issue(binding, from, to, NOW.minus(Duration.ofDays(15)),
                new Key(from.plusSeconds(30), "00000001"), from.plusSeconds(30));

        ApiError gone = historyError(expired, List.of(), HttpStatus.GONE);
        assertThat(gone.code()).isEqualTo("monitor.cursor-expired");

        ApiError largePage = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl1/metrics/history")
                        .queryParam("from", from)
                        .queryParam("to", to)
                        .queryParam("limit", 1001)
                        .build())
                .header("Authorization", "Bearer " + token())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
        assertThat(largePage.code()).isEqualTo("control.malformed-request");

        ApiError selectors = client().get().uri(uri -> {
                    uri.path("/api/pipelines/pl1/metrics/history")
                            .queryParam("from", from)
                            .queryParam("to", to);
                    for (int i = 0; i < 21; i++) {
                        uri.queryParam("table", "table_" + i);
                    }
                    return uri.build();
                })
                .header("Authorization", "Bearer " + token())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
        assertThat(selectors.code()).isEqualTo("control.malformed-request");

        ApiError unbounded = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl1/metrics/history")
                        .queryParam("from", NOW.minus(Duration.ofDays(15)).minusSeconds(1))
                        .queryParam("to", NOW)
                        .build())
                .header("Authorization", "Bearer " + token())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
        assertThat(unbounded.code()).isEqualTo("control.malformed-request");

        ApiError unauthenticated = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl1/metrics/history")
                        .queryParam("from", from)
                        .queryParam("to", to)
                        .build())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
        assertThat(unauthenticated.code()).isEqualTo("control.unauthenticated");
    }

    @Test
    void retentionCutoffHidesPhysicallyPresentOldSamplesAndKeepsEmptyDistinct() {
        Instant cutoff = NOW.minus(Duration.ofDays(15));
        history().append(new RateSample("pl1", cutoff.minusSeconds(60),
                Map.of("records.out", 0L, "bytes.out", 0L), Map.of(), COUNTING_SINCE));
        history().append(new RateSample("pl1", cutoff.plusSeconds(60),
                Map.of("records.out", 120L, "bytes.out", 1_200L), Map.of(), COUNTING_SINCE));

        Map<String, Object> clipped = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl1/metrics/history")
                        .queryParam("from", cutoff.minus(Duration.ofHours(1)))
                        .queryParam("to", cutoff.plus(Duration.ofMinutes(2)))
                        .queryParam("resolution", "raw")
                        .build())
                .header("Authorization", "Bearer " + token())
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});

        assertThat(clipped).containsEntry("effectiveFrom", cutoff.toString())
                .containsEntry("retentionCutoff", cutoff.toString());
        assertThat(intervalEnds(clipped)).containsExactly(cutoff.plusSeconds(60).toString());
        assertThat(points(clipped).getFirst()).doesNotContainKeys("recordsOut", "bytesOut");

        Map<String, Object> empty = client().get().uri(uri -> uri
                        .path("/api/pipelines/pl1/metrics/history")
                        .queryParam("from", cutoff.minus(Duration.ofHours(2)))
                        .queryParam("to", cutoff.minus(Duration.ofHours(1)))
                        .build())
                .header("Authorization", "Bearer " + token())
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});
        assertThat(empty).containsEntry("status", "NO_RETAINED_SAMPLES")
                .containsEntry("retentionCutoff", cutoff.toString())
                .containsEntry("nextCursor", null);
        assertThat(empty.get("segments")).isEqualTo(List.of());
    }

    private Map<String, Object> page(String cursor, List<String> tables) {
        return client().get().uri(uri -> {
                    uri.path("/api/pipelines/pl1/metrics/history")
                            .queryParam("from", "2026-07-12T11:58:00Z")
                            .queryParam("to", "2026-07-12T12:00:00Z")
                            .queryParam("resolution", "raw")
                            .queryParam("limit", 1);
                    tables.forEach(table -> uri.queryParam("table", table));
                    if (cursor != null) {
                        uri.queryParam("cursor", cursor);
                    }
                    return uri.build();
                })
                .header("Authorization", "Bearer " + token())
                .retrieve().body(new ParameterizedTypeReference<Map<String, Object>>() {});
    }

    private ApiError historyError(String cursor, List<String> tables, HttpStatus expected) {
        return client().get().uri(uri -> {
                    uri.path("/api/pipelines/pl1/metrics/history")
                            .queryParam("from", "2026-07-12T11:58:00Z")
                            .queryParam("to", "2026-07-12T12:00:00Z")
                            .queryParam("resolution", "raw")
                            .queryParam("limit", 1)
                            .queryParam("cursor", cursor);
                    tables.forEach(table -> uri.queryParam("table", table));
                    return uri.build();
                })
                .header("Authorization", "Bearer " + token())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(expected);
                    assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                    return response.bodyTo(ApiError.class);
                });
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> points(Map<String, Object> page) {
        return ((List<Map<String, Object>>) page.get("segments")).stream()
                .flatMap(segment -> ((List<Map<String, Object>>) segment.get("points")).stream())
                .toList();
    }

    private static List<String> intervalEnds(Map<String, Object> page) {
        return points(page).stream().map(point -> String.valueOf(point.get("intervalEnd"))).toList();
    }

    private static RateSample sample(String at, long records) {
        return new RateSample("pl1", Instant.parse(at),
                Map.of("records.out", records, "bytes.out", records * 10), Map.of(), COUNTING_SINCE);
    }

    private PipelineObservationApiTest.FakeRateHistoryStore history() {
        return context.getBean(PipelineObservationApiTest.FakeRateHistoryStore.class);
    }

    private String token() {
        return context.getBean(TokenService.class).create(Scope.READ);
    }

    private RestClient client() {
        return RestClient.create("http://127.0.0.1:" + port);
    }
}
