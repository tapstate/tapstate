package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.HistoryResolution;
import io.tapstate.control.core.PipelineExplainService;
import io.tapstate.control.core.PipelineHistoryQuery;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.core.common.TapstateException;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Thin HTTP projection of the shared history and explanation services. */
@RestController
class PipelineObservabilityController {

    private final PipelineHistoryQueryService history;
    private final PipelineExplainService explanations;

    PipelineObservabilityController(PipelineHistoryQueryService history, PipelineExplainService explanations) {
        this.history = history;
        this.explanations = explanations;
    }

    @Verb("pipeline.metrics.history")
    @GetMapping("/pipelines/{id}/metrics/history")
    ResponseEntity<PipelineHistoryResponse> history(
            @PathVariable("id") String id,
            @RequestParam(name = "from", required = false) String from,
            @RequestParam(name = "to", required = false) String to,
            @RequestParam(name = "resolution", required = false, defaultValue = "auto") String resolution,
            @RequestParam(name = "limit", required = false) String limit,
            @RequestParam(name = "table", required = false) List<String> tables,
            @RequestParam(name = "cursor", required = false) String cursor) {
        PipelineHistoryQuery query = new PipelineHistoryQuery(
                id,
                instant(from, "from"),
                instant(to, "to"),
                resolution(resolution),
                limit(limit),
                tables == null ? List.of() : tables,
                cursor);
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(PipelineHistoryResponse.of(history.query(query)));
    }

    @Verb("pipeline.explain")
    @GetMapping("/pipelines/{id}/explain")
    ResponseEntity<PipelineExplanationResponse> explain(@PathVariable("id") String id) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(PipelineExplanationResponse.of(explanations.explain(id)));
    }

    private static java.time.Instant instant(String value, String name) {
        if (value == null || value.isBlank()) {
            throw malformed(name + " is required");
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException invalid) {
            throw malformed(name + " must be an RFC3339 timestamp with a UTC offset");
        }
    }

    private static HistoryResolution resolution(String value) {
        return switch (value) {
            case "auto" -> HistoryResolution.AUTO;
            case "raw" -> HistoryResolution.RAW;
            case "PT5M", "PT30M", "PT1H", "PT3H", "PT6H" ->
                    HistoryResolution.valueOf(value.toUpperCase(Locale.ROOT));
            default -> throw malformed(
                    "resolution must be auto, raw, PT5M, PT30M, PT1H, PT3H, or PT6H");
        };
    }

    private static int limit(String value) {
        if (value == null) {
            return PipelineHistoryQuery.DEFAULT_LIMIT;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException invalid) {
            throw malformed("limit must be an integer");
        }
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }
}
