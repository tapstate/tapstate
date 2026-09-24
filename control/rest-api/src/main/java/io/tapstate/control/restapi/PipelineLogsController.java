package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineLogs;
import io.tapstate.core.logging.LogCursor;
import io.tapstate.core.logging.PipelineLogLevel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The node-local logs read face projected onto HTTP: {@code GET /api/pipelines/{id}/logs}. Kept separate
 * from the store-backed observation reads because its source is different — it tails the process's own log
 * output for the pipeline rather than a published, cross-node observation doc. The handler is a thin
 * pass-through to the control-core query service and carries no business logic. The read mutates nothing,
 * so like the other reads it is unaudited and names no caller principal; the interceptor still
 * authenticates and grade-checks it. A pipeline that has logged nothing yields a benign empty tail with a
 * normal 200, not a coded not-found — the absence of log lines is normal.
 */
@RestController
class PipelineLogsController {

    private final PipelineLogQueryService logs;

    PipelineLogsController(PipelineLogQueryService logs) {
        this.logs = logs;
    }

    @Verb("pipeline.logs")
    @GetMapping("/pipelines/{id}/logs")
    PipelineLogs logs(
            @PathVariable("id") String id,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "after", required = false) String after) {
        if (limit != null && limit < 1) {
            throw MalformedRequest.rejecting("limit must be positive", null);
        }
        LogCursor cursor;
        try {
            cursor = after == null ? null : LogCursor.parse(after);
        } catch (IllegalArgumentException invalidCursor) {
            throw MalformedRequest.rejecting("after must be a log cursor", invalidCursor);
        }
        return logs.logs(id, cursor, limit == null ? Integer.MAX_VALUE : limit);
    }

    @Verb("pipeline.log-level")
    @PostMapping("/pipelines/{id}:log-level")
    PipelineLogLevel level(@PathVariable("id") String id, @RequestBody PipelineLogLevelRequest request) {
        if (request == null || request.level() == null || request.level().isBlank()) {
            throw MalformedRequest.rejecting("level must be one of ERROR, WARN, INFO, DEBUG, or TRACE", null);
        }
        try {
            return logs.level(id, PipelineLogLevel.parse(request.level()));
        } catch (IllegalArgumentException invalidLevel) {
            throw MalformedRequest.rejecting("level must be one of ERROR, WARN, INFO, DEBUG, or TRACE", invalidLevel);
        }
    }

    /** Reads the threshold used for future node-local log capture without changing it. */
    @Verb("pipeline.logs")
    @GetMapping("/pipelines/{id}:log-level")
    PipelineLogLevel level(@PathVariable("id") String id) {
        return logs.level(id);
    }
}
