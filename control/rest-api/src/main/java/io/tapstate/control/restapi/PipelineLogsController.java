package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineLogQueryService.Scope;
import io.tapstate.control.core.PipelineLogs;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
            @RequestParam(value = "scope", required = false, defaultValue = "current") String scope) {
        if (limit != null && limit < 1) {
            throw MalformedRequest.rejecting("limit must be positive", null);
        }
        Scope selected = switch (scope) {
            case "current" -> Scope.CURRENT;
            case "incarnation" -> Scope.INCARNATION;
            default -> throw MalformedRequest.rejecting("scope must be current or incarnation", null);
        };
        return limit == null ? logs.logs(id, selected) : logs.logs(id, limit, selected);
    }
}
