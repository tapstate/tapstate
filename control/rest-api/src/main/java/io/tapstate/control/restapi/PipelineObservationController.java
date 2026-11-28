package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineRunProgressQuery;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.control.core.ConnectorUsageQuery;
import org.springframework.beans.factory.annotation.Autowired;
import io.tapstate.messages.MessageCatalog;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

/**
 * Status combines catalog intent with runtime observation fields. Metrics and snapshot remain observation-based
 * reads; a known pipeline without an observation can still return its catalog status.
 */
@RestController
class PipelineObservationController {

    private final PipelineObservationQueryService observations;
    private final PipelineCatalogService pipelines;
    private final MessageCatalog catalog;
    private ConnectorUsageQuery usage;
    private PipelineRunProgressQuery runProgress;

    @Autowired(required = false)
    void connectorUsage(ConnectorUsageQuery usage) { this.usage = usage; }

    @Autowired(required = false)
    void runProgressQuery(PipelineRunProgressQuery runProgress) { this.runProgress = runProgress; }

    PipelineObservationController(PipelineObservationQueryService observations,
            PipelineCatalogService pipelines, MessageCatalog catalog) {
        this.observations = observations;
        this.pipelines = pipelines;
        this.catalog = catalog;
    }

    @Verb("pipeline.status")
    @GetMapping("/pipelines/{id}/status")
    PipelineStatusResponse status(@PathVariable("id") String id, HttpServletRequest request) {
        java.util.Locale locale = ApiExceptionHandler.localeFor(request);
        Optional<PipelineCatalogItem> item = pipelines.find(id);
        if (item.isPresent()) {
            PipelineCatalogItem pipeline = item.orElseThrow();
            var result = observations.findStatusWithPlan(pipeline)
                    .map(status -> PipelineStatusResponse.of(pipeline, catalog, status, locale))
                    .orElseGet(() -> PipelineStatusResponse.of(pipeline, catalog, java.time.Clock.systemUTC(), locale));
            return connectorVersions(result, id);
        }
        return connectorVersions(PipelineStatusResponse.of(observations.status(id), catalog, java.time.Clock.systemUTC(), locale), id);
    }

    private PipelineStatusResponse connectorVersions(PipelineStatusResponse response, String id) {
        return response.withConnectorVersions(usage == null ? java.util.List.of() : usage.forPipeline(id));
    }

    @Verb("pipeline.run-progress")
    @GetMapping("/pipelines/{id}/run-progress")
    PipelineRunProgressResponse runProgress(@PathVariable("id") String id) {
        return runProgress == null ? PipelineRunProgressResponse.unavailable()
                : runProgress.find(id).map(PipelineRunProgressResponse::from)
                        .orElseGet(PipelineRunProgressResponse::unavailable);
    }

    @Verb("pipeline.metrics")
    @GetMapping("/pipelines/{id}/metrics")
    PipelineMetricsResponse metrics(@PathVariable("id") String id) {
        return PipelineMetricsResponse.of(observations.metrics(id));
    }

    @Verb("pipeline.snapshot")
    @GetMapping("/pipelines/{id}/snapshot")
    PipelineSnapshotResponse snapshot(@PathVariable("id") String id) {
        PipelineSnapshot snapshot = observations.snapshot(id);
        return PipelineSnapshotResponse.of(snapshot);
    }
}
