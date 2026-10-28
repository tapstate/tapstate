package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.messages.MessageCatalog;
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

    PipelineObservationController(PipelineObservationQueryService observations,
            PipelineCatalogService pipelines, MessageCatalog catalog) {
        this.observations = observations;
        this.pipelines = pipelines;
        this.catalog = catalog;
    }

    @Verb("pipeline.status")
    @GetMapping("/pipelines/{id}/status")
    PipelineStatusResponse status(@PathVariable("id") String id) {
        Optional<PipelineCatalogItem> item = pipelines.find(id);
        if (item.isPresent()) {
            PipelineCatalogItem pipeline = item.orElseThrow();
            return observations.findStatusWithPlan(pipeline)
                    .map(status -> PipelineStatusResponse.of(pipeline, catalog, status))
                    .orElseGet(() -> PipelineStatusResponse.of(pipeline, catalog));
        }
        return PipelineStatusResponse.of(observations.status(id), catalog);
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
