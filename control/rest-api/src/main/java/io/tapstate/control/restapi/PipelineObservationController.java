package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.messages.MessageCatalog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * Status, metrics, and snapshot retain their observation-based read contracts. Catalog UI projections
 * qualify a successful status read without replacing its runtime state or unobserved refusal.
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
        PipelineStatus runtime = observations.status(id);
        return pipelines.find(id)
                .map(item -> PipelineStatusResponse.of(item, catalog, runtime))
                .orElseGet(() -> PipelineStatusResponse.of(runtime, catalog));
    }

    @Verb("pipeline.metrics")
    @GetMapping("/pipelines/{id}/metrics")
    PipelineMetricsResponse metrics(@PathVariable("id") String id) {
        return PipelineMetricsResponse.of(observations.metrics(id));
    }

    @Verb("pipeline.snapshot")
    @GetMapping("/pipelines/{id}/snapshot")
    PipelineSnapshot snapshot(@PathVariable("id") String id) {
        return observations.snapshot(id);
    }
}
