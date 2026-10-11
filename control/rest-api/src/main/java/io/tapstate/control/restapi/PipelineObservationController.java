package io.tapstate.control.restapi;

import io.tapstate.control.core.MonitorError;
import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineError;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineSnapshot;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Status reads the merged authoring, artifact, intent, and observation projection; metrics and snapshot
 * remain observation-only. A saved Pipeline always has a status even before its first run.
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
        PipelineCatalogItem item = pipelines.find(id).orElseThrow(() ->
                new TapstateException(PipelineError.NOT_FOUND, Map.of("id", id), null));
        PipelineStatus runtime = null;
        if (item.status().observedState() != null) {
            try {
                runtime = observations.status(id);
            } catch (TapstateException failure) {
                if (failure.code() != MonitorError.NO_OBSERVATION) {
                    throw failure;
                }
            }
        }
        return PipelineStatusResponse.of(item, catalog, runtime);
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
