package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.DataBrowserNestSampler;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.NestExplainService;
import io.tapstate.control.core.NestSampler;
import io.tapstate.control.core.NestTreeReader;
import io.tapstate.control.core.NestInspectService;
import io.tapstate.control.core.NestStateInspector;
import io.tapstate.control.core.NestProgress;
import io.tapstate.control.core.NestProgressService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.messages.MessageCatalog;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The nest reads, projected onto HTTP. A thin pass-through: which tables a step depends on and what it is
 * waiting for are worked out behind the service, from the definition and the published observation.
 */
@RestController
class NestController {

    private final NestProgressService progress;
    private final NestInspectService inspect;
    private final NestExplainService explain;
    private final MessageCatalog catalog;

    NestController(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            ObjectProvider<NestStateInspector> inspector, ObjectProvider<NestTreeReader> trees,
            ObjectProvider<DataBrowserService> browser, MessageCatalog catalog) {
        this.progress = new NestProgressService(artifacts, observations);
        this.inspect = new NestInspectService(artifacts, inspector.getIfAvailable(() -> NestStateInspector.NONE));
        DataBrowserService reader = browser.getIfAvailable();
        this.explain = new NestExplainService(artifacts, trees.getIfAvailable(() -> NestTreeReader.NONE),
                reader == null ? NestSampler.NONE : new DataBrowserNestSampler(reader));
        this.catalog = catalog;
    }

    @Verb("pipeline.nest.progress")
    @GetMapping("/pipelines/{id}/nest/progress")
    ResponseEntity<NestProgress> progress(@PathVariable("id") String id,
            @RequestParam(name = "step", required = false) String step) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(progress.progress(id, step == null || step.isBlank() ? null : step));
    }

    /** What {@code step} holds under the root key, one {@code key} parameter per root key column, in order. */
    @Verb("pipeline.nest.inspect")
    @GetMapping("/pipelines/{id}/nest/{step}/documents")
    ResponseEntity<NestInspection> inspect(@PathVariable("id") String id, @PathVariable("step") String step,
            @RequestParam(name = "key") List<String> key) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(inspect.inspect(id, step, key));
    }

    /** What {@code step} compiles to, and what a sample of its sources warns about. */
    @Verb("pipeline.nest.explain")
    @GetMapping("/pipelines/{id}/nest/{step}/explain")
    ResponseEntity<NestExplainResponse> explain(@PathVariable("id") String id, @PathVariable("step") String step) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(NestExplainResponse.of(explain.explain(id, step), catalog));
    }
}
