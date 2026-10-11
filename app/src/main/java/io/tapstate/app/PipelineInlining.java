package io.tapstate.app;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.spi.store.ArtifactStore;

/**
 * Expands a pipeline's {@code use:} references into the definitions they name, so the DAG builder is
 * handed a pipeline whose view and serve blocks are inline.
 *
 * <p>A reusable definition and an inline one describe the same thing; which form an author chose is a
 * property of how the workspace is written, not of what runs. The builder therefore refuses a
 * reference outright rather than resolving one itself - it has no store to resolve against - and this
 * is the step that makes that refusal unreachable.
 *
 * <p>The reference is carried by the block, and what it names is carried by the definition: a block
 * supplies the wiring ({@code from:}) and its own id, while the definition supplies everything about
 * the thing being reused. So expansion is a copy of the definition's body under the block's wiring,
 * never a merge - a reference has nothing of its own to merge in.
 *
 * <p>A definition that is missing or of the wrong kind crashes bare rather than carrying a code: the
 * reference was already resolved against the workspace when the pipeline was applied, so its absence
 * here is a defect on this side, not something an author can act on.
 */
final class PipelineInlining {

    private PipelineInlining() {
    }

    /** The pipeline with every {@code use:} reference expanded, or the same instance when it has none. */
    static PipelineResource inline(PipelineResource pipeline, ArtifactStore artifacts) {
        ViewBlock view = inlineView(pipeline.view(), artifacts);
        ServeBlock serve = inlineServe(pipeline.serve(), artifacts);
        if (view == pipeline.view() && serve == pipeline.serve()) {
            return pipeline;
        }
        return new PipelineResource(pipeline.id(), pipeline.metadata(), pipeline.sources(),
                pipeline.transforms(), view, serve, pipeline.settings(), pipeline.experimental());
    }

    private static ViewBlock inlineView(ViewBlock view, ArtifactStore artifacts) {
        if (!(view instanceof ViewBlock.Use use)) {
            return view;
        }
        ViewResource definition = require(artifacts, use.use(), ViewResource.class, "view");
        // The definition says how the view runs as well as where it writes; a reference carries neither,
        // so both come from the definition, and dropping the first here would run the view at its default
        // width with the artifact saying otherwise.
        return new ViewBlock.Inline(use.id(), use.from(),
                definition.primaryKey(), definition.storage(), definition.execution());
    }

    private static ServeBlock inlineServe(ServeBlock serve, ArtifactStore artifacts) {
        if (!(serve instanceof ServeBlock.Use use)) {
            return serve;
        }
        ServeResource definition = require(artifacts, use.use(), ServeResource.class, "serve");
        return new ServeBlock.Inline(use.id(), use.from(),
                definition.sync(), definition.query(), definition.push());
    }

    private static <T extends Resource> T require(
            ArtifactStore artifacts, String id, Class<T> kind, String referencedAs) {
        Resource resource = artifacts.get(id).orElseThrow(() -> new IllegalStateException(
                referencedAs + " '" + id + "' referenced by a pipeline is not in the store"));
        if (!kind.isInstance(resource)) {
            throw new IllegalStateException("resource '" + id + "' referenced as a " + referencedAs
                    + " is a '" + resource.kind() + "'");
        }
        return kind.cast(resource);
    }
}
