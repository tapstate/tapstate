package io.tapstate.core.model.canonical;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceRef;

import java.util.List;

/**
 * What a pipeline's run is assembled from, as a content hash -- the same canonical text the revision is
 * taken over, with the fields that only decide how a run is wired erased first.
 *
 * <p>Two definitions share an assembly identity exactly when everything they differ in can be re-read by
 * building the run again. That is the question a paused pipeline's resume has to answer, and asking it as
 * an equality rather than as a diff is what keeps it from drifting away from the revision: both readings
 * come out of {@link CanonicalWriter}, so a field that changes one changes the other.
 *
 * <h2>The whitelist</h2>
 *
 * <p><strong>The per-pipeline srs switch, and nothing else.</strong> It decides whether this pipeline
 * reads its source through the shared replay store, which is a wiring choice made when the run is
 * assembled: build the run again and the new answer is simply used. Nothing already written to the target
 * was produced differently because of it.
 *
 * <p>This is a declaration, not a size test. A change is whitelisted because re-assembling actually
 * re-reads it, never because the diff looked small. Transforms, views, serve and sync blocks are
 * deliberately outside it and always will be: data already written to the target went through the old
 * logic, and assembling a new run does not go back and change it. A pipeline whose transform changed has
 * a target that is half one definition and half the other, and no re-assembly repairs that -- which is
 * why the answer there stays a refusal rather than becoming a rebuild.
 *
 * <p>Erasing rather than listing field names is the point: the switch is removed from the resource and
 * the remainder is re-serialized, so any field this class does not know about is still in the text being
 * compared. A field added to a pipeline tomorrow is outside the whitelist by default, which is the safe
 * direction for a whitelist to fail in.
 */
public final class AssemblyIdentity {

    private static final CanonicalWriter WRITER = new CanonicalWriter();

    private AssemblyIdentity() {
    }

    /** The content hash of {@code resource} with every whitelisted field erased. */
    public static String of(Resource resource) {
        return CanonicalHash.of(WRITER.write(withWhitelistedFieldsErased(resource)));
    }

    private static Resource withWhitelistedFieldsErased(Resource resource) {
        if (!(resource instanceof PipelineResource pipeline)) {
            // Nothing on any other kind is whitelisted, so its assembly identity is its content hash.
            // A source's own srs block is emphatically not whitelisted here: this asks what one
            // pipeline's run is built from, and a source changing under it is a different question with
            // its own guard.
            return resource;
        }
        List<SourceRef> withoutSwitches = pipeline.sources().stream()
                .map(ref -> (SourceRef) SourceRef.bare(ref.id()))
                .toList();
        return new PipelineResource(
                pipeline.id(), pipeline.metadata(), withoutSwitches, pipeline.transforms(),
                pipeline.view(), pipeline.serve(), pipeline.settings(), pipeline.experimental());
    }
}
