package io.tapstate.core.dsl;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.WriteMode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The two things an unwind has to settle before it may run, both answerable from the document
 * alone. Expanding a row is the first operation here that changes how many rows there are, and both
 * rules exist because the ways that goes wrong are silent: the pipeline is green, the target holds
 * rows, and nothing counts the ones that were overwritten or the ones that should have gone away.
 *
 * <ul>
 *   <li><b>The rows have to be tellable apart.</b> An expansion that names nothing varying per
 *       element gives every row the parent's own key, so an upsert matches all of them to one row.
 *       The target then holds exactly what it would hold if the step were not there at all.</li>
 *   <li><b>The target has to be able to converge.</b> Appending never matches a write to an
 *       existing row, so deleting a parent appends the elements it used to hold rather than
 *       removing their rows. Nothing about the expansion is wrong there - the combination simply
 *       cannot deliver what it promises, so it is refused rather than promised.</li>
 * </ul>
 *
 * <p>Sibling of {@link WriteKeyRules} and answering the same question about the same write, but on
 * the other side of the discovery boundary: that one asks what a table declares, which only a
 * discovered model carries, while both of these are properties of the document and so are judged
 * offline, where an author still has the file open.
 */
public final class UnwindRules {

    private UnwindRules() {
    }

    /** Refuses every unwind in {@code batch} that cannot key its rows or cannot converge. */
    public static void validate(Collection<Resource> batch) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource r : batch) {
            byId.put(r.id(), r);
        }
        for (Resource r : batch) {
            if (r instanceof PipelineResource p) {
                validatePipeline(p, byId);
            }
        }
    }

    private static void validatePipeline(PipelineResource p, Map<String, Resource> byId) {
        List<Step> steps = p.transforms() == null ? List.of() : p.transforms();
        Set<String> unwinds = new LinkedHashSet<>();
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (!(bodyOf(step, byId) instanceof TransformBody.Unwind unwind)) {
                continue;
            }
            unwinds.add(step.id());
            // Judged wherever it is declared, read or not. What the step says about its own rows is
            // a property of the declaration, and an author who has written one that cannot key its
            // output wants to hear so at the step, not once they later wire it to something.
            if (UnwindWriteKeys.elementLocator(unwind) == null) {
                String path = "transforms[" + i + "]";
                throw new DslException(DslError.UNWIND_NEEDS_AN_ELEMENT_KEY, path, 0, 0, null,
                        Map.of("step", step.id(), "path", path));
            }
        }
        if (!unwinds.isEmpty()) {
            refuseAppendBelow(p, byId, unwinds);
        }
    }

    /**
     * The write mode is only judged against the unwinds whose rows actually arrive at it. An
     * expansion on a branch the serve block never reads writes nothing, so it converges nowhere and
     * there is nothing to refuse - blaming the target's mode for it would refuse a pipeline over
     * rows that never reach the target.
     */
    private static void refuseAppendBelow(
            PipelineResource p, Map<String, Resource> byId, Set<String> unwindIds) {
        ServeBlock serve = p.serve();
        if (serve == null) {
            return;
        }
        Set<String> read = new Wiring(p, byId).nodesReaching(switch (serve) {
            case ServeBlock.Inline inline -> inline.from();
            case ServeBlock.Use use -> use.from();
        });
        String reaching = unwindIds.stream().filter(read::contains).findFirst().orElse(null);
        if (reaching == null) {
            return;
        }
        List<SyncElement> sync = syncElements(serve, byId);
        for (int i = 0; i < sync.size(); i++) {
            if (sync.get(i).writeMode() == WriteMode.APPEND) {
                String path = "serve.sync[" + i + "].write_mode";
                throw new DslException(DslError.UNWIND_NEEDS_AN_UPSERT_TARGET, path, 0, 0, null,
                        Map.of("step", reaching, "sync", sync.get(i).id(), "path", path));
            }
        }
    }

    /** The body a step runs, taken from the definition it names where it names one. */
    private static TransformBody bodyOf(Step step, Map<String, Resource> byId) {
        return switch (step) {
            case Step.Inline inline -> inline.body();
            case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                    ? definition.body()
                    : null;
        };
    }

    /** The sync elements a serve block declares, inline or through the definition it names. */
    private static List<SyncElement> syncElements(ServeBlock serve, Map<String, Resource> byId) {
        List<SyncElement> sync = switch (serve) {
            case ServeBlock.Inline inline -> inline.sync();
            case ServeBlock.Use use ->
                    byId.get(use.use()) instanceof ServeResource definition ? definition.sync() : null;
        };
        return sync == null ? List.of() : sync;
    }
}
