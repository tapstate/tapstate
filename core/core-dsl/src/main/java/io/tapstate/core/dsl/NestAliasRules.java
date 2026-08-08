package io.tapstate.core.dsl;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A nest body names the streams it assembles by the aliases its step declares, and this is what checks
 * that those names exist. The alias map itself is batch wiring resolved by the closure; this is the
 * other half, where the body points back at the map.
 *
 * <p>Left unchecked a dangling name passes validate and surfaces only once the pipeline runs, as a
 * stream whose rows never arrive - indistinguishable from a source that is merely slow, and only after
 * a deployment.
 *
 * <p>Only a step with the body written into it is checked. A step that reuses a named definition cannot
 * declare the alias map a nest needs - the form is only accepted where the step itself says it is a
 * nest - so a reused nest body has no wiring here to resolve against.
 */
final class NestAliasRules {

    private NestAliasRules() {
    }

    /** Validates every nest body in the batch against the aliases its step declares. */
    static void validate(Collection<Resource> batch) {
        for (Resource resource : batch) {
            if (resource instanceof PipelineResource pipeline) {
                validatePipeline(pipeline);
            }
        }
    }

    private static void validatePipeline(PipelineResource pipeline) {
        List<Step> steps = pipeline.transforms();
        if (steps == null) {
            return;
        }
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (step instanceof Step.Inline inline
                    && inline.body() instanceof TransformBody.Nest nest
                    && inline.from() instanceof FromClause.Aliases wiring) {
                checkRoot(nest.root(), wiring.aliases().keySet(), "transforms[" + i + "].root");
            }
        }
    }

    private static void checkRoot(NestRoot root, Set<String> declared, String path) {
        require(root.from(), declared, path + ".from");
        checkEmbeds(root.embed(), declared, path);
    }

    private static void checkEmbeds(List<Embed> embeds, Set<String> declared, String path) {
        if (embeds == null) {
            return;
        }
        for (int i = 0; i < embeds.size(); i++) {
            Embed embed = embeds.get(i);
            String here = path + ".embed[" + i + "]";
            require(embed.from(), declared, here + ".from");
            checkEmbeds(embed.embed(), declared, here);
        }
    }

    private static void require(String alias, Set<String> declared, String path) {
        if (!declared.contains(alias)) {
            throw new DslException(DslError.MISSING_REFERENCE, path, 0, 0, null, Map.of("ref", alias));
        }
    }
}
