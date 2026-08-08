package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.runtime.engine.nest.NestTopology;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The capacity an author wrote on a nest step is only a field until something reads it. This is the
 * step that turns the two numbers in the artifact into the settings the run is actually held to.
 *
 * <p>What is checked here is not that a number arrives but that it arrives <em>where it applies</em>.
 * The per-document limit is filed under the namespace of the nest that wrote it, so a pipeline holding
 * two nests holds two limits; the budget is one number for the pipeline. A wiring that read both the
 * same way would pass a test that only asked whether the value came through.
 *
 * <p>A pipeline that wrote neither keeps what the deployment was started with, which is what leaves
 * every pipeline authored before these fields existed running exactly as it did.
 */
class TheCapacityAPipelineWroteIsWhatItRunsOnTest {

    private static final String PIPELINE = "p";

    private static final Map<String, NestTable> TABLES = new LinkedHashMap<>(Map.of(
            "customer", new NestTable("customers", List.of("customer_id")),
            "policy", new NestTable("policies", List.of("policy_id"))));

    /** A nest step over customers with policies embedded, carrying whatever capacity the case gives it. */
    private static Step nestStep(String nodeId, Integer entriesInMemory, Integer maxElements) {
        Embed policy = new Embed("policy", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_no"), null, null, null);
        TransformBody.Nest body = new TransformBody.Nest(null, null, entriesInMemory, maxElements,
                new NestRoot("customer", List.of("customer_id"), null, null, List.of(policy)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("customer", FromRef.literal("customers"));
        aliases.put("policy", FromRef.literal("policies"));
        return Step.inline(nodeId, FromClause.aliases(aliases), body, null, null);
    }

    private static PipelineResource pipelineOf(Step... steps) {
        List<Step> transforms = new ArrayList<>(List.of(steps));
        return new PipelineResource(PIPELINE, null, List.of("customers", "policies"), transforms, null,
                new ServeBlock.Inline("serve", FromRef.literal(transforms.get(transforms.size() - 1).id()),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);
    }

    /** The name the compiler gives one step's document level, asked of the compiler rather than spelled. */
    private static String documentNamespaceOf(Step step) {
        TransformBody.Nest body = (TransformBody.Nest) ((Step.Inline) step).body();
        return NestTopology.compile(PIPELINE, step.id(), body, TABLES::get).assembler().mapName();
    }

    private static NestSettings settingsFor(PipelineResource pipeline, NestSettings base) {
        return PipelineDagBuilder.nestSettings(pipeline, TABLES::get, base);
    }

    @Test
    void aPipelineThatWroteNothingKeepsWhatTheDeploymentSet() {
        NestSettings base = NestSettings.defaults().withEntriesHeldInMemory(40_000);
        Step step = nestStep("doc", null, null);

        NestSettings settings = settingsFor(pipelineOf(step), base);

        assertThat(settings.entriesHeldInMemory()).isEqualTo(40_000);
        assertThat(settings.elementsAllowedIn(documentNamespaceOf(step)))
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void theBudgetAPipelineWroteReplacesTheOneItWasStartedWith() {
        NestSettings settings =
                settingsFor(pipelineOf(nestStep("doc", 60_000, null)),
                        NestSettings.defaults().withEntriesHeldInMemory(40_000));

        assertThat(settings.entriesHeldInMemory()).isEqualTo(60_000);
    }

    @Test
    void theDocumentLimitAPipelineWroteIsFiledUnderTheNestThatWroteIt() {
        Step step = nestStep("doc", null, 2_000);

        NestSettings settings = settingsFor(pipelineOf(step), NestSettings.defaults());

        assertThat(settings.elementsAllowedIn(documentNamespaceOf(step))).isEqualTo(2_000);
        // A namespace nobody wrote a limit for still takes the default. A wiring that applied the number
        // to every namespace would pass the assertion above and hold unrelated levels to a stranger's
        // limit - which only shows up as a failed run on a level whose author never named a number.
        assertThat(settings.elementsAllowedIn("nest.other.assembler"))
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void twoNestsEachKeepTheirOwnDocumentLimit() {
        Step first = nestStep("doc_a", null, 2_000);
        Step second = nestStep("doc_b", null, 9_000);

        NestSettings settings = settingsFor(pipelineOf(first, second), NestSettings.defaults());

        // The discriminating pair: filed per nest, so the second does not overwrite the first. Keyed on
        // anything coarser than the vertex - the pipeline, say - the last one walked would win silently.
        assertThat(settings.elementsAllowedIn(documentNamespaceOf(first))).isEqualTo(2_000);
        assertThat(settings.elementsAllowedIn(documentNamespaceOf(second))).isEqualTo(9_000);
    }

    @Test
    void aPipelineWithNoNestAtAllIsLeftExactlyAsItWas() {
        NestSettings base = NestSettings.defaults().withEntriesHeldInMemory(40_000);
        PipelineResource pipeline = new PipelineResource(PIPELINE, null, List.of("customers"), null, null,
                new ServeBlock.Inline("serve", FromRef.literal("customers"),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        assertThat(settingsFor(pipeline, base)).isSameAs(base);
    }

    @Test
    void aBudgetTooSmallToMeanAnythingIsRefusedWithItsCode() {
        // The substrate spends the budget per partition, so below the partition count it has already been
        // rounded up by the time it is enforced. Reachable from an artifact now that an author can write
        // it, so the coded refusal has to survive the trip rather than be swallowed on the way.
        assertThatThrownBy(() -> settingsFor(pipelineOf(nestStep("doc", 10, null)), NestSettings.defaults()))
                .hasMessageContaining("nest.memory-budget-below-partition-count");
    }
}
