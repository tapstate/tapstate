package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interactive pipeline wizard's question flow, driven by a scripted prompter. Asserts on the
 * canonical artifact the collected answers produce — the wizard's job is to build a valid pipeline.
 */
class PipelineWizardTest {

    private static String yaml(PipelineResource p) {
        return new CanonicalWriter().write(p);
    }

    @Test
    void wizardOutputIsACanonicalFixedPoint() {
        // a pipeline the wizard writes must re-parse to an equal model: write(parse(write(m))) == write(m).
        // anonymous serve/sync/push ids are filled by the parser, so the wizard must set them to match.
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b");
        String once = yaml(new PipelineWizard(p, List.of()).run());
        String twice = new CanonicalWriter().write(new DslParser().parse(once));
        assertThat(twice).isEqualTo(once);
    }

    @Test
    void buildsAMinimalMirrorPipelineFromFreeTextSourceAndTarget() {
        // no existing workspace sources -> both refs are free-text; the spine is a mirror-all
        // (from: /.*/) pipeline that syncs every source table to one target store
        ScriptedPrompter p = new ScriptedPrompter("p_min", "src_a", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p_min
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void offersExistingWorkspaceSourcesAsChoices() {
        // src_a and tgt_b already exist as kind:source files -> both refs are menu choices
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of("src_a", "tgt_b")).run();
        assertThat(pipe.sourceIds()).containsExactly("src_a");
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
        // the source question (1st choose) offers the existing sources + a free-text escape
        assertThat(p.offered.get(0)).containsExactly("src_a", "tgt_b", "(other)");
    }

    @Test
    void fallsBackToFreeTextWhenOtherIsChosen() {
        // existing sources present, but the user picks "(other)" and types an id not in the list
        ScriptedPrompter p = new ScriptedPrompter("p1", "(other)", "src_new", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of("tgt_b")).run();
        assertThat(pipe.sourceIds()).containsExactly("src_new");
    }

    @Test
    void addsAFilterTransformAndWiresTheServeFromIt() {
        // id, source, then one filter transform (id / from / CEL expr), then done, then sync target
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "filter", "flt", "orders", "op != 'd'", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - id: flt
                    type: filter
                    from: [orders]
                    expr: "op != 'd'"
                serve:
                  id: serve
                  from: flt
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void addsAMapTransformWithRenameComputedAndDropRules() {
        // each field is name + rule: $src rename / =expr computed / false drop; blank name ends fields
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a",
                "map", "norm", "legacy_customers",
                "customer_id", "$cust_no",
                "joined_at", "=timestamp(after.join_date)",
                "ssn", "false",
                "",
                "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - id: norm
                    type: map
                    from: [legacy_customers]
                    fields:
                      customer_id: $cust_no
                      joined_at: "=timestamp(after.join_date)"
                      ssn: false
                serve:
                  id: serve
                  from: norm
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void addsAJsTransformAsALiteralScriptBlock() {
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "js", "parse", "orders_topic", "emit(after)", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - id: parse
                    type: js
                    from: [orders_topic]
                    script: |
                      emit(after)
                serve:
                  id: serve
                  from: parse
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void addsAMultilineJsTransformViaTheSharedBodyPrompter() {
        // the inline js prompt now captures a multi-line block (the lines() primitive) through the
        // shared TransformBodyPrompter — the same seam the standalone transform wizard uses
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "js", "parse", "orders", "emit(after)\nemit(before)", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - id: parse
                    type: js
                    from: [orders]
                    script: |
                      emit(after)
                      emit(before)
                serve:
                  id: serve
                  from: parse
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void chainsTransformsWiringEachFromThePrevious() {
        // the second filter leaves from: blank -> it auto-wires to the first step (f1); serve.from = last
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a",
                "filter", "f1", "orders", "op != 'd'",
                "filter", "f2", "", "amount > 0",
                "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - id: f1
                    type: filter
                    from: [orders]
                    expr: "op != 'd'"
                  - id: f2
                    type: filter
                    from: [f1]
                    expr: "amount > 0"
                serve:
                  id: serve
                  from: f2
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void buildsAServePushWithTopic() {
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "push", "tgt_kfk", "orders_events");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  push:
                    - id: push_1
                      source: tgt_kfk
                      topic: orders_events
                """);
    }

    @Test
    void buildsAServeQueryEndpoint() {
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "query", "rest");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  query:
                    - type: rest
                """);
    }

    @Test
    void buildsAViewOnlyPipeline() {
        // serve is declined ((none)), so a view becomes the sole output; the view stage then offers
        // an inline definition (no reusable view bodies present in this workspace)
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "(none)", "inline", "v_cust", "customer_id");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  id: v_cust
                  from: /.*/
                  primary_key: customer_id
                """);
    }

    @Test
    void defaultsToASyncServeWhenTheOutputPromptIsSkipped() {
        // an exhausted / Enter answer at the Output prompt must yield the sync mirror, not a view
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.serve()).isNotNull();
        assertThat(pipe.view()).isNull();
    }

    @Test
    void skipsAMapTransformThatHasNoFields() {
        // picking map then entering no fields must not emit a fields-less map (it would crash validate)
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "map", "m1", "orders", "", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.transforms()).isNull();
        assertThat(pipe.serve()).isNotNull();
    }

    @Test
    void defaultsThePipelineIdWhenLeftBlank() {
        ScriptedPrompter p = new ScriptedPrompter("", "src_a", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.id()).isEqualTo("pipeline");
    }

    @Test
    void fallsBackWhenTheFromAnswerHasNoUsableToken() {
        // a comma-only 'From' answer must not crash; it falls back to the suggested default (/.*/)
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "filter", "f1", ",", "op != 'd'", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).contains("from: [/.*/]");
    }

    @Test
    void reusesATransformDefinitionByUse() {
        // mask_pii exists as a kind: transform definition -> offered for reuse in the transform menu
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(use)", "mask_pii", "customers", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of(), List.of("mask_pii")).run();
        // the transform menu (the first choose, since there are no existing sources to pick) offers (use)
        assertThat(p.offered.get(0)).containsExactly("filter", "map", "js", "(use)", "(done)");
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                transforms:
                  - use: mask_pii
                    from: [customers]
                serve:
                  id: serve
                  from: mask_pii
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void omitsTheReuseOptionWhenNoTransformDefinitionsExist() {
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b");
        new PipelineWizard(p, List.of()).run();
        assertThat(p.offered.get(0)).containsExactly("filter", "map", "js", "(done)");
    }

    @Test
    void buildsAServePushWithoutATopic() {
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "push", "tgt_kfk", "");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  id: serve
                  from: /.*/
                  push:
                    - id: push_1
                      source: tgt_kfk
                """);
    }

    @Test
    void buildsAServeQueryForGraphqlAndMcp() {
        for (String type : List.of("graphql", "mcp")) {
            ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "query", type);
            PipelineResource pipe = new PipelineWizard(p, List.of()).run();
            assertThat(yaml(pipe)).contains("- type: " + type);
        }
    }

    @Test
    void buildsAViewWithoutAPrimaryKey() {
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "(none)", "inline", "v_cust", "");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  id: v_cust
                  from: /.*/
                """);
    }

    @Test
    void buildsACombinedInlineViewAndSyncServe() {
        // pick a serve (sync) AND a view: the pipeline carries both surfaces, and the serve reads
        // from the view (natural-order wiring transforms -> view -> serve)
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "sync", "tgt_b", "inline", "v_cust", "customer_id");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  id: v_cust
                  from: /.*/
                  primary_key: customer_id
                serve:
                  id: serve
                  from: v_cust
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void reusesAServeDefinitionByUse() {
        // std_api exists as a kind: serve definition -> offered in the serve menu, referenced by use:
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "std_api");
        PipelineResource pipe =
                new PipelineWizard(p, List.of(), List.of(), List.of(), List.of("std_api")).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                serve:
                  use: std_api
                  from: /.*/
                """);
        // the serve menu (2nd choose: transform menu is 1st when there are no existing sources) offers it
        assertThat(p.offered.get(1)).contains("std_api");
    }

    @Test
    void reusesAViewDefinitionByUse() {
        // v_cust exists as a kind: view definition; serve is declined so the reused view is the output
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "(none)", "v_cust");
        PipelineResource pipe =
                new PipelineWizard(p, List.of(), List.of(), List.of("v_cust"), List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  use: v_cust
                  from: /.*/
                """);
        // the view menu (3rd choose: transform, serve, then view) offers the reusable definition
        assertThat(p.offered.get(2)).contains("v_cust");
    }

    @Test
    void reproducesTheReuseAssemblyCorpusShape() {
        // the crm_pack golden: filter -> use mask_pii -> use v_cust view -> use std_api serve, wired
        // transforms -> view -> serve. The wizard must rebuild that exact canonical artifact.
        ScriptedPrompter p = new ScriptedPrompter(
                "crm_pack", "src_crm",
                "filter", "filter_1", "customers", "op != 'd'",
                "(use)", "mask_pii", "",
                "(done)",
                "std_api", "v_cust");
        PipelineResource pipe = new PipelineWizard(
                p, List.of(), List.of("mask_pii"), List.of("v_cust"), List.of("std_api")).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: crm_pack
                source: src_crm
                transforms:
                  - id: filter_1
                    type: filter
                    from: [customers]
                    expr: "op != 'd'"
                  - use: mask_pii
                    from: [filter_1]
                view:
                  use: v_cust
                  from: mask_pii
                serve:
                  use: std_api
                  from: v_cust
                """);
    }

    @Test
    void buildsACombinedInlineViewAndQueryServe() {
        // a non-sync serve (query) combined with a view wires the same way: serve reads from the view
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "query", "rest", "inline", "v_cust", "customer_id");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  id: v_cust
                  from: /.*/
                  primary_key: customer_id
                serve:
                  id: serve
                  from: v_cust
                  query:
                    - type: rest
                """);
    }

    @Test
    void reusesAViewWithAnInlineServe() {
        // a reused view definition combined with an inline sync serve; serve reads from the view id
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b", "v_cust");
        PipelineResource pipe =
                new PipelineWizard(p, List.of(), List.of(), List.of("v_cust"), List.of()).run();
        assertThat(yaml(pipe)).isEqualTo(
                """
                version: tapstate/v1
                kind: pipeline
                id: p1
                source: src_a
                view:
                  use: v_cust
                  from: /.*/
                serve:
                  id: serve
                  from: v_cust
                  sync:
                    - id: sync_1
                      source: tgt_b
                """);
    }

    @Test
    void listsReusableDefinitionsFirstInBothOutputMenus() {
        // both output menus prepend the workspace's reusable definitions, then the inline surfaces
        ScriptedPrompter p = new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b", "(none)");
        new PipelineWizard(p, List.of(), List.of(),
                List.of("v_a", "v_b"), List.of("s_a", "s_b")).run();
        // offered: [0] transform menu, [1] serve menu, [2] view menu (sink is a free-text ask, not a choose)
        assertThat(p.offered.get(1)).containsExactly("s_a", "s_b", "push", "query", "(none)", "sync");
        assertThat(p.offered.get(2)).containsExactly("v_a", "v_b", "inline", "(none)");
    }

    @Test
    void combinedAndReuseOutputsAreCanonicalFixedPoints() {
        // every output shape the new stages can produce must re-parse to an equal model
        assertFixedPoint(new PipelineWizard(new ScriptedPrompter(
                "p1", "src_a", "(done)", "sync", "tgt_b", "inline", "v_cust", "customer_id"),
                List.of()));
        assertFixedPoint(new PipelineWizard(new ScriptedPrompter(
                "p1", "src_a", "(done)", "query", "rest", "inline", "v_cust", "customer_id"),
                List.of()));
        assertFixedPoint(new PipelineWizard(
                new ScriptedPrompter("p1", "src_a", "(done)", "std_api"),
                List.of(), List.of(), List.of(), List.of("std_api")));
        assertFixedPoint(new PipelineWizard(
                new ScriptedPrompter("p1", "src_a", "(done)", "(none)", "v_cust"),
                List.of(), List.of(), List.of("v_cust"), List.of()));
        assertFixedPoint(new PipelineWizard(
                new ScriptedPrompter("p1", "src_a", "(done)", "sync", "tgt_b", "v_cust"),
                List.of(), List.of(), List.of("v_cust"), List.of()));
    }

    private static void assertFixedPoint(PipelineWizard wizard) {
        String once = yaml(wizard.run());
        String twice = new CanonicalWriter().write(new DslParser().parse(once));
        assertThat(twice).isEqualTo(once);
    }

    @Test
    void repromptsAnInlineViewIdThatCollidesWithTheServeBlock() {
        // a combined pipeline's inline serve is id "serve"; an inline view named "serve" would
        // duplicate it and crash validate, so the wizard re-prompts until a distinct id is given
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "(done)", "sync", "tgt_b", "inline", "serve", "v_real", "");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.view()).isInstanceOf(ViewBlock.Inline.class);
        assertThat(((ViewBlock.Inline) pipe.view()).id()).isEqualTo("v_real");
    }

    @Test
    void repromptsAnInlineViewIdThatCollidesWithATransformStep() {
        // the inline view id and transform step ids share one pipeline-internal namespace; naming the
        // view after the transform would duplicate the id, so the wizard re-prompts
        ScriptedPrompter p = new ScriptedPrompter(
                "p1", "src_a", "filter", "norm", "orders", "op != 'd'",
                "(done)", "(none)", "inline", "norm", "v_real", "");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(((ViewBlock.Inline) pipe.view()).id()).isEqualTo("v_real");
    }

    @Test
    void repromptsAnIdContainingADot() {
        // ids may not contain a dot (it would crash the parser); the wizard re-prompts on one
        ScriptedPrompter p = new ScriptedPrompter("a.b", "p_ok", "src_a", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.id()).isEqualTo("p_ok");
    }

    @Test
    void repromptsABlankRequiredSource() {
        // a pipeline must name a source; a blank answer (no workspace sources to choose) is re-asked
        ScriptedPrompter p = new ScriptedPrompter("p1", "", "src_real", "(done)", "sync", "tgt_b");
        PipelineResource pipe = new PipelineWizard(p, List.of()).run();
        assertThat(pipe.sourceIds()).containsExactly("src_real");
    }
}
