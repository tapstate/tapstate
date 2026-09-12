package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The interactive standalone {@code serve} wizard, driven by a scripted prompter. A {@code kind: serve}
 * definition is a reusable, composable publish surface: 0..n of sync / query / push with no {@code from:}
 * wiring (X19), at least one surface required. Unlike the pipeline's shallow inline serve, this richer
 * flow asks each sync's write mode and DDL policy (Tier-1); rename and per-element options are authored
 * by hand. It collects answers through a {@link Prompter}, never a terminal directly.
 */
class ServeWizardTest {

    @Test
    void recordsExplicitFullLoadClearAndOffersAppendAsSafeDefault() {
        ScriptedPrompter p = new ScriptedPrompter("target", "sync", "warehouse", "upsert", "fail", "clear");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).contains("on_full_load: clear");
        assertThat(p.offered).contains(List.of("clear", "fail", "append"));
    }

    private static String yaml(ServeResource s) {
        return new CanonicalWriter().write(s);
    }

    @Test
    void buildsAServeWithASingleSyncSurface() {
        // write mode / ddl left to default (upsert / fail) — both omitted from the canonical form
        ScriptedPrompter p = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_sink
                sync:
                  - id: sync_1
                    source: tgt_b
                """);
    }

    @Test
    void recordsANonDefaultWriteModeAndDdlPolicy() {
        ScriptedPrompter p = new ScriptedPrompter("std_sink", "sync", "tgt_b", "append", "apply");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_sink
                sync:
                  - id: sync_1
                    source: tgt_b
                    write_mode: append
                    ddl: apply
                """);
    }

    @Test
    void buildsAServeWithAQueryEndpointWithoutABackend() {
        // a query with no sync to back it is a parallel egress from the view store: no backend written
        ScriptedPrompter p = new ScriptedPrompter("std_api", "query", "rest");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                query:
                  - type: rest
                """);
    }

    @Test
    void wiresAQueryBackendToAnEarlierSyncId() {
        // sync first, then a query whose backend names that sync's auto id (the API-on-sink shape)
        ScriptedPrompter p = new ScriptedPrompter(
                "std_api", "sync", "tgt_b", "upsert", "fail", "append", "query", "rest", "sync_1");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                sync:
                  - id: sync_1
                    source: tgt_b
                query:
                  - type: rest
                    backend: sync_1
                """);
    }

    @Test
    void buildsAServeWithAPushSurfaceWithATopic() {
        ScriptedPrompter p = new ScriptedPrompter("evt", "push", "tgt_kfk", "orders_events");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: evt
                push:
                  - id: push_1
                    source: tgt_kfk
                    topic: orders_events
                """);
    }

    @Test
    void buildsAServeWithAPushSurfaceWithoutATopic() {
        ScriptedPrompter p = new ScriptedPrompter("evt", "push", "tgt_kfk", "");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: evt
                push:
                  - id: push_1
                    source: tgt_kfk
                """);
    }

    @Test
    void composesMultipleSurfacesWithRunningAutoIds() {
        // two syncs (sync_1, sync_2), a backed query, and a push (push_1) all in one serve
        ScriptedPrompter p = new ScriptedPrompter(
                "std_sink",
                "sync", "tgt_b", "upsert", "fail", "append",
                "sync", "tgt_c", "append", "fail", "append",
                "query", "rest", "sync_2",
                "push", "tgt_kfk", "orders_events",
                "(done)");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_sink
                sync:
                  - id: sync_1
                    source: tgt_b
                  - id: sync_2
                    source: tgt_c
                    write_mode: append
                query:
                  - type: rest
                    backend: sync_2
                push:
                  - id: push_1
                    source: tgt_kfk
                    topic: orders_events
                """);
    }

    @Test
    void requiresAtLeastOneSurface() {
        // the first surface menu omits (done): a serve with no surface is meaningless, so the user
        // cannot finish with zero. An exhausted script settles on the safe default (sync), never (done).
        ScriptedPrompter p = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(serve.sync()).hasSize(1);
        assertThat(p.offered.get(0)).doesNotContain("(done)").contains("sync");
    }

    @Test
    void offersExistingWorkspaceSourcesForASyncTarget() {
        ScriptedPrompter p = new ScriptedPrompter("std_sink", "sync", "tgt_b");
        new ServeWizard(p, List.of("tgt_b", "tgt_c")).run();
        // offered[0] is the surface menu; offered[1] is the sync target choice (existing + free-text)
        assertThat(p.offered.get(1)).containsExactly("tgt_b", "tgt_c", "(other)");
    }

    @Test
    void defaultsTheServeIdWhenLeftBlank() {
        ScriptedPrompter p = new ScriptedPrompter("", "sync", "tgt_b");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(serve.id()).isEqualTo("serve");
    }

    @Test
    void omitsTheQueryBackendWhenNoneIsChosenWithASyncPresent() {
        // a sync exists, so the backend menu offers it plus (none); choosing (none) is a parallel egress
        ScriptedPrompter p = new ScriptedPrompter(
                "std_api", "sync", "tgt_b", "upsert", "fail", "append", "query", "rest", "(none)");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: std_api
                sync:
                  - id: sync_1
                    source: tgt_b
                query:
                  - type: rest
                """);
        // the backend menu offered the earlier sync id and the (none) sentinel
        assertThat(p.offered).contains(List.of("sync_1", "(none)"));
    }

    @Test
    void composesTwoPushSurfacesWithRunningAutoIds() {
        ScriptedPrompter p = new ScriptedPrompter(
                "evt", "push", "k1", "t1", "push", "k2", "t2", "(done)");
        ServeResource serve = new ServeWizard(p, List.of()).run();
        assertThat(yaml(serve)).isEqualTo(
                """
                version: tapstate/v1
                kind: serve
                id: evt
                push:
                  - id: push_1
                    source: k1
                    topic: t1
                  - id: push_2
                    source: k2
                    topic: t2
                """);
    }

    @Test
    void everySurfaceShapeIsACanonicalFixedPoint() {
        assertFixedPoint(new ServeWizard(new ScriptedPrompter("s1", "sync", "tgt_b"), List.of()));
        assertFixedPoint(new ServeWizard(new ScriptedPrompter(
                "s1", "sync", "tgt_b", "append", "apply"), List.of()));
        assertFixedPoint(new ServeWizard(new ScriptedPrompter("s1", "query", "rest"), List.of()));
        assertFixedPoint(new ServeWizard(new ScriptedPrompter(
                "s1", "sync", "tgt_b", "upsert", "fail", "query", "rest", "sync_1"), List.of()));
        assertFixedPoint(new ServeWizard(new ScriptedPrompter(
                "s1", "push", "tgt_kfk", "orders_events"), List.of()));
    }

    private static void assertFixedPoint(ServeWizard wizard) {
        String once = yaml(wizard.run());
        String twice = new CanonicalWriter().write(new DslParser().parse(once));
        assertThat(twice).isEqualTo(once);
    }
}
