package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.sql.JoinKind;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.runtime.engine.nest.NestInbound;
import io.tapstate.runtime.engine.nest.NestLookup;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.runtime.engine.nest.NestVertex;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SourceMoveColumnsTest {
    @Test
    void aDirectCopyGuardsIsolatedUniqueValueChangesAndLeavesOrdinaryUpdatesUnarmed() {
        var model = new TargetTable("orders", List.of(new TargetField("id", "bigint", true),
                new TargetField("owner", "varchar", false), new TargetField("label", "varchar", false)),
                List.of(new TargetIndex(List.of("id"), true), new TargetIndex(List.of("owner"), true),
                        new TargetIndex(List.of("label"), false)));
        var guarded = List.copyOf(SourceMoveColumns.uniqueColumns(model));
        assertThat(guarded).containsExactlyInAnyOrder("id", "owner");
        var before = Map.<String, Object>of("id", 1, "owner", "Ada", "label", "old");
        var ordinary = Envelope.update(1, "orders", before,
                Map.of("id", 1, "owner", "Ada", "label", "new"), null);
        var move = Envelope.update(2, "orders", before,
                Map.of("id", 1, "owner", "Bob", "label", "old"), null);
        var missing = Envelope.update(3, "orders", Map.of("id", 1),
                Map.of("id", 1, "owner", "Bob", "label", "old"), null);
        assertThat(ScopedKeyMoveGuard.mayMove(move, List.of("id"))).isFalse();
        assertThat(ScopedKeyMoveGuard.mayMove(move, guarded)).isTrue();
        assertThat(ScopedKeyMoveGuard.mayMove(missing, guarded)).isTrue();
        assertThat(ScopedKeyMoveGuard.mayMove(ordinary, guarded)).isFalse();
    }

    @Test
    void directSourceAliasesProtectChangedStateKeysWithoutRegisteringProjectionOnlyUpdates() {
        var aliases = FromClause.aliases(Map.of("orders", FromRef.literal("one.rows"),
                "people", FromRef.literal("two.rows"), "child", FromRef.literal("one.rows")));
        var sql = new TransformBody.Join(JoinEngine.BUILTIN, "SELECT people.email FROM orders LEFT JOIN people ON orders.owner = people.email");
        var join = new JoinTree.Join(new JoinTree.Source("orders", "rows"),
                new JoinTree.Source("alternative", "rows", "people"), JoinKind.LEFT,
                List.of(new JoinTree.KeyPair(new JoinTree.ColumnRef("orders", "owner"),
                        new JoinTree.ColumnRef("alternative", "email"))), false);
        var compiled = new StoreBackedDagSource.CompiledJoin(new JoinPlan(List.of(), join, Map.of()),
                List.of("id"), Map.of(), Map.of(), sql, List.of());
        var tracked = new NestInbound(0, "child", List.of("children"), List.of("parent"), List.of("code"), true, "rows");
        var root = new NestVertex(List.of(), "root", "root-state", List.of("id"), List.of(), List.of(tracked));
        var resolver = new NestVertex(List.of("children"), "resolver", "resolver-state", List.of("code"),
                List.of("parent_group"), List.of(tracked));
        var lookup = new NestLookup(List.of("children", "person"), "people", "people-lookup", "lookup-state",
                List.of("external_id"), "child", List.of("lookup_fk"), List.of("custom_identity"),
                List.of("children"), 1, true, List.of("person_id"));
        var nest = new OperatorCoordinatePlan.NestStep("db",
                new NestTopology(List.of(root, resolver), List.of(), List.of(), List.of(lookup), true), NestSettings.defaults());
        var body = new TransformBody.Nest(null, null, new NestRoot("orders", List.of("id"), null, true, List.of()));
        var keys = SourceMoveColumns.forDirectInputs(Map.of("join", Step.inline("join", aliases, sql, Map.of()),
                        "nest", Step.inline("nest", aliases, body, Map.of())),
                Map.of("join", compiled), Map.of("nest", nest),
                ref -> List.of(((FromRef.Literal) ref).ref()), Set.of("one.rows", "two.rows"));

        assertThat(keys.get("one.rows")).containsExactlyInAnyOrder("owner", "parent", "code",
                "parent_group", "lookup_fk", "custom_identity");
        assertThat(keys.get("two.rows")).containsExactlyInAnyOrder("email", "person_id", "external_id");
        var before = Map.<String, Object>of("id", 1, "person_id", 2, "external_id", 3, "email", "a", "label", "old");
        var ordinary = Envelope.update(1, "two.rows", before, Map.of("id", 1, "person_id", 2, "external_id", 3, "email", "a", "label", "new"), null);
        var move = Envelope.update(2, "two.rows", before, Map.of("id", 1, "person_id", 2, "external_id", 3, "email", "b", "label", "old"), null);
        assertThat(ScopedKeyMoveGuard.mayMove(ordinary, List.copyOf(keys.get("two.rows")))).isFalse();
        assertThat(ScopedKeyMoveGuard.mayMove(move, List.of("id"))).isFalse();
        assertThat(ScopedKeyMoveGuard.mayMove(move, List.copyOf(keys.get("two.rows")))).isTrue();
    }
}
