package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.UnwindIdentity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TargetIdentityResolverTest {
    private static final String PRESERVED = "_unwind_source_id";

    @Test
    void aDocumentIdentityIsSeparatedWithoutChangingTheFullLogicalKey() {
        var plan = TargetIdentityResolver.resolve(expanded("_id", PRESERVED), document());
        assertThat(plan.status()).isEqualTo(TargetIdentityPlan.AUTOMATIC);
        assertThat(plan.writeKey()).containsExactly(PRESERVED, "position");
        assertThat(plan.target().fields()).extracting(TargetField::name)
                .containsExactly(PRESERVED, "position", "value");
        assertThat(plan.target().indexes()).containsExactly(new TargetIndex(List.of(PRESERVED, "position"), true));
    }

    @Test
    void aCompositeTargetKeyNeedsNoProjection() {
        var facts = new TargetIdentityFacts(true, false, List.of("id", "position"), Set.of(),
                Set.of("id", "position", "value"), List.of(List.of("id", "position")),
                Set.of(), false, true, false, false, Set.of(PRESERVED, "position"));
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED), facts).status())
                .isEqualTo(TargetIdentityPlan.NOT_REQUIRED);
    }

    @Test
    void aCompositePrimaryKeyDoesNotHideAConflictingBusinessUniqueKey() {
        var facts = new TargetIdentityFacts(true, false, List.of("id", "position"), Set.of(),
                Set.of("id", "position", "value"), List.of(List.of("id", "position"), List.of("id")),
                Set.of(), false, true, false, false, Set.of(PRESERVED, "position"));
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED), facts).reason())
                .isEqualTo("other-unique-constraint");
    }

    @Test
    void theSameRuleHandlesAnExistingRelationalGeneratedIdentity() {
        var facts = relational(true, Set.of("id", PRESERVED, "position", "value"),
                List.of(List.of("id"), List.of(PRESERVED, "position")), Set.of());
        var plan = TargetIdentityResolver.resolve(expanded("id", PRESERVED), facts);
        assertThat(plan.status()).isEqualTo(TargetIdentityPlan.AUTOMATIC);
        assertThat(plan.target().fields()).extracting(TargetField::name).doesNotContain("id");
        assertThat(plan.writeKey()).containsExactly(PRESERVED, "position");
    }

    @Test
    void targetKeyCoercionCannotCollapseDistinctSourceIdentities() {
        var numeric = expanded("id", PRESERVED);
        var fields = new java.util.ArrayList<>(numeric.fields());
        fields.set(0, new TargetField("id", "varchar(80)", true, io.tapstate.core.common.TapstateType.STRING));
        var strings = new TargetTable(numeric.name(), fields, numeric.indexes(), numeric.unwindIdentity());
        assertThat(TargetIdentityResolver.resolve(strings, relational(true, Set.of("id", PRESERVED, "position"),
                List.of(List.of(PRESERVED, "position")), Set.of())).reason()).isEqualTo("key-representation-unsupported");
    }

    @Test
    void runtimeKeyValuesCannotContradictTheSampledIntegerModel() {
        var plan = TargetIdentityResolver.resolve(expanded("id", PRESERVED),
                relational(true, Set.of("id", PRESERVED, "position"),
                        List.of(List.of(PRESERVED, "position")), Set.of()));
        var valid = Map.<String, Object>of("id", new ConvertedValue(700L, "bigint"), "position", 0L);
        assertThat(TargetIdentityProjection.row(plan, valid)).containsEntry(PRESERVED, valid.get("id"));
        for (Object invalid : java.util.Arrays.asList("01", "1", 1.0, java.math.BigDecimal.ONE,
                java.math.BigInteger.valueOf(Long.MAX_VALUE).add(java.math.BigInteger.ONE), null)) {
            for (String field : List.of("id", "position")) {
                var bad = new java.util.LinkedHashMap<>(valid);
                bad.put(field, invalid);
                for (Envelope event : List.of(Envelope.insert(1L, "rows", bad, null),
                        Envelope.update(1L, "rows", bad, valid, null),
                        Envelope.delete(1L, "rows", bad, null))) {
                    assertThatThrownBy(() -> TargetIdentityProjection.apply(plan, event))
                            .isInstanceOfSatisfying(TapstateException.class, error -> {
                                assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CONFLICT);
                                assertThat(error.args()).containsEntry("reason", "key-representation-unsupported");
                            });
                }
            }
        }
        for (Object validInteger : List.of((byte) 1, (short) 1, 1, Long.MIN_VALUE, Long.MAX_VALUE,
                java.math.BigInteger.valueOf(Long.MAX_VALUE))) {
            assertThat(TargetIdentityProjection.row(plan, Map.of("id", validInteger, "position", 0L)))
                    .containsEntry(PRESERVED, validInteger);
        }
    }

    @Test
    void aRequiredIdentityWithoutGenerationCannotBeFixedByRenamingIt() {
        var facts = relational(false, Set.of("id", PRESERVED, "position"),
                List.of(List.of(PRESERVED, "position")), Set.of());
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED), facts).reason())
                .isEqualTo("identity-generation-unavailable");
    }

    @Test
    void anExistingFixedSchemaMustAlreadyHoldThePreservedFieldAndSafeMatchingIndex() {
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED),
                relational(true, Set.of("id", "position"), List.of(), Set.of())).reason())
                .isEqualTo("target-columns-missing");
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED),
                relational(true, Set.of("id", PRESERVED, "position"), List.of(List.of("id")), Set.of())).reason())
                .isEqualTo("logical-key-index-missing");
    }

    @Test
    void nullableRelationalMatchingKeysDoNotClaimReplaySafety() {
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED),
                relational(true, Set.of("id", PRESERVED, "position"),
                        List.of(List.of(PRESERVED, "position")), Set.of("position"))).reason())
                .isEqualTo("nullable-key-not-unique");
    }

    @Test
    void compoundKeysWithoutUnwindAndExplicitlyRenamedParentIdsAreLeftAlone() {
        var expanded = expanded("_id", PRESERVED);
        assertThat(TargetIdentityResolver.resolve(new TargetTable("rows", expanded.fields(), expanded.indexes()),
                document()).status()).isEqualTo(TargetIdentityPlan.NOT_REQUIRED);
        assertThat(TargetIdentityResolver.resolve(expanded("business_id", PRESERVED), document()).status())
                .isEqualTo(TargetIdentityPlan.NOT_REQUIRED);
    }

    @Test
    void aConfiguredNameMustNotOverwriteAnExistingColumn() {
        var plan = TargetIdentityResolver.resolve(expanded("_id", "value"), document());
        assertThat(plan.status()).isEqualTo(TargetIdentityPlan.ACTION_REQUIRED);
        assertThat(plan.reason()).isEqualTo("field-collision");
    }

    @Test
    void bothImagesAndRemovedFieldsUseTheSameProjectionAndPreserveValueTypes() {
        var plan = TargetIdentityResolver.resolve(expanded("_id", "original_id"), document());
        Object identity = new ConvertedValue("012345678901234567890123", "ObjectId");
        Map<String, Object> before = Map.of("_id", identity, "position", 1L, "value", "old");
        Map<String, Object> after = Map.of("_id", identity, "position", 1L, "value", "new");
        Envelope event = Envelope.update(1L, "rows", before, after, null).withRemoved(Set.of("obsolete"));
        Envelope projected = TargetIdentityProjection.apply(plan, event);
        assertThat(projected.before()).containsEntry("original_id", identity).doesNotContainKey("_id");
        assertThat(projected.after()).containsEntry("original_id", identity).doesNotContainKey("_id");
        assertThat(projected.removed()).containsExactly("obsolete");
        assertThat(event.before()).containsKey("_id");
        assertThat(TargetIdentityProjection.apply(plan, Envelope.delete(2L, "rows", before, null)).before())
                .containsEntry("original_id", identity);
    }

    @Test
    void aDynamicFieldCollisionIsRejectedBeforeReplacingAnyValue() {
        var plan = TargetIdentityResolver.resolve(expanded("_id", PRESERVED), document());
        Map<String, Object> row = Map.of("_id", "a", "position", 0L, PRESERVED, "user data");
        assertThatThrownBy(() -> TargetIdentityProjection.row(plan, row))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CONFLICT);
                    assertThat(error.args()).containsEntry("reason", "field-collision");
                });
        assertThat(row).containsEntry(PRESERVED, "user data");
    }

    @Test
    void opaqueUniqueConstraintsCannotBeDiscardedAsIfTheyDidNotExist() {
        var facts = new TargetIdentityFacts(true, false, List.of("id"), Set.of("id"),
                Set.of("id", PRESERVED, "position", "value"),
                List.of(List.of("id"), List.of(PRESERVED, "position")), Set.of(), false, true, false, true, Set.of(PRESERVED, "position"));
        assertThat(TargetIdentityResolver.resolve(expanded("id", PRESERVED), facts).reason())
                .isEqualTo("other-unique-constraint");
    }

    private static TargetTable expanded(String id, String preserved) {
        return new TargetTable("rows", List.of(new TargetField(id, "string", true, id.equals("id") ? io.tapstate.core.common.TapstateType.INT64 : io.tapstate.core.common.TapstateType.STRING),
                new TargetField("position", "long", true, io.tapstate.core.common.TapstateType.INT64), new TargetField("value", "string", false)),
                List.of(new TargetIndex(List.of(id, "position"), true)),
                new UnwindIdentity(List.of(id), preserved));
    }

    private static TargetIdentityFacts document() {
        return new TargetIdentityFacts(false, true, List.of("_id"), Set.of("_id"), Set.of(),
                List.of(List.of("_id")), Set.of(), true, true, true, false, Set.of());
    }

    private static TargetIdentityFacts relational(boolean generated, Set<String> columns,
            List<List<String>> indexes, Set<String> nullable) {
        return new TargetIdentityFacts(true, false, List.of("id"), generated ? Set.of("id") : Set.of(),
                columns, indexes, nullable, false, true, false, false, Set.of(PRESERVED, "position"));
    }
}
