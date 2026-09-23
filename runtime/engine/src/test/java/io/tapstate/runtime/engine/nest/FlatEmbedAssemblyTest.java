package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlatEmbedAssemblyTest {

    private static final String PROFILE_STATE = "$flat[profile]";
    private static final String REGION_STATE = "$flat[region]";
    private static final String CUSTOMER_LOOKUP = "nest.p.document.$flat[region]";
    private static final EmbedSlot PROFILE = flat(PROFILE_STATE, PROFILE_STATE);
    private static final List<EmbedSlot> PROFILE_ONLY = List.of(PROFILE);

    @Test
    void oneFlatRowContributesItsFieldsDirectlyToTheParent() {
        RootAssembly assembly = root();
        assembly.applyElement(profile("p1"), row("tier", "gold", "nickname", "Ada"), at(2), noPositions());

        assertThat(assembly.render(PROFILE_ONLY).orElseThrow())
                .containsEntry("id", 7)
                .containsEntry("tier", "gold")
                .containsEntry("nickname", "Ada")
                .doesNotContainKey(PROFILE_STATE);
    }

    @Test
    void severalDisjointFlatChildrenCanMergeIntoTheSameParent() {
        RootAssembly assembly = root();
        assembly.applyElement(profile("p1"), row("tier", "gold"), at(2), noPositions());
        assembly.applyElement(region("r1"), row("region_name", "east"), at(3), noPositions());

        assertThat(assembly.render(List.of(PROFILE, flat(REGION_STATE, REGION_STATE))).orElseThrow())
                .containsEntry("tier", "gold")
                .containsEntry("region_name", "east");
    }

    @Test
    void aSecondLiveChildRowStopsRatherThanChoosingOne() {
        RootAssembly assembly = root();
        assembly.applyElement(profile("p1"), row("tier", "gold"), at(2), noPositions());
        assembly.applyElement(profile("p2"), row("tier", "silver"), at(3), noPositions());

        assertThatThrownBy(() -> assembly.render(PROFILE_ONLY))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(NestError.FLAT_CARDINALITY_VIOLATION);
                    assertThat(error.args()).containsEntry("embedPath", PROFILE_STATE)
                            .containsEntry("rows", 2);
                });
    }

    @Test
    void actualRowsCatchConflictsAnUnavailableModelCouldNotPreflight() {
        RootAssembly withParentConflict = root();
        withParentConflict.applyElement(profile("p1"), row("id", 99), at(2), noPositions());

        assertThatThrownBy(() -> withParentConflict.render(PROFILE_ONLY))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT);
                    assertThat(error.args()).containsEntry("fields", "id")
                            .containsEntry("occupiedBy", "the parent row");
                });

        RootAssembly withSiblingConflict = root();
        withSiblingConflict.applyElement(profile("p1"), row("label", "profile"), at(2), noPositions());
        withSiblingConflict.applyElement(region("r1"), row("label", "region"), at(3), noPositions());
        assertThatThrownBy(() -> withSiblingConflict.render(List.of(PROFILE, flat(REGION_STATE, REGION_STATE))))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT));

        RootAssembly withPathConflict = root();
        withPathConflict.applyElement(profile("p1"), row("contact", "flat"), at(2), noPositions());
        EmbedSlot nestedPath = new EmbedSlot("contact.address", EmbedAs.OBJECT, List.of());
        assertThatThrownBy(() -> withPathConflict.render(List.of(nestedPath, PROFILE)))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(NestError.FLAT_FIELD_CONFLICT));
    }

    @Test
    void updatesKeyChangesAndDeletesRemainOneFlatRowAndRemoveOldFields() {
        RootAssembly assembly = root();
        ElementRef original = profile("p1");
        assembly.applyElement(original, row("tier", "gold", "nickname", "Ada"), at(2), noPositions());
        Map<String, Object> first = assembly.render(PROFILE_ONLY).orElseThrow();
        assertThat(first).containsEntry("nickname", "Ada");

        assembly.applyElement(original, row("tier", "platinum"), at(3), noPositions());
        Map<String, Object> updated = assembly.render(PROFILE_ONLY).orElseThrow();
        assertThat(updated).containsEntry("tier", "platinum").doesNotContainKey("nickname");
        assertThat(assembly.embedsNotRendered(PROFILE_ONLY, updated)).contains("nickname");

        ElementRef renamed = profile("p2");
        assembly.reparentElement(original, renamed, row("tier", "platinum"), at(4), noPositions());
        assertThat(assembly.render(PROFILE_ONLY).orElseThrow()).containsEntry("tier", "platinum");

        assembly.deleteElement(renamed, at(5), noPositions());
        Map<String, Object> deleted = assembly.render(PROFILE_ONLY).orElseThrow();
        assertThat(deleted).containsOnlyKeys("id", "customer_ref");
        assertThat(assembly.embedsNotRendered(PROFILE_ONLY, deleted)).contains("tier", "nickname");
    }

    @Test
    void aRestartKeepsTheFlatFieldNamesADeleteMustRemove() throws Exception {
        RootAssembly before = root();
        before.applyElement(profile("p1"), row("tier", "gold", "nickname", "Ada"), at(2), noPositions());
        before.render(PROFILE_ONLY).orElseThrow();

        RootAssembly restored = roundTrip(before);
        restored.deleteElement(profile("p1"), at(3), noPositions());
        Map<String, Object> rendered = restored.render(PROFILE_ONLY).orElseThrow();

        assertThat(restored.embedsNotRendered(PROFILE_ONLY, rendered)).contains("tier", "nickname");
    }

    @Test
    void descendantsOfAFlatRowAreMergedAtTheParentLevel() {
        EmbedSlot addresses = new EmbedSlot("addresses", EmbedAs.ARRAY, List.of());
        EmbedSlot profileWithChildren = new EmbedSlot(
                PROFILE_STATE,
                null,
                PROFILE_STATE,
                EmbedAs.FLAT,
                null,
                null,
                List.of(addresses));
        RootAssembly assembly = root();
        assembly.applyElement(
                new ElementRef(List.of(PROFILE_STATE), null, List.of("p1"), "profile-1"),
                row("tier", "gold"), at(2), noPositions());
        assembly.applyElement(
                new ElementRef(List.of(PROFILE_STATE, "addresses"), "profile-1", List.of("a1"), null),
                row("city", "Paris"), at(3), noPositions());

        Map<String, Object> document = assembly.render(List.of(profileWithChildren)).orElseThrow();
        assertThat(document).containsEntry("tier", "gold");
        assertThat(document.get("addresses")).isEqualTo(List.of(row("city", "Paris")));
    }

    @Test
    void manyParentsCanFlattenTheSameReferencedRowAndFollowItsDeletion() {
        EmbedSlot region = new EmbedSlot(
                REGION_STATE,
                null,
                REGION_STATE,
                EmbedAs.FLAT,
                List.of("customer_ref"),
                CUSTOMER_LOOKUP,
                List.of());
        RootAssembly first = root();
        RootAssembly second = new RootAssembly();
        second.applyRoot(row("id", 8, "customer_ref", 9), at(1));
        Map<String, Map<Object, Map<String, Object>>> rows = Map.of(
                CUSTOMER_LOOKUP,
                Map.of(List.of(9), row("region_name", "east", "timezone", "UTC+8")));

        assertThat(first.render(List.of(region), rows).orElseThrow()).containsEntry("region_name", "east");
        assertThat(second.render(List.of(region), rows).orElseThrow()).containsEntry("region_name", "east");

        Map<String, Map<Object, Map<String, Object>>> deleted = Map.of(
                CUSTOMER_LOOKUP,
                Map.of(List.of(9), Map.of()));
        Map<String, Object> firstAfter = first.render(List.of(region), deleted).orElseThrow();
        Map<String, Object> secondAfter = second.render(List.of(region), deleted).orElseThrow();
        assertThat(firstAfter).doesNotContainKeys("region_name", "timezone");
        assertThat(secondAfter).doesNotContainKeys("region_name", "timezone");
        assertThat(first.embedsNotRendered(List.of(region), firstAfter)).contains("region_name", "timezone");
        assertThat(second.embedsNotRendered(List.of(region), secondAfter)).contains("region_name", "timezone");
    }

    private static RootAssembly root() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 7, "customer_ref", 9), at(1));
        return assembly;
    }

    private static EmbedSlot flat(String stateField, String diagnosticPath) {
        return new EmbedSlot(stateField, null, diagnosticPath, EmbedAs.FLAT, null, null, List.of());
    }

    private static ElementRef profile(Object key) {
        return new ElementRef(List.of(PROFILE_STATE), null, List.of(key), null);
    }

    private static ElementRef region(Object key) {
        return new ElementRef(List.of(REGION_STATE), null, List.of(key), null);
    }

    private static RootAssembly roundTrip(RootAssembly assembly) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (RootAssembly) in.readObject();
        }
    }
}
