package io.tapstate.core.validation;

import static io.tapstate.core.validation.ValidationNestFieldCheck.*;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ValidationNestParentFieldConflictsTest {
    private static final ValidationNestFieldCheck.Limits FIELDS = new ValidationNestFieldCheck.Limits(65536, 10000, 32, 32);
    private static final ValidationValues.Limits VALUES = new ValidationValues.Limits(65536, 100, 16);

    @Test
    void parentObjectAndArrayFieldsConflictWithDeclaredPathsEvenWithNoReferenceOrEmptyChildren() {
        var parent = new LinkedHashMap<String, Object>();
        parent.put("id", 1); parent.put("customer_id", null);
        parent.put("customer", Map.of("legacy", 1)); parent.put("items", List.of()); parent.put("known", 7);
        var claims = new ArrayList<Field>();
        parent.keySet().forEach(name -> claims.add(new Field("parent", FieldKind.PARENT, List.of(name))));
        claims.add(new Field("customer-reference", FieldKind.EMBED_PATH, List.of("customer")));
        claims.add(new Field("item-gather", FieldKind.EMBED_PATH, List.of("items")));
        var expected = new LinkedHashMap<>(parent);
        var target = new LinkedHashMap<>(parent); target.put("customer", Map.of("arbitrary", 99)); target.put("items", List.of(99)); target.put("known", 8);
        for (boolean reverse : List.of(false, true)) {
            if (reverse) Collections.reverse(claims);
            var observation = observation(1); var root = builder(observation);
            var checked = check(observation, claims, FIELDS, root);
            assertThat(checked.conflicts()).hasSize(2);
            assertThat(checked.uncontested()).extracting(Field::path)
                    .containsExactlyInAnyOrder(List.of("id"), List.of("customer_id"), List.of("known"));
            ValidationFieldCheck.checkPartial(observation, List.of(), ValidationValues.ownRow(parent, VALUES),
                    ValidationValues.ownRow(target, VALUES), Optional.empty(), Optional.empty(), Optional.empty(), String::equals,
                    new ValidationValueComparison.TraversalLimits(VALUES, 10000),
                    List.of(List.of("customer"), List.of("items")), root);
            var result = finish(root);
            assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_FIELD_CONFLICT, 2L, Kind.TARGET_VALUE, 1L));
            assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.TARGET_VALUE)
                    .singleElement().satisfies(finding -> assertThat(finding.location().fieldPath()).containsExactly("known"));
            assertThat(result.observedAgreement()).isFalse();
            assertThat(parent).isEqualTo(expected);
        }
        var next = observation(2); var healthy = builder(next);
        assertThat(check(next, List.of(new Field("parent", FieldKind.PARENT, List.of("id")),
                new Field("child", FieldKind.EMBED_PATH, List.of("customer"))), FIELDS, healthy).conflicts()).isEmpty();
        assertThat(finish(healthy).observedAgreement()).isTrue();
    }

    @Test
    void parentClaimsAndEmbeddedPathsRetainKindsAndActualDiagnosticLocations() {
        var parent = new Field("same-owner", FieldKind.PARENT, List.of("customer.address"));
        var embedded = new Field("same-owner", FieldKind.EMBED_PATH, List.of("customer", "address"));
        var safe = new Field("parent", FieldKind.PARENT, List.of("customer.address2"));
        for (var claims : List.of(List.of(parent, embedded, safe), List.of(embedded, safe, parent))) {
            var observation = observation(1); var root = builder(observation);
            var checked = check(observation, claims, FIELDS, root);
            assertThat(checked.conflicts()).containsExactly(new Conflict(embedded, parent));
            assertThat(checked.uncontested()).containsExactly(safe);
            var result = finish(root);
            assertThat(result.notCompared()).extracting(blocked -> blocked.location().fieldPath())
                    .contains(List.of("customer", "address"), List.of("customer.address"));
            assertThat(result.counts()).containsOnlyKeys(Kind.SOURCE_FIELD_CONFLICT);
        }
    }

    private static ValidationObservation observation(long key) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(key))), List.of()), 1);
    }
    private static Builder builder(ValidationObservation observation) {
        return ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_EXIST,
                TargetPresence.PRESENT, new ValidationRootCheck.Limits(65536, 100, 100, 20, 4))
                .read(Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "complete", Instant.EPOCH, Instant.EPOCH))
                .read(Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "complete", Instant.EPOCH, Instant.EPOCH));
    }
    private static ValidationRootCheck finish(Builder root) {
        return root.checked(Check.IDENTITY).checked(Check.PARTICIPATION).checked(Check.STRUCTURE).checked(Check.VALUES).finish();
    }
}
