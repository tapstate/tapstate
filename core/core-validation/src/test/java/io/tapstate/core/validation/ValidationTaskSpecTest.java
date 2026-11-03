package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationTaskSpec.*;

class ValidationTaskSpecTest {
    @Test void countPinsPhysicalScopeWithoutInventingValueCodecsOrKeys() {
        var source = new Table("input", "source", "db", null, "orders", null, null);
        var target = new Table("output", "views", "db", null, "renamed", null, null);
        var task = spec(new Mapping("mapping", OutputKind.SYNC, "output", source, target, null));
        assertThatCode(() -> task.requireFor(ValidationMethod.COUNT, ValidationMode.FULL)).doesNotThrowAnyException();
        assertThat(new ValidationRunSpec(task, ValidationMode.FULL, ValidationMethod.COUNT).fingerprint()).hasSize(64);
        assertThatThrownBy(() -> task.requireFor(ValidationMethod.UNIQUE_KEY, ValidationMode.FULL)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> task.requireFor(ValidationMethod.FULL_COLUMN, ValidationMode.INCREMENTAL)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationRunSpec(task, ValidationMode.INCREMENTAL, ValidationMethod.COUNT)).isInstanceOf(TapstateException.class);
    }

    @Test void mapAndTypeSetOrderDoNotChangeThePinnedComparisonIdentity() {
        var hashes = new LinkedHashMap<String, String>(); hashes.put("views", "c".repeat(64)); hashes.put("source", "b".repeat(64)); hashes.put("flow", "a".repeat(64));
        var a = spec(mapping());
        var reverse = new LinkedHashSet<>(List.of("integer", "decimal"));
        var key = new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, reverse);
        var original = mapping();
        var b = new ValidationTaskSpec("flow", hashes, 1, "rules-v1", List.of(new Mapping(original.mappingId(), original.outputKind(), original.outputId(),
                original.source(), original.target(), new Keys("confirmed", List.of(key), List.of(key)))));
        assertThat(a).isEqualTo(b); assertThat(a.fingerprint()).isEqualTo(b.fingerprint());
        assertThat(a.fingerprint()).isEqualTo("4e062d43642615a1fc61c7d67ab9f9482f63046ca11223248d6cab6879961ab9");
    }

    @Test void methodAndModeArePartOfTheActualRunSnapshot() {
        var task = spec(mapping());
        var count = new ValidationRunSpec(task, ValidationMode.FULL, ValidationMethod.COUNT);
        var keys = new ValidationRunSpec(task, ValidationMode.FULL, ValidationMethod.UNIQUE_KEY);
        var full = new ValidationRunSpec(task, ValidationMode.FULL, ValidationMethod.FULL_COLUMN);
        var incremental = new ValidationRunSpec(task, ValidationMode.INCREMENTAL, ValidationMethod.FULL_COLUMN);
        assertThat(List.of(count.fingerprint(), keys.fingerprint(), full.fingerprint(), incremental.fingerprint())).doesNotHaveDuplicates();
    }

    @Test void changesToActualTablesDependenciesKeysAndVersionsChangeTheFrozenSpec() {
        var original = mapping(); var baseline = spec(original).fingerprint(); var alternatives = new ArrayList<ValidationTaskSpec>();
        alternatives.add(new ValidationTaskSpec("flow", Map.of("flow", "d".repeat(64), "source", "b".repeat(64), "views", "c".repeat(64)), 1, "rules-v1", List.of(original)));
        alternatives.add(new ValidationTaskSpec("flow", hashes(), 2, "rules-v1", List.of(original)));
        alternatives.add(new ValidationTaskSpec("flow", hashes(), 1, "rules-v2", List.of(original)));
        var side = original.source();
        alternatives.add(spec(new Mapping("mapping", OutputKind.SYNC, "output", new Table(side.nodeId(), side.sourceId(), side.database(), side.schema(), "other", side.modelVersion(), side.codecBinding()), original.target(), original.keys())));
        alternatives.add(spec(new Mapping("mapping", OutputKind.SYNC, "output", new Table(side.nodeId(), side.sourceId(), side.database(), "", side.table(), side.modelVersion(), side.codecBinding()), original.target(), original.keys())));
        alternatives.add(spec(new Mapping("mapping", OutputKind.SYNC, "output", new Table(side.nodeId(), side.sourceId(), side.database(), side.schema(), side.table(), "model-v2", side.codecBinding()), original.target(), original.keys())));
        alternatives.add(spec(new Mapping("mapping", OutputKind.SYNC, "output", new Table(side.nodeId(), side.sourceId(), side.database(), side.schema(), side.table(), side.modelVersion(), "codec-v2"), original.target(), original.keys())));
        var key = new ValidationKeyBinding.Component(List.of("other-id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("decimal", "integer"));
        alternatives.add(spec(new Mapping("mapping", OutputKind.SYNC, "output", side, original.target(), new Keys("confirmed", List.of(key), original.keys().target()))));
        for (var changed : alternatives) assertThat(changed.fingerprint()).isNotEqualTo(baseline);
        assertThat(alternatives.stream().map(ValidationTaskSpec::fingerprint).toList()).doesNotHaveDuplicates();
    }

    @Test void inputMutationCannotChangeAnAlreadyFrozenSpec() {
        var hashes = new LinkedHashMap<>(hashes()); var mappings = new ArrayList<>(List.of(mapping()));
        var task = new ValidationTaskSpec("flow", hashes, 1, "rules-v1", mappings); var fixed = task.fingerprint();
        hashes.put("source", "d".repeat(64)); mappings.clear();
        assertThat(task.fingerprint()).isEqualTo(fixed); assertThat(task.mappings()).hasSize(1);
        assertThatThrownBy(() -> task.dependencyHashes().put("source", "e".repeat(64))).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> task.mappings().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void ambiguousMappingsAndUnpinnedSourcesAreRejected() {
        var mapping = mapping();
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", hashes(), 1, "rules-v1", List.of(mapping, mapping))).isInstanceOf(TapstateException.class);
        var duplicate = new Mapping("another-id", mapping.outputKind(), mapping.outputId(), mapping.source(), mapping.target(), mapping.keys());
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", hashes(), 1, "rules-v1", List.of(mapping, duplicate))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", Map.of("flow", "a".repeat(64), "source", "b".repeat(64)), 1, "rules-v1", List.of(mapping)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", Map.of("flow", "not-a-hash", "source", "b".repeat(64), "views", "c".repeat(64)), 1, "rules-v1", List.of(mapping)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void keyArityDomainsAndPhysicalFieldOrderRemainExplicit() {
        var numeric = new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("integer"));
        var text = new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.TEXT, Set.of("text"));
        assertThatThrownBy(() -> new Keys("confirmed", List.of(numeric), List.of(text))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Keys("confirmed", List.of(numeric, numeric), List.of(numeric, numeric))).isInstanceOf(TapstateException.class);
        var other = new ValidationKeyBinding.Component(List.of("other"), ValidationKey.Kind.EXACT_NUMBER, Set.of("integer"));
        var mapping = mapping();
        var first = spec(new Mapping("mapping", OutputKind.SYNC, "output", mapping.source(), mapping.target(), new Keys("confirmed", List.of(numeric, other), List.of(numeric, other))));
        var second = spec(new Mapping("mapping", OutputKind.SYNC, "output", mapping.source(), mapping.target(), new Keys("confirmed", List.of(other, numeric), List.of(other, numeric))));
        assertThat(first.fingerprint()).isNotEqualTo(second.fingerprint());
    }

    @Test void mappingCardinalityIsCheckedBeforeCopyingItsInputs() {
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", hashes(), 1, "rules-v1", java.util.Collections.nCopies(MAX_MAPPINGS + 1, mapping())))
                .isInstanceOf(TapstateException.class);
    }

    @Test void aggregateMetadataIsBoundedEvenWhenEveryIndividualCountAndFieldIsLegal() {
        var types = new LinkedHashSet<String>();
        for (int i = 0; i < 32; i++) types.add("type-" + i + "x".repeat(490));
        var key = new ValidationKeyBinding.Component(List.of("k".repeat(512)), ValidationKey.Kind.EXACT_NUMBER, types);
        var keys = new Keys("confirmed", List.of(key), List.of(key)); var mappings = new ArrayList<Mapping>();
        for (int i = 0; i < 16; i++) mappings.add(new Mapping("mapping-" + i, OutputKind.SYNC, "output",
                new Table("input", "source", "db", null, "source-" + i, "model", "codec"),
                new Table("output", "views", "db", null, "target-" + i, "model", "codec"), keys));
        assertThatThrownBy(() -> new ValidationTaskSpec("flow", hashes(), 1, "rules-v1", mappings))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validation.specMetadata");
    }

    private static ValidationTaskSpec spec(Mapping mapping) { return new ValidationTaskSpec("flow", hashes(), 1, "rules-v1", List.of(mapping)); }
    private static Map<String, String> hashes() { return Map.of("flow", "a".repeat(64), "source", "b".repeat(64), "views", "c".repeat(64)); }
    private static Mapping mapping() {
        var source = new Table("input", "source", "db", null, "orders", "model-v1", "codec-v1");
        var target = new Table("output", "views", "db", null, "renamed", "model-v1", "codec-v1");
        var key = new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("decimal", "integer"));
        return new Mapping("mapping", OutputKind.SYNC, "output", source, target, new Keys("confirmed", List.of(key), List.of(key)));
    }
}
