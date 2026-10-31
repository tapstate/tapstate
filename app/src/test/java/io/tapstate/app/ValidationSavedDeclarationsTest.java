package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ValidationSavedDeclarationsTest {
    @Test void theSelectedDestinationIsNotAnUpstreamSourceEvenWhenItIsInTheHashClosure() {
        var original = inputs(false, false, "target"); var previous = original.task(); var mapping = previous.comparison().mappings().getFirst();
        var wrong = new ValidationTaskSpec(previous.comparison().pipelineId(), previous.comparison().dependencyHashes(), 1, previous.comparison().comparisonRulesVersion(),
                List.of(new ValidationTaskSpec.Mapping(mapping.mappingId(), mapping.outputKind(), mapping.outputId(),
                        new ValidationTaskSpec.Table("target", "target", "source", null, "orders", null, null), mapping.target(), null)));
        var saved = new ValidationTaskStore.Snapshot(previous.task(), previous.revision(), previous.commandId(), wrong, previous.settings(),
                previous.schedulingRevision(), previous.schedulingCommandId(), false, previous.full(), previous.incremental());
        var forged = new ValidationTaskStore.Inputs(saved, original.configurationRevision(), original.canonicalResources());
        assertThatThrownBy(() -> ValidationSavedDeclarations.inspect(forged, "target")).isInstanceOf(TapstateException.class).hasMessageContaining("sourceScope");
    }
    @Test void selectedSavedDeclarationsStayOwnedAndDoNotExposeConnectionSecretsInDiagnostics() {
        var inputs = inputs(false, false, "target"); var selected = ValidationSavedDeclarations.inspect(inputs, "target");
        assertThat(selected.mapping("mapping").sync().id()).isEqualTo("out");
        assertThat(selected.mapping("mapping").routes()).extracting(value -> value.source()).contains("input");
        assertThat(selected.toString()).doesNotContain("source-secret", "target-secret", "password");
        var source = (SourceResource) selected.artifacts().get("input").orElseThrow();
        assertThatThrownBy(() -> source.config().put("password", "mutated")).isInstanceOf(UnsupportedOperationException.class);
        var next = (SourceResource) selected.artifacts().get("input").orElseThrow();
        assertThat(next).isEqualTo(source).isNotSameAs(source); assertThat(next.config()).containsEntry("password", "source-secret");
        assertThatThrownBy(() -> selected.artifacts().saveAll(List.of(source))).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void aSavedMappingCannotChooseAnUnrelatedTargetOrHideExtraResourcePins() {
        assertThatThrownBy(() -> ValidationSavedDeclarations.inspect(inputs(false, false, "another"), "target"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("targetScope");
        assertThatThrownBy(() -> ValidationSavedDeclarations.inspect(inputs(false, true, "target"), "target"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("dependencyClosure");
    }

    @Test void evenAnInternalDescriptorCannotReplaceTheActualPinnedResourceBody() {
        var original = inputs(false, false, "target"); var changed = new HashMap<>(original.canonicalResources());
        changed.put("input", new CanonicalWriter().write(new SourceResource("input", null, "fixture", Map.of("password", "different-secret"),
                SourceMode.CDC, List.of(TableRef.literal("orders")), null, null)));
        var fabricated = new ValidationTaskStore.Inputs(original.task(), original.configurationRevision(), changed);
        assertThatThrownBy(() -> ValidationSavedDeclarations.inspect(fabricated, "target"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validationInputs");
    }

    @Test void aViewUsesTheActualDefaultStoreAndItsSavedPhysicalCollection() {
        var inputs = inputs(true, false, "target");
        assertThat(ValidationSavedDeclarations.inspect(inputs, "target").mapping("mapping").view().id()).isEqualTo("view_out");
        assertThatThrownBy(() -> ValidationSavedDeclarations.inspect(inputs, "another"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("viewStore");
    }

    private static ValidationTaskStore.Inputs inputs(boolean view, boolean extra, String target) {
        var resources = new LinkedHashMap<String, Resource>();
        resources.put("input", new SourceResource("input", null, "fixture", Map.of("password", "source-secret"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        resources.put("target", new SourceResource("target", null, "fixture", Map.of("password", "target-secret"), null, null, null, null));
        if (extra || !target.equals("target")) resources.put("another", new SourceResource("another", null, "fixture", Map.of("password", "other-secret"), null, null, null, null));
        var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: [input]\n" +
                (view ? "view:\n  id: view_out\n  from: orders\n  primary_key: id\n" : "serve:\n  id: serving\n  from: orders\n  sync:\n    - id: out\n      source: target\n"));
        resources.put("flow", pipeline); String output = view ? "view_out" : "out";
        var hashes = new TreeMap<String, String>(); var canonical = new TreeMap<String, String>();
        resources.forEach((id, resource) -> { hashes.put(id, CanonicalHash.of(resource)); canonical.put(id, new CanonicalWriter().write(resource)); });
        var comparison = new ValidationTaskSpec("flow", hashes, 1, "rules", List.of(new ValidationTaskSpec.Mapping("mapping",
                view ? ValidationTaskSpec.OutputKind.VIEW : ValidationTaskSpec.OutputKind.SYNC, output,
                new ValidationTaskSpec.Table("input", "input", "source", null, "orders", null, null),
                new ValidationTaskSpec.Table(output, target, "target", null, view ? "view_out" : "orders", null, null), null)));
        var task = new ValidationReportAdmission.Task("cluster", "flow", "task", UUID.randomUUID()); var command = UUID.randomUUID();
        var snapshot = new ValidationTaskStore.Snapshot(task, 1, command, comparison, ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT, 0),
                1, command, false, new ValidationTaskStore.Mode(1, null), new ValidationTaskStore.Mode(1, null));
        return new ValidationTaskStore.Inputs(snapshot, 5, canonical);
    }
}
