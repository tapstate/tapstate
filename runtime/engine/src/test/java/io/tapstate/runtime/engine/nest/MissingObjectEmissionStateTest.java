package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.EmbedAs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MissingObjectEmissionStateTest {

    @Test
    void independentlyMissingCompositeReferencesCompleteOneAtATimeAfterDeserialization() throws Exception {
        List<EmbedSlot> slots = List.of(
                new EmbedSlot("billing", EmbedAs.OBJECT, List.of("tenant", "billing_id"), "billing", List.of()),
                new EmbedSlot("shipping", EmbedAs.OBJECT, List.of("tenant", "shipping_id"), "shipping", List.of()));
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 1, "tenant", "T", "billing_id", 10, "shipping_id", 20), at(1));
        assembly.rememberMissingObjects(slots, Map.of());
        assembly.documentSent();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assembly = (RootAssembly) in.readObject();
        }
        Map<String, Map<Object, Map<String, Object>>> first = Map.of(
                "billing", Map.of(List.of("T", 10), row("name", "billing")));
        assertThat(assembly.needsReferenceUpdate(slots, first)).isTrue();
        assembly.rememberMissingObjects(slots, first);
        assembly.documentSent();
        assertThat(assembly.needsReferenceUpdate(slots, first)).isFalse();
        assertThat(assembly.mayNeedReferenceUpdate()).isTrue();
        Map<String, Map<Object, Map<String, Object>>> both = Map.of(
                "billing", first.get("billing"), "shipping", Map.of(List.of("T", 20), row("name", "shipping")));
        assertThat(assembly.needsReferenceUpdate(slots, both)).isTrue();
        assembly.rememberMissingObjects(slots, both);
        assembly.documentSent();
        assertThat(assembly.mayNeedReferenceUpdate()).isFalse();
    }

    @Test
    void retargetingAndDeletingDiscardResponsibilityForTheOldMissingIdentity() {
        List<EmbedSlot> slots = List.of(
                new EmbedSlot("customer", EmbedAs.OBJECT, List.of("cust_ref"), "customers", List.of()));
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("id", 1, "cust_ref", "old"), at(1));
        assembly.rememberMissingObjects(slots, Map.of());
        assembly.documentSent();
        assembly.applyRoot(row("id", 1, "cust_ref", "new"), at(2));
        assembly.reconcileMissingObjects(slots);
        assembly.rememberMissingObjects(slots, Map.of());
        assembly.documentSent();
        Map<String, Map<Object, Map<String, Object>>> oldArrival = Map.of(
                "customers", Map.of(List.of("old"), row("name", "old")));
        assertThat(assembly.needsReferenceUpdate(slots, oldArrival)).isFalse();
        assembly.deleteRoot(at(3));
        assertThat(assembly.mayNeedReferenceUpdate()).isFalse();
        assertThat(assembly.needsReferenceUpdate(slots,
                Map.of("customers", Map.of(List.of("new"), row("name", "new"))))).isFalse();
        assertThat(assembly.render(slots, oldArrival)).isEmpty();
    }
}
