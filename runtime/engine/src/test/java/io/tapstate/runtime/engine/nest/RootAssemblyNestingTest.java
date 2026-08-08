package io.tapstate.runtime.engine.nest;

import io.tapstate.core.model.EmbedAs;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.element;
import static io.tapstate.runtime.engine.nest.NestFixtures.listAt;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RootAssemblyNestingTest {

    private static final List<EmbedSlot> POLICIES_CLAIMS_DOCUMENTS = List.of(
            new EmbedSlot("policies", EmbedAs.ARRAY, List.of(
                    new EmbedSlot("claims", EmbedAs.ARRAY, List.of(
                            new EmbedSlot("documents", EmbedAs.ARRAY, List.of()))))));

    /** A claims element as the four-level shape renders it: its own row, plus the empty documents array. */
    private static Map<String, Object> claim(String number) {
        return row("claim_no", number, "documents", List.of());
    }

    private static RootAssembly customerWithOnePolicy() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        assembly.applyElement(element(List.of("policies"), null, "P1", "P1"), row("policy_no", "P1"), at(2), noPositions());
        return assembly;
    }

    @Test
    void aGrandchildLandsUnderItsOwnParentElement() {
        RootAssembly assembly = customerWithOnePolicy();
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(3), noPositions());

        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies")).hasSize(1);
        assertThat(listAt(document, "policies", "claims")).containsExactly(claim("CL1"));
    }

    @Test
    void aFourLevelTreeAssemblesAllTheWayDown() {
        RootAssembly assembly = customerWithOnePolicy();
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", "CL1"), row("claim_no", "CL1"), at(3), noPositions());
        // The deepest row names only its own parent - it never carries the root's key.
        assembly.applyElement(
                element(List.of("policies", "claims", "documents"), "CL1", "D1", null),
                row("document_no", "D1"), at(4), noPositions());

        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies", "claims", "documents"))
                .containsExactly(row("document_no", "D1"));
    }

    @Test
    void siblingEmbedsWhoseIdentityValuesCollideStayApart() {
        List<EmbedSlot> twoBranches = List.of(
                new EmbedSlot("policies", EmbedAs.ARRAY, List.of(
                        new EmbedSlot("claims", EmbedAs.ARRAY, List.of()))),
                new EmbedSlot("orders", EmbedAs.ARRAY, List.of(
                        new EmbedSlot("items", EmbedAs.ARRAY, List.of()))));
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        // Two auto-increment keys from different tables, both 77.
        assembly.applyElement(element(List.of("policies"), null, "P77", 77), row("policy_no", "P77"), at(2), noPositions());
        assembly.applyElement(element(List.of("orders"), null, "O77", 77), row("order_no", "O77"), at(3), noPositions());

        assembly.applyElement(
                element(List.of("policies", "claims"), 77, "CL1", null), row("claim_no", "CL1"), at(4), noPositions());
        assembly.applyElement(
                element(List.of("orders", "items"), 77, "IT1", null), row("sku", "IT1"), at(5), noPositions());

        Map<String, Object> document = assembly.render(twoBranches).orElseThrow();
        assertThat(listAt(document, "policies", "claims")).containsExactly(row("claim_no", "CL1"));
        assertThat(listAt(document, "orders", "items")).containsExactly(row("sku", "IT1"));
    }

    @Test
    void aChildWaitsWhileItsParentElementIsMissingAndAppearsWhenItArrives() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", null), row("claim_no", "CL1"), at(2), noPositions());

        Map<String, Object> beforeTheParent = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(beforeTheParent, "policies")).isEmpty();

        assembly.applyElement(element(List.of("policies"), null, "P1", "P1"), row("policy_no", "P1"), at(3), noPositions());

        Map<String, Object> afterTheParent = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(afterTheParent, "policies", "claims")).containsExactly(claim("CL1"));
    }

    @Test
    void aChildDeleteThatOutrunsItsParentStillDeletes() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        ElementRef claim = element(List.of("policies", "claims"), "P1", "CL1", null);
        assembly.deleteElement(claim, at(10), noPositions());

        assembly.applyElement(element(List.of("policies"), null, "P1", "P1"), row("policy_no", "P1"), at(11), noPositions());
        assembly.applyElement(claim, row("claim_no", "CL1"), at(9), noPositions());

        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies", "claims")).isEmpty();
    }

    @Test
    void updatingAParentRowKeepsTheChildrenItAlreadyHas() {
        RootAssembly assembly = customerWithOnePolicy();
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", null), row("claim_no", "CL1"), at(3), noPositions());

        assembly.applyElement(
                element(List.of("policies"), null, "P1", "P1"), row("policy_no", "P1", "status", "closed"), at(4), noPositions());

        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies").get(0)).containsEntry("status", "closed");
        assertThat(listAt(document, "policies", "claims")).containsExactly(claim("CL1"));
    }

    @Test
    void deletingAnElementKeepsItsSubtreeForTheRebuild() {
        RootAssembly assembly = customerWithOnePolicy();
        ElementRef policy = element(List.of("policies"), null, "P1", "P1");
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", null), row("claim_no", "CL1"), at(3), noPositions());

        assembly.deleteElement(policy, at(4), noPositions());
        assertThat(listAt(assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow(), "policies")).isEmpty();

        assembly.applyElement(policy, row("policy_no", "P1"), at(5), noPositions());
        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies", "claims")).containsExactly(claim("CL1"));
    }

    @Test
    void aChildOfADeletedParentIsHeldRatherThanDropped() {
        RootAssembly assembly = customerWithOnePolicy();
        ElementRef policy = element(List.of("policies"), null, "P1", "P1");
        assembly.deleteElement(policy, at(3), noPositions());

        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", null), row("claim_no", "CL1"), at(4), noPositions());
        assertThat(listAt(assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow(), "policies")).isEmpty();

        assembly.applyElement(policy, row("policy_no", "P1"), at(5), noPositions());
        Map<String, Object> document = assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies", "claims")).containsExactly(claim("CL1"));
    }

    @Test
    void anElementAlwaysNamesTheEmbedItBelongsTo() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ElementRef(List.of(), null, List.of("CL1"), null));
    }

    @Test
    void detachingAnElementThatIsNotThereStillLeavesATombstone() {
        RootAssembly assembly = customerWithOnePolicy();
        ElementRef claim = element(List.of("policies", "claims"), "P1", "CL1", null);

        // The old root of a cross-root move is told to detach an element it may never have received --
        // the child row could still be in flight, or a replay may deliver it again. Without a tombstone
        // here, that arrival would attach an element the source has already moved away.
        assertThat(assembly.deleteElement(claim, at(10), noPositions())).isTrue();
        assertThat(assembly.applyElement(claim, row("claim_no", "CL1"), at(9), noPositions())).isFalse();
        assertThat(listAt(assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow(), "policies", "claims"))
                .isEmpty();

        // And a genuine rebuild above the tombstone still revives it.
        assertThat(assembly.applyElement(claim, row("claim_no", "CL1"), at(11), noPositions())).isTrue();
        assertThat(listAt(assembly.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow(), "policies", "claims"))
                .containsExactly(claim("CL1"));
    }

    @Test
    void aWaitingChildSurvivesSerialization() throws Exception {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("customer_id", "C1"), at(1));
        assembly.applyElement(
                element(List.of("policies", "claims"), "P1", "CL1", null), row("claim_no", "CL1"), at(2), noPositions());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(assembly);
        }
        RootAssembly restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (RootAssembly) in.readObject();
        }

        restored.applyElement(element(List.of("policies"), null, "P1", "P1"), row("policy_no", "P1"), at(3), noPositions());
        Map<String, Object> document = restored.render(POLICIES_CLAIMS_DOCUMENTS).orElseThrow();
        assertThat(listAt(document, "policies", "claims")).containsExactly(claim("CL1"));
    }
}
