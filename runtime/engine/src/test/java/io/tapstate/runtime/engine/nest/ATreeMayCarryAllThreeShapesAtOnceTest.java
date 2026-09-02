package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.EmbedAs;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That one tree can carry all three shapes at once, and that changing any of them leaves the other two
 * where they were.
 *
 * <p><b>Everything else about rendering is measured on a tree with one shape in it.</b> The cases beside
 * this one hold an array, or an object, or a pointed-at row, and each of them is right about the shape it
 * holds. What none of them can be wrong about is the shape it does not have - and the failures worth
 * fearing here only exist when the shapes share a document: the layer holding what a document points at
 * displacing the elements gathered into it, two embeds landing on the same path, a fill-in reordering
 * what was already assembled. A tree with one shape in it is green through all three.
 *
 * <p><b>Changing each of them in turn is the second half, and it is not the same assertion three times.</b>
 * Rendering all three correctly once says they can coexist; it does not say they are held apart. An
 * implementation that keeps them in one place renders the first document correctly and then lets an edit
 * to one shape disturb another - which reads as a document that was right a moment ago and is now subtly
 * wrong, with nothing thrown and no count out of place.
 *
 * <p>This works the assembly directly rather than through a job. What a document renders as is decided
 * here, and the cases that need a running cluster need it for the state layer beneath, which is a
 * different question with its own cases.
 */
class ATreeMayCarryAllThreeShapesAtOnceTest {

    private static final String ITEMS = "items";
    private static final String PROFILE = "profile";
    private static final String CUSTOMER = "customer";

    /** The namespace the pointed-at rows are read from, named the way a compiled tree names it. */
    private static final String CUSTOMER_LOOKUP = "nest.p.step.customer";

    /**
     * The three shapes on one root: rows gathered into an array, a single row gathered into an object, and
     * a row the root points at by a column of its own.
     */
    private static final List<EmbedSlot> ALL_THREE = List.of(
            new EmbedSlot(ITEMS, EmbedAs.ARRAY, List.of()),
            new EmbedSlot(PROFILE, EmbedAs.OBJECT, List.of()),
            new EmbedSlot(CUSTOMER, EmbedAs.OBJECT, List.of("cust_ref"), CUSTOMER_LOOKUP, List.of()));

    private static ElementRef item(Object key) {
        return new ElementRef(List.of(ITEMS), null, List.of(key), null);
    }

    private static ElementRef profile() {
        return new ElementRef(List.of(PROFILE), null, List.of("only"), null);
    }

    /** What a fetch of the pointed-at rows answered with, for a customer of the given name. */
    private static Map<String, Map<Object, Map<String, Object>>> customerNamed(String name) {
        return Map.of(CUSTOMER_LOOKUP,
                Map.of(List.of(7), row("customer_id", 7, "name", name)));
    }

    /** A root holding two items, a profile and a pointer at customer 7. */
    private static RootAssembly assembled() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row("order_id", 1, "cust_ref", 7), at(1));
        assembly.applyElement(item("i1"), row("sku", "a"), at(2), noPositions());
        assembly.applyElement(item("i2"), row("sku", "b"), at(3), noPositions());
        assembly.applyElement(profile(), row("tier", "gold"), at(4), noPositions());
        return assembly;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> itemsOf(Map<String, Object> document) {
        return (List<Map<String, Object>>) document.get(ITEMS);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> objectAt(Map<String, Object> document, String path) {
        return (Map<String, Object>) document.get(path);
    }

    @Test
    @DisplayName("an array, an object and a pointed-at row render side by side in one document")
    void allThreeShapesAppearInOneDocumentEachInItsOwnShape() {
        Map<String, Object> document = assembled().render(ALL_THREE, customerNamed("Ada")).orElseThrow();

        assertThat(itemsOf(document))
                .describedAs("the gathered rows render as an array of every element under that path")
                .containsExactly(row("sku", "a"), row("sku", "b"));
        assertThat(objectAt(document, PROFILE))
                .describedAs("a single gathered row renders as the row itself, not as a list of one - "
                        + "which is the shape the declaration asked for and not a property of how many "
                        + "rows happened to arrive")
                .isEqualTo(row("tier", "gold"));
        assertThat(objectAt(document, CUSTOMER))
                .describedAs("the row the document points at is filled in from what was fetched for it, "
                        + "and is the row its own column named rather than anything gathered here")
                .isEqualTo(row("customer_id", 7, "name", "Ada"));
    }

    @Test
    @DisplayName("changing the pointed-at row leaves the gathered shapes alone")
    void aDifferentAnswerForThePointedAtRowDisturbsNeitherOfTheOthers() {
        RootAssembly assembly = assembled();

        Map<String, Object> before = assembly.render(ALL_THREE, customerNamed("Ada")).orElseThrow();
        Map<String, Object> after = assembly.render(ALL_THREE, customerNamed("Grace")).orElseThrow();

        assertThat(objectAt(after, CUSTOMER))
                .describedAs("the pointed-at row is read at render, so a new answer for it is the whole "
                        + "of what an edit to that row does - nothing about this document was rewritten")
                .isEqualTo(row("customer_id", 7, "name", "Grace"));
        assertThat(itemsOf(after))
                .describedAs("the elements gathered into the array are untouched by it. An implementation "
                        + "holding both in one place can lose them here while every count stays right")
                .isEqualTo(itemsOf(before));
        assertThat(objectAt(after, PROFILE))
                .describedAs("and so is the row gathered into the object")
                .isEqualTo(objectAt(before, PROFILE));
    }

    @Test
    @DisplayName("changing one gathered element leaves the other shapes alone")
    void anEditToAnArrayElementDisturbsNeitherThePointedAtRowNorTheObject() {
        RootAssembly assembly = assembled();

        assembly.applyElement(item("i1"), row("sku", "a-edited"), at(5), noPositions());
        Map<String, Object> document = assembly.render(ALL_THREE, customerNamed("Ada")).orElseThrow();

        assertThat(itemsOf(document))
                .describedAs("the edited element is the edited one, and it stays where it was among the "
                        + "others rather than moving to the end")
                .containsExactly(row("sku", "a-edited"), row("sku", "b"));
        assertThat(objectAt(document, CUSTOMER))
                .describedAs("the pointed-at row is not something an element edit reaches. It is not held "
                        + "in this document at all, and this is what says so")
                .isEqualTo(row("customer_id", 7, "name", "Ada"));
        assertThat(objectAt(document, PROFILE))
                .describedAs("nor is the row gathered into the object")
                .isEqualTo(row("tier", "gold"));
    }

    @Test
    @DisplayName("changing the object embed leaves the array and the pointed-at row alone")
    void anEditToTheObjectEmbedDisturbsNeitherOfTheOthers() {
        RootAssembly assembly = assembled();

        assembly.applyElement(profile(), row("tier", "platinum"), at(5), noPositions());
        Map<String, Object> document = assembly.render(ALL_THREE, customerNamed("Ada")).orElseThrow();

        assertThat(objectAt(document, PROFILE))
                .describedAs("the object embed took the edit")
                .isEqualTo(row("tier", "platinum"));
        assertThat(itemsOf(document))
                .describedAs("the array is untouched. Two embeds at one level sharing a path is the "
                        + "failure this separates out, and it shows up as an edit to one of them "
                        + "emptying or duplicating the other")
                .containsExactly(row("sku", "a"), row("sku", "b"));
        assertThat(objectAt(document, CUSTOMER))
                .describedAs("and so is the pointed-at row")
                .isEqualTo(row("customer_id", 7, "name", "Ada"));
    }

    @Test
    @DisplayName("a document whose pointed-at row was not fetched renders the other two shapes")
    void theGatheredShapesRenderWhenNothingWasFetchedForTheReference() {
        Map<String, Object> document = assembled().render(ALL_THREE, Map.of()).orElseThrow();

        assertThat(document)
                .describedAs("a reference with nothing fetched for it renders no field at all - not a "
                        + "null, which a sink would write over a value that is merely not read yet")
                .doesNotContainKey(CUSTOMER);
        assertThat(itemsOf(document))
                .describedAs("and the shapes that do not depend on a fetch are unaffected by one not "
                        + "having happened. Holding the whole document back for an unfetched reference "
                        + "is the failure here, and it looks like a document that never came out")
                .containsExactly(row("sku", "a"), row("sku", "b"));
        assertThat(objectAt(document, PROFILE)).isEqualTo(row("tier", "gold"));
    }

    @Test
    @DisplayName("the document asks for the row its own column names, once")
    void whatTheDocumentAsksToHaveFetchedIsTheKeyOnItsOwnRow() {
        assertThat(assembled().referencesNeeded(ALL_THREE))
                .describedAs("the key asked for is read off the root's own column, so nothing is stored "
                        + "per document to know what to fetch; asking for something else is how a "
                        + "document ends up carrying another row's customer")
                .isEqualTo(Map.of(CUSTOMER_LOOKUP, java.util.Set.of(List.of(7))));
    }
}
