package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

class ValidationNestTreeTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(256 * 1024, 2000, 24);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    private static final ValidationObservation ROOT = new ValidationObservation(new ValidationRunIdentity(
            new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0), "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
            new ValidationRoot("mapping", "output", key(102), List.of()), 1);
    private static final ValidationKeyBinding ID = keys("id");

    @Test void rootReaderTraversesObjectArrayObjectAndSiblingWithActualUnorderedTarget() {
        var fixture = new Fixture(); var result = fixture.read(List.of(target(false, false)));
        assertThat(result.observedAgreement()).isTrue();
        assertThat(fixture.opened).containsExactly("root", "customer", "items", "product", "product", "shipping", "target");
        assertThat(fixture.cursors).allSatisfy(cursor -> {
            assertThat(cursor.closed).isTrue(); assertThat(cursor.borrowed).isEqualTo(cursor.released);
        });
        assertThat(result.reads()).hasSize(7);
    }

    @Test void wrongNestedValueAndIndependentSiblingRemainAtTheirOriginalRootAndAncestor() {
        var fixture = new Fixture(); var result = fixture.read(List.of(target(true, false)));
        assertThat(result.counts()).containsEntry(Kind.TARGET_VALUE, 2L);
        assertThat(result.findings().keySet()).anySatisfy(finding -> {
            assertThat(finding.location().scope().rootKey()).isEqualTo(key(102));
            assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("customer", "items"), key(11)));
            assertThat(finding.location().fieldPath()).containsExactly("product", "name");
        }).anySatisfy(finding -> {
            assertThat(finding.location().scope().ancestors()).isEmpty();
            assertThat(finding.location().fieldPath()).containsExactly("shipping", "label");
        });
    }

    @Test void wholeUnknownChildRetainsDefinedLiteralAndSourceProblemEvenWithoutUniqueTargetRoot() {
        var fixture = new Fixture(); fixture.partialProduct = true;
        var result = fixture.read(List.of(target(false, true)));
        assertThat(result.counts()).containsEntry(Kind.SOURCE_FIELD_INVALID, 1L).containsEntry(Kind.TARGET_VALUE, 1L);
        assertThat(result.findings().keySet()).anySatisfy(finding -> {
            assertThat(finding.kind()).isEqualTo(Kind.TARGET_VALUE);
            assertThat(finding.location().fieldPath()).containsExactly("product", "known");
            assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("customer", "items"), key(11)));
        });
        assertThat(result.notCompared()).isNotEmpty();
        for (var targets : List.of(List.<Map<String, Object>>of(), List.of(target(false, true), target(false, true)))) {
            var other = new Fixture(); other.partialProduct = true;
            var withoutUniqueTarget = other.read(targets);
            assertThat(withoutUniqueTarget.counts()).containsEntry(Kind.SOURCE_FIELD_INVALID, 1L);
            assertThat(withoutUniqueTarget.observedAgreement()).isFalse();
        }
    }

    @Test void invalidArrayIdentityIsARootProblemAndDoesNotHideSiblingDifferences() {
        var fixture = new Fixture();
        fixture.data.put("items", List.of(Map.of("customer_id", 9, "product_id", 501, "amount", 5)));
        var result = fixture.read(List.of(target(true, false)));
        assertThat(result.counts()).containsKey(Kind.SOURCE_FIELD_MISSING);
        assertThat(result.findings().keySet()).anySatisfy(finding -> {
            assertThat(finding.kind()).isEqualTo(Kind.TARGET_VALUE);
            assertThat(finding.location().fieldPath()).containsExactly("shipping", "label");
        });
        assertThat(fixture.opened).doesNotContain("product");
    }

    @Test void nullReferenceOmitsTheObjectAndMissingReferenceStaysASourceProblem() {
        var fixture = new Fixture(); var raw = new HashMap<>(order()); raw.put("customer_id", null);
        fixture.data.put("root", List.of(raw)); var target = new HashMap<>(target(false, false)); target.put("customer_id", null); target.remove("customer");
        assertThat(fixture.read(List.of(target)).observedAgreement()).isTrue();
        assertThat(fixture.opened).doesNotContain("customer", "items", "product");
        var missing = new Fixture(); raw = new HashMap<>(order()); raw.remove("customer_id"); missing.data.put("root", List.of(raw));
        assertThat(missing.read(List.of(target)).counts()).containsKey(Kind.SOURCE_FIELD_MISSING);
        assertThat(missing.opened).doesNotContain("customer", "items", "product");
    }

    @Test void parentConflictAndReadFailureCannotTurnIntoAnEmptySuccessfulTree() {
        var conflict = new Fixture(); var raw = new HashMap<>(order()); raw.put("customer", Map.of("legacy", 1)); conflict.data.put("root", List.of(raw));
        var result = conflict.read(List.of(target(true, false)));
        assertThat(result.counts()).containsKey(Kind.SOURCE_FIELD_CONFLICT);
        assertThat(conflict.opened).doesNotContain("customer", "items", "product");
        var failed = new Fixture(); failed.failProduct = true;
        assertThatThrownBy(() -> failed.read(List.of(target(false, false)))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.READ_FAILED));
        assertThat(failed.opened).doesNotContain("target");
        assertThat(failed.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
    }

    @Test void excludedArrayRowsDoNotRequireTheirUnusedElementIdentity() {
        var fixture = new Fixture(); fixture.excludeItems = true; fixture.sourceElement = "unused-element";
        var target = new HashMap<>(target(false, false));
        target.put("customer", Map.of("id", 9, "name", "Ada", "items", List.of()));
        var result = fixture.read(List.of(target));
        assertThat(result.observedAgreement()).isTrue();
        assertThat(fixture.opened).doesNotContain("product");
    }

    @Test void duplicateTargetRootsStillReserveTheirProjectedRowsBeforeRetention() {
        var fixture = new Fixture(); fixture.noChildren = true; fixture.bytes = 512 * 1024; fixture.wideTarget = true;
        assertThatThrownBy(() -> fixture.read(List.of(order(), order()))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "nestTree.bytes"));
        assertThat(fixture.opened).containsExactly("root", "target");
        assertThat(fixture.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
    }

    @Test void childRuleEvidenceCannotMaskAnotherAncestorOrSiblingObject() {
        var fixture = new Fixture(); fixture.foreignEvidence = true;
        assertThatThrownBy(() -> fixture.read(List.of(target(true, false)))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("field", "nestTree.ruleEvidenceScope"));
        assertThat(fixture.opened).doesNotContain("target");
    }

    @Test void arrayMemberProjectionKeepsDefinedLiteralAndRebindsItsUnknownScope() {
        var fixture = new Fixture(); fixture.partialItem = true;
        var target = new HashMap<>(target(false, false));
        var item = new HashMap<>(item(11)); item.put("known", 8);
        target.put("customer", Map.of("id", 9, "name", "Ada", "items", List.of(item,
                Map.of("id", 12, "customer_id", 9, "product_id", 502, "amount", 6, "product", Map.of("id", 502, "name", "P2")))));
        var result = fixture.read(List.of(target));
        assertThat(result.counts()).containsEntry(Kind.SOURCE_FIELD_INVALID, 1L).containsEntry(Kind.TARGET_VALUE, 1L);
        assertThat(result.findings().keySet()).anySatisfy(finding -> {
            assertThat(finding.kind()).isEqualTo(Kind.TARGET_VALUE);
            assertThat(finding.location().fieldPath()).containsExactly("known");
            assertThat(finding.location().scope().ancestors()).containsExactly(new ValidationRoot.Ancestor(List.of("customer", "items"), key(11)));
        });
    }

    @Test void targetTraversalAlsoConsumesTheSameRootRowBudget() {
        var fixture = new Fixture(); fixture.rowLimit = 8;
        assertThatThrownBy(() -> fixture.read(List.of(target(false, false)))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "nestTree.rows"));
    }

    @Test void malformedTargetElementsStillConsumeRootRowsAndValidElementsAreChargedOnce() {
        var fixture = new Fixture(); fixture.rootArrayOnly = true; fixture.rowLimit = 3; fixture.data.put("items", List.of());
        var target = new HashMap<>(order()); target.put("items", List.of(Map.of("amount", 1), Map.of("amount", 2), Map.of("amount", 3), Map.of("amount", 4)));
        assertThatThrownBy(() -> fixture.read(List.of(target))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "nestTree.rows"));
        var exact = new Fixture(); exact.rowLimit = 14;
        assertThat(exact.read(List.of(target(false, false))).observedAgreement()).isTrue();
        var shortByOne = new Fixture(); shortByOne.rowLimit = 13;
        assertThatThrownBy(() -> shortByOne.read(List.of(target(false, false)))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "nestTree.rows"));
    }

    @Test void completeRootQueryBudgetIncludesSourceAndTargetWithoutOpeningBeyondTheLimit() {
        var fixture = new Fixture(); fixture.queryLimit = 6;
        assertThatThrownBy(() -> fixture.read(List.of(target(false, false)))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "nestTree.queries"));
        assertThat(fixture.opened).hasSize(6).doesNotContain("target");
    }

    private static final class Fixture {
        final Map<String, List<Map<String, Object>>> data = new HashMap<>();
        final List<String> opened = new ArrayList<>(); final List<Cursor> cursors = new ArrayList<>();
        final AtomicInteger queryIds = new AtomicInteger(); boolean partialProduct; boolean failProduct; int queryLimit = 20;
        boolean excludeItems; String sourceElement = "id"; boolean noChildren; long bytes = 32 * 1024 * 1024;
        boolean wideTarget; boolean foreignEvidence; boolean partialItem; int rowLimit = 100; boolean rootArrayOnly;
        Fixture() {
            data.put("root", List.of(order())); data.put("customer", List.of(Map.of("id", 9, "name", "Ada")));
            data.put("items", List.of(item(11), item(12)));
            data.put("product", List.of(Map.of("id", 501, "name", "P1"), Map.of("id", 502, "name", "P2")));
            data.put("shipping", List.of(Map.of("id", 77, "label", "ship")));
        }
        ValidationRootCheck read(List<Map<String, Object>> targets) {
            var product = embed("product", Map.of("id", "product_id"), EmbedAs.OBJECT, List.of());
            var items = embed("items", Map.of("customer_id", "id"), EmbedAs.ARRAY, List.of(product));
            var customer = embed("customer", Map.of("id", "customer_id"), EmbedAs.OBJECT, List.of(items));
            var shipping = embed("shipping", Map.of("id", "shipping_id"), EmbedAs.OBJECT, List.of());
            var plan = ValidationNestRelations.compile("tree", new NestRoot("root", List.of("id"), null, null, noChildren ? List.of() : rootArrayOnly ? List.of(items) : List.of(customer, shipping)),
                    data.keySet(), Map.of(), new ValidationNestRelations.Limits(16, 8, 1024 * 1024));
            var bindings = new LinkedHashMap<List<String>, ValidationNestTree.Binding>(); bind(plan.children(), bindings);
            var tree = new ValidationNestTree(new ValidationNestObjectProjection.Scope(ROOT.run(), "mapping", "output"), plan,
                    new ValidationNestObjectProjection.RootKeys(ID, ID), bindings,
                    new ValidationNestTree.Limits(ROW, bytes, 64 * 1024 * 1024, 100_000,
                            queryLimit, rowLimit, 8, 20, new Limits(4 * 1024 * 1024, 100, 100, 256, 20)), Set.of(), String::equals);
            return tree.read(ROOT, new ValidationRootReader.Query("root-source", "v1", () -> cursor("root", data.get("root"))),
                    new ValidationRootReader.Query("target", "v1", () -> cursor("target", targets)), CLOCK, () -> {}, rule("root"), raw -> {
                        var fields = new HashMap<String, Object>(raw.fields()); if (wideTarget) fields.put("payload", "x".repeat(90_000));
                        return new ValidationRootReader.Target(wideTarget ? row(fields) : raw,
                                identity("target", ((Number) raw.fields().get("id")).longValue()));
                    });
        }
        void bind(List<ValidationNestRelations.Relation> relations, Map<List<String>, ValidationNestTree.Binding> bindings) {
            for (var relation : relations) {
                var lookup = new ValidationNestLookupBinding("tree", relation,
                        relation.join().stream().map(pair -> component(pair.parent())).toList(),
                        keys(relation.join().getFirst().child()), ROW);
                var array = relation.shape() == EmbedAs.ARRAY
                        ? Optional.of(new ValidationNestTree.ArrayKeys(ID, keys(sourceElement), ID, Optional.of(ID))) : Optional.<ValidationNestTree.ArrayKeys>empty();
                bindings.put(relation.treePath(), new ValidationNestTree.Binding(lookup, request -> {
                    var selected = data.get(relation.alias()).stream().filter(fields ->
                            key(((Number) fields.get(relation.join().getFirst().child())).longValue()).equals(request.key())).toList();
                    return new ValidationRootReader.Query("child-" + queryIds.incrementAndGet(), "v1", () -> cursor(relation.alias(), selected));
                }, rule(relation.alias()), array));
                bind(relation.children(), bindings);
            }
        }
        ValidationNestTree.Rule rule(String alias) {
            return (context, raw) -> {
                if (failProduct && alias.equals("product")) throw new TapstateException(ValidationError.READ_FAILED, Map.of("table", "product"), null);
                var identityMap = ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW);
                var view = identityMap.apply(raw, List.of()); var evidence = ValidationRootReader.SourceEvidence.empty();
                if (excludeItems && alias.equals("items")) return new ValidationNestTree.Row(new ValidationRootReader.Source(
                        new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.EXCLUDED, view.row(), Optional.empty()), List.of()), view);
                if (foreignEvidence && alias.equals("product") && ((Number) raw.fields().get("id")).intValue() == 501) {
                    var at = new Location(context.root().root(), List.of("shipping", "label")); var cause = new Finding(Kind.SOURCE_FIELD_INVALID, at);
                    evidence = new ValidationRootReader.SourceEvidence(context.root(), List.of(),
                            List.of(new ValidationRootReader.SourceIssue(cause, Optional.empty(), List.of(at))));
                }
                if (partialProduct && alias.equals("product") && ((Number) raw.fields().get("id")).intValue() == 501
                        || partialItem && alias.equals("items") && ((Number) raw.fields().get("id")).intValue() == 11) {
                    view = ValidationStaticMap.compile("literal", new TransformBody.MapProjection(Map.of("known", new FieldRule.Literal(7))), ROW)
                            .apply(identityMap.apply(raw, List.of(List.of())));
                    var at = context.scope().location(List.of()); var cause = new Finding(Kind.SOURCE_FIELD_INVALID, at);
                    evidence = new ValidationRootReader.SourceEvidence(context.root(), List.of(), List.of(new ValidationRootReader.SourceIssue(cause,
                            Optional.of(new SampledSide(identity(alias, ((Number) raw.fields().get("id")).longValue()), raw, context.readId())), List.of(at))));
                }
                return new ValidationNestTree.Row(new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(
                        ValidationRelationRows.Disposition.INCLUDED, view.row(), raw.fields().get("id") instanceof Number actualId
                                ? Optional.of(identity(alias, actualId.longValue())) : Optional.empty()),
                        List.of(), evidence), view);
            };
        }
        Cursor cursor(String alias, List<Map<String, Object>> rows) {
            opened.add(alias); var cursor = new Cursor(rows.stream().map(ValidationNestTreeTest::row).toList()); cursors.add(cursor); return cursor;
        }
    }
    private static final class Cursor implements ValidationRowCursor {
        final List<ValidationValues.Row> values; int borrowed; int released; boolean closed;
        Cursor(List<ValidationValues.Row> values) { this.values = values; }
        @Override public Optional<ValidationRowCursor.Row> next() {
            if (closed || borrowed != released) throw new IllegalStateException("cursor ownership");
            if (borrowed == values.size()) return Optional.empty();
            int index = borrowed++;
            return Optional.of(new ValidationRowCursor.Row() {
                @Override public long ordinal() { return index; }
                @Override public ValidationValues.Row values() { return values.get(index); }
                @Override public void close() { released++; }
            });
        }
        @Override public void close() { closed = true; }
    }
    private static Embed embed(String name, Map<String, String> on, EmbedAs as, List<Embed> children) {
        return new Embed(name, on, as, name, List.of("id"), null, null, null, children);
    }
    private static Map<String, Object> order() { return Map.of("id", 102, "customer_id", 9, "shipping_id", 77, "label", "ok"); }
    private static Map<String, Object> item(int id) { return Map.of("id", id, "customer_id", 9, "product_id", id + 490, "amount", id - 6); }
    private static Map<String, Object> target(boolean wrong, boolean partial) {
        var root = new HashMap<>(order()); var customer = new HashMap<String, Object>(Map.of("id", 9, "name", "Ada"));
        var items = new ArrayList<Map<String, Object>>();
        for (int id : List.of(12, 11)) {
            var item = new HashMap<>(item(id)); var product = new HashMap<String, Object>(Map.of("id", id + 490, "name", id == 11 ? wrong ? "wrong" : "P1" : "P2"));
            if (partial && id == 11) product.put("known", 8);
            item.put("product", product); items.add(item);
        }
        customer.put("items", items); root.put("customer", customer);
        root.put("shipping", Map.of("id", 77, "label", wrong ? "wrong" : "ship")); return root;
    }
    private static ValidationKeyBinding keys(String field) { return new ValidationKeyBinding(field, List.of(component(field)), Set.of(), ROW); }
    private static ValidationKeyBinding.Component component(String field) { return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of()); }
    private static ValidationPhysicalIdentity identity(String table, long id) { return new ValidationPhysicalIdentity("connection", "db", null, table, List.of(List.of("id")), key(id)); }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationValues.Row row(Map<String, ?> value) { return ValidationValues.ownRow(value, ROW); }
}
