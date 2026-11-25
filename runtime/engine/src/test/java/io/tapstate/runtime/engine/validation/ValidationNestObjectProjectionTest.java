package io.tapstate.runtime.engine.validation;

import static io.tapstate.core.validation.ValidationRootCheck.*;
import static org.assertj.core.api.Assertions.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
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
import org.junit.jupiter.api.Test;

class ValidationNestObjectProjectionTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(262144, 2000, 24);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);
    private static final ValidationRunIdentity RUN = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1),
            "task", "run", ValidationMode.FULL, "spec", 1, "rules");
    private static final ValidationObservation ROOT = new ValidationObservation(RUN, new ValidationRoot("mapping", "output", key(7), List.of()), 1);

    @Test void readsTwoIndependentObjectLevelsAndComparesTheirActualPayloadAtTheRoot() {
        var fixture = new Fixture(true, 8);
        var source = new Cursor(List.of(parent()));
        var target = new Cursor(List.of(row(Map.of("id", 7, "customer_id", 9, "known", 1,
                "customer", Map.of("id", 9, "region_id", 3, "name", "Ada", "region", Map.of("id", 3, "label", "wrong"))))));
        var result = rootRead(fixture, source, target);
        assertThat(result.counts()).containsExactly(entry(Kind.TARGET_VALUE, 1L));
        assertThat(result.findings().keySet()).singleElement().satisfies(finding ->
                assertThat(finding.location().fieldPath()).containsExactly("customer", "region", "label"));
        assertThat(fixture.requested).containsExactly(key(9), key(3));
        assertThat(fixture.cursors).allSatisfy(cursor -> { assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(cursor.rows.size()); });
        assertThat(source.closed).isTrue(); assertThat(target.closed).isTrue();
        assertThat(result.reads()).hasSize(4);
    }

    @Test void nullReferenceOmitsTheObjectWithoutOpeningAQueryButAnExistingParentFieldStillConflicts() {
        var fields = new HashMap<String, Object>(parent().fields()); fields.put("customer_id", null);
        var fixture = new Fixture(true, 8);
        var absent = fixture.read(row(fields));
        assertThat(absent.view().row().fields()).doesNotContainKey("customer");
        assertThat(absent.evidence().reads()).isEmpty(); assertThat(fixture.requested).isEmpty();
        fields.put("customer", Map.of("legacy", 1));
        var conflicting = fixture.read(row(fields));
        assertThat(conflicting.view().row().fields()).containsEntry("customer", Map.of("legacy", 1));
        assertThat(conflicting.view().unavailablePaths()).containsExactly(List.of("customer"));
        assertThat(conflicting.evidence().issues()).singleElement().satisfies(issue ->
                assertThat(issue.finding().kind()).isEqualTo(Kind.SOURCE_FIELD_CONFLICT));
        assertThat(fixture.requested).isEmpty();
    }

    @Test void missingReferenceZeroRowsAndDuplicateRowsRemainDistinctSourceFacts() {
        var missing = new Fixture(true, 8); var fields = new HashMap<String, Object>(parent().fields()); fields.remove("customer_id");
        var missingResult = missing.read(row(fields));
        assertThat(missingResult.evidence().issues()).extracting(issue -> issue.finding().kind()).containsExactly(Kind.SOURCE_FIELD_MISSING);
        assertThat(missing.requested).isEmpty();
        var empty = new Fixture(true, 8); empty.data.put("customer", List.of());
        var emptyResult = empty.read(parent());
        assertThat(emptyResult.evidence().issues()).extracting(issue -> issue.finding().kind()).containsExactly(Kind.SOURCE_REFERENCE_MISSING);
        assertThat(empty.requested).containsExactly(key(9));
        var duplicate = new Fixture(true, 8); duplicate.data.put("customer", List.of(customer(), customer()));
        var duplicateResult = duplicate.read(parent());
        assertThat(duplicateResult.evidence().issues()).extracting(issue -> issue.finding().kind()).containsExactly(Kind.SOURCE_DUPLICATE);
        assertThat(duplicate.requested).containsExactly(key(9));
        assertThat(duplicateResult.view().unavailablePaths()).containsExactly(List.of("customer"));
        assertThat(duplicateResult.view().row().fields()).doesNotContainKey("customer");
    }

    @Test void aLocalizedUnknownJoinFieldPropagatesItsOriginalCauseWithoutQuerying() {
        var fixture = new Fixture(true, 8); var original = parent(); var sample = sample("orders", original, "source");
        var field = new Location(ROOT.root(), List.of("customer_id"));
        var cause = new Finding(Kind.SOURCE_FIELD_INVALID, field);
        var evidence = new ValidationRootReader.SourceEvidence(ROOT, List.of(),
                List.of(new ValidationRootReader.SourceIssue(cause, Optional.of(sample), List.of(field))));
        var result = fixture.projection.read(ROOT, sample, view(original, List.of(List.of("customer_id"))), evidence, CLOCK, () -> {});
        assertThat(fixture.requested).isEmpty();
        assertThat(result.view().unavailablePaths()).containsExactlyInAnyOrder(List.of("customer_id"), List.of("customer"));
        assertThat(result.evidence().issues()).singleElement().satisfies(issue -> {
            assertThat(issue.finding()).isEqualTo(cause);
            assertThat(issue.affected()).extracting(Location::fieldPath).containsExactlyInAnyOrder(List.of("customer_id"), List.of("customer"));
        });
    }

    @Test void queryBudgetAndFailedChildReadCannotPublishAPartialRootOrOpenTheTarget() {
        var limited = new Fixture(true, 1); var source = new Cursor(List.of(parent())); var target = new Cursor(List.of(parent()));
        assertThatThrownBy(() -> rootRead(limited, source, target)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(limited.requested).hasSize(1); assertThat(limited.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
        assertThat(source.closed).isTrue(); assertThat(target.opens).isZero();
        var failed = new Fixture(true, 8); failed.failAlias = "region";
        var otherTarget = new Cursor(List.of(parent()));
        assertThatThrownBy(() -> rootRead(failed, new Cursor(List.of(parent())), otherTarget)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.READ_FAILED));
        assertThat(otherTarget.opens).isZero(); assertThat(failed.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
    }

    @Test void sharingAWiderRootLedgerCannotRaiseAnObjectComponentsLocalQueryLimit() {
        var fixture = new Fixture(true, 1); var original = parent();
        var shared = new ValidationNestBudget(ROOT, "source", new ValidationNestBudget.Limits(8 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 8), "tree");
        assertThatThrownBy(() -> fixture.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.args()).containsEntry("limit", "objectProjection.queries"));
        assertThat(fixture.requested).hasSize(1);
        assertThat(fixture.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
    }

    @Test void localBytesRowsAndOperationsStillApplyAndPoisonTheSharedRoot() {
        for (String dimension : List.of("bytes", "rows", "operations")) {
            var fixture = new Fixture(true, 8); var original = parent();
            var shared = new ValidationNestBudget(ROOT, "source", new ValidationNestBudget.Limits(8 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 8), "tree");
            var local = new ValidationNestObjectProjection.Limits(ROW, dimension.equals("bytes") ? 1 : 8 * 1024 * 1024,
                    dimension.equals("operations") ? 1 : 64 * 1024 * 1024, 8, dimension.equals("rows") ? 1 : 100, 8);
            var projection = new ValidationNestObjectProjection(new ValidationNestObjectProjection.Scope(RUN, "mapping", "output"),
                    fixture.plan, keys(), fixture.bindings, local);
            var failure = catchThrowable(() -> projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                    ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared));
            assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.args()).containsEntry("limit", "objectProjection." + dimension));
            assertThat(catchThrowable(shared::reads)).isSameAs(failure);
            assertThat(fixture.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
            var later = new Fixture(false, 8);
            assertThat(catchThrowable(() -> later.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                    ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared))).isSameAs(failure);
            assertThat(later.requested).isEmpty();
        }
    }

    @Test void sharingKeepsEachComponentsReadEvidenceLocalAndCallsEachQueryFactoryOnce() {
        var original = parent();
        var shared = new ValidationNestBudget(ROOT, "source", new ValidationNestBudget.Limits(8 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 8), "tree");
        var first = new Fixture(false, 8); first.forcedReadId = "first-read";
        var second = new Fixture(false, 8); second.forcedReadId = "second-read";
        for (var fixture : List.of(first, second)) {
            var result = fixture.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                    ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared);
            assertThat(result.evidence().reads()).extracting(ValidationCheckEvent.ReadBoundary::readId).containsExactly(fixture.forcedReadId);
            assertThat(fixture.requested).hasSize(1);
        }
        assertThat(shared.reads()).extracting(ValidationCheckEvent.ReadBoundary::readId).containsExactly("first-read", "second-read");
    }

    @Test void aNarrowSharedBudgetStillGatesWiderObjectLimitsBeforeAnotherQueryFactory() {
        var fixture = new Fixture(true, 8); var original = parent();
        var shared = new ValidationNestBudget(ROOT, "source", new ValidationNestBudget.Limits(8 * 1024 * 1024, 64 * 1024 * 1024, 1, 100, 8), "tree");
        assertThatThrownBy(() -> fixture.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "tree.queries"));
        assertThat(fixture.requested).hasSize(1);
        assertThat(fixture.cursors).allSatisfy(cursor -> assertThat(cursor.closed).isTrue());
    }

    @Test void sharedDepthIsAdmittedBeforeOpeningTheChildQuery() {
        var fixture = new Fixture(true, 8); var original = parent();
        var shared = new ValidationNestBudget(ROOT, "source", new ValidationNestBudget.Limits(8 * 1024 * 1024, 64 * 1024 * 1024, 8, 100, 1), "tree");
        assertThatThrownBy(() -> fixture.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {}, shared)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("limit", "tree.depth"));
        assertThat(fixture.requested).isEmpty();
        assertThat(fixture.cursors).isEmpty();
    }

    @Test void oldScopeChangedRootKeysAndUnsupportedShapesAreRefusedBeforeChildQueries() {
        var fixture = new Fixture(true, 8); var original = parent();
        var wrong = new ValidationObservation(RUN, new ValidationRoot("mapping", "output", key(8), List.of()), 1);
        assertThatThrownBy(() -> fixture.projection.read(wrong, sample("orders", original, "source"), view(original, List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {})).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> fixture.projection.read(ROOT, sample("orders", original, "source"), view(original, List.of(List.of("id"))),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {})).isInstanceOf(TapstateException.class);
        var array = new Embed("customer", Map.of("id", "customer_id"), EmbedAs.ARRAY, "customer", List.of("id"), null, null, null, List.of());
        var plan = ValidationNestRelations.compile("nest", new NestRoot("orders", List.of("id"), null, null, List.of(array)),
                Set.of("orders", "customer"), Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
        assertThatThrownBy(() -> new ValidationNestObjectProjection(new ValidationNestObjectProjection.Scope(RUN, "mapping", "output"), plan,
                keys(), Map.of(), limits(8))).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        assertThat(fixture.requested).isEmpty();
    }

    @Test void relabelingTheObservationCannotAdmitAChangedProjectedRootIdentity() {
        var fixture = new Fixture(true, 8); var original = parent();
        var fields = new HashMap<String, Object>(original.fields()); fields.put("id", 8);
        var relabeled = new ValidationObservation(RUN, new ValidationRoot("mapping", "output", key(8), List.of()), 1);
        assertThatThrownBy(() -> fixture.projection.read(relabeled, sample("orders", original, "source"), view(row(fields), List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {})).isInstanceOf(TapstateException.class);
        assertThat(fixture.requested).isEmpty();
    }

    @Test void aChildReadCannotReuseThePendingOriginalParentReadId() {
        var fixture = new Fixture(false, 8); fixture.forcedReadId = "source";
        var target = new Cursor(List.of(parent()));
        assertThatThrownBy(() -> rootRead(fixture, new Cursor(List.of(parent())), target)).isInstanceOf(TapstateException.class);
        assertThat(target.opens).isZero();
        assertThat(fixture.cursors).singleElement().satisfies(cursor -> assertThat(cursor.opens).isZero());
    }

    @Test void transformedChildRetentionIsChargedBeforeTheReaderRequestsEof() {
        var fixture = new Fixture(false, 8, 450000); fixture.workingPayload = "x".repeat(100000);
        assertThatThrownBy(() -> fixture.read(parent())).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(fixture.cursors).singleElement().satisfies(cursor -> {
            assertThat(cursor.nextCalls).isEqualTo(1);
            assertThat(cursor.released).isEqualTo(1); assertThat(cursor.closed).isTrue();
        });
    }

    @Test void physicalIdentityAndAnExplicitlySelectedComparisonIdentityMayUseDifferentColumns() {
        var fixture = new Fixture(true, 8);
        var physical = new ValidationKeyBinding("physical", List.of(component("id")), Set.of(), ROW);
        var comparison = new ValidationKeyBinding("comparison", List.of(component("code")), Set.of(), ROW);
        var projection = new ValidationNestObjectProjection(new ValidationNestObjectProjection.Scope(RUN, "mapping", "output"),
                fixture.plan, new ValidationNestObjectProjection.RootKeys(physical, comparison, comparison), fixture.bindings, limits(8));
        var fields = new HashMap<String, Object>(parent().fields()); fields.put("code", 99);
        var original = row(fields);
        var selected = new ValidationObservation(RUN, new ValidationRoot("mapping", "output", key(99), List.of()), 1);
        var result = projection.read(selected, sample("orders", original, "source"), view(original, List.of()),
                ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {});
        assertThat(result.view().row().fields()).containsEntry("id", 7).containsEntry("code", 99);
        assertThat(fixture.requested).containsExactly(key(9), key(3));
    }

    @Test void aDuplicateObjectDoesNotPreventAnIndependentSiblingFromBeingReadAndCompared() {
        var fixture = new Fixture(true, 8, 8 * 1024 * 1024, true);
        fixture.data.put("customer", List.of(customer(), customer()));
        var fields = new HashMap<String, Object>(parent().fields()); fields.put("shipping_id", 5);
        var actual = new HashMap<>(fields); actual.put("shipping", Map.of("id", 5, "label", "wrong"));
        var result = rootRead(fixture, new Cursor(List.of(row(fields))), new Cursor(List.of(row(actual))));
        assertThat(fixture.requested).containsExactly(key(9), key(5));
        assertThat(result.counts()).containsExactlyInAnyOrderEntriesOf(Map.of(Kind.SOURCE_DUPLICATE, 1L, Kind.TARGET_VALUE, 1L));
        assertThat(result.findings().keySet()).filteredOn(finding -> finding.kind() == Kind.TARGET_VALUE).singleElement()
                .satisfies(finding -> assertThat(finding.location().fieldPath()).containsExactly("shipping", "label"));
    }

    @Test void aRowOutsideTheRequestedPhysicalRelationCannotBecomeAnAgreedExpectedObject() {
        var fixture = new Fixture(false, 8);
        var wrongChild = row(Map.of("id", 999, "region_id", 3, "name", "Ada"));
        fixture.data.put("customer", List.of(wrongChild));
        var actual = new HashMap<String, Object>(parent().fields()); actual.put("customer", wrongChild.fields());
        var target = new Cursor(List.of(row(actual)));
        assertThatThrownBy(() -> rootRead(fixture, new Cursor(List.of(parent())), target)).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(ValidationError.READ_PROTOCOL_VIOLATION));
        assertThat(target.opens).isZero();
        assertThat(fixture.cursors).singleElement().satisfies(cursor -> {
            assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(1);
        });
    }

    private static ValidationRootCheck rootRead(Fixture fixture, Cursor source, Cursor target) {
        return ValidationRootReader.read(ROOT, ValidationMethod.FULL_COLUMN,
                new ValidationRootReader.Query("source", "v1", source::open), new ValidationRootReader.Query("target", "v1", target::open),
                new ValidationRelationRows.Limits(262144, ROW), new ValidationRootCheck.Limits(1048576, 100, 100, 20, 8), CLOCK, () -> {},
                original -> {
                    var projected = fixture.read(original);
                    return new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                            projected.view().row(), Optional.of(identity("orders", 7))), List.of(), projected.evidence());
                }, actual -> new ValidationRootReader.Target(actual, identity("target", 7)),
                (pair, root) -> ValidationFieldCheck.checkPartial(ROOT, List.of(), pair.source().working(), pair.target().working(),
                        pair.sourceSample(), pair.targetSample(), Optional.empty(), String::equals,
                        new ValidationValueComparison.TraversalLimits(ROW, 100000), pair.sourceEvidence().issues().stream()
                                .flatMap(issue -> issue.affected().stream()).map(Location::fieldPath).distinct().toList(), root));
    }
    private static final class Fixture {
        final Map<String, List<ValidationValues.Row>> data = new HashMap<>();
        final List<ValidationKey> requested = new ArrayList<>(); final List<Cursor> cursors = new ArrayList<>();
        final ValidationNestObjectProjection projection;
        final ValidationNestRelations.Plan plan;
        final Map<List<String>, ValidationNestObjectProjection.Binding> bindings = new LinkedHashMap<>();
        String failAlias;
        String forcedReadId;
        String workingPayload;
        Fixture(boolean nested, int queryLimit) { this(nested, queryLimit, 8 * 1024 * 1024); }
        Fixture(boolean nested, int queryLimit, long bytes) { this(nested, queryLimit, bytes, false); }
        Fixture(boolean nested, int queryLimit, long bytes, boolean sibling) {
            data.put("customer", List.of(customer())); data.put("region", List.of(row(Map.of("id", 3, "label", "north"))));
            var region = new Embed("region", Map.of("id", "region_id"), EmbedAs.OBJECT, "region", List.of("id"), null, null, null, List.of());
            var customer = new Embed("customer", Map.of("id", "customer_id"), EmbedAs.OBJECT, "customer", List.of("id"), null, null, null,
                    nested ? List.of(region) : List.of());
            var shipping = new Embed("shipping", Map.of("id", "shipping_id"), EmbedAs.OBJECT, "shipping", List.of("id"), null, null, null, List.of());
            data.put("shipping", List.of(row(Map.of("id", 5, "label", "ready"))));
            plan = ValidationNestRelations.compile("nest", new NestRoot("orders", List.of("id"), null, null,
                    sibling ? List.of(customer, shipping) : List.of(customer)), Set.of("orders", "customer", "region", "shipping"),
                    Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
            bind(plan.children(), bindings);
            projection = new ValidationNestObjectProjection(new ValidationNestObjectProjection.Scope(RUN, "mapping", "output"), plan, keys(), bindings, new ValidationNestObjectProjection.Limits(ROW, bytes, 64 * 1024 * 1024, queryLimit, 100, 8));
        }
        void bind(List<ValidationNestRelations.Relation> relations, Map<List<String>, ValidationNestObjectProjection.Binding> bindings) {
            for (var relation : relations) {
                var lookup = new ValidationNestLookupBinding("nest", relation, relation.join().stream().map(pair -> component(pair.parent())).toList(),
                        new ValidationKeyBinding("physical", relation.join().stream().map(pair -> component(pair.child())).toList(), Set.of(), ROW), ROW);
                bindings.put(relation.treePath(), new ValidationNestObjectProjection.Binding(lookup, request -> {
                    requested.add(request.key()); var cursor = new Cursor(data.get(relation.alias())); cursors.add(cursor);
                    if (relation.alias().equals(failAlias)) cursor.failure = new TapstateException(ValidationError.READ_FAILED, Map.of("table", relation.alias()), null);
                    return new ValidationRootReader.Query(forcedReadId == null ? relation.alias() + "-" + requested.size() : forcedReadId, "v1", cursor::open);
                }, (context, original) -> new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                        workingPayload != null && relation.alias().equals("customer") ? row(Map.of("id", original.fields().get("id"), "payload", workingPayload)) : original,
                        Optional.of(identity(relation.alias(), ((Number) original.fields().get("id")).longValue()))), List.of())));
                bind(relation.children(), bindings);
            }
        }
        ValidationNestObjectProjection.Result read(ValidationValues.Row original) {
            return projection.read(ROOT, sample("orders", original, "source"), view(original, List.of()),
                    ValidationRootReader.SourceEvidence.empty(), CLOCK, () -> {});
        }
    }
    private static ValidationNestObjectProjection.Limits limits(int queries) {
        return new ValidationNestObjectProjection.Limits(ROW, 8 * 1024 * 1024, 64 * 1024 * 1024, queries, 100, 8);
    }
    private static ValidationNestObjectProjection.RootKeys keys() {
        var key = new ValidationKeyBinding("root", List.of(component("id")), Set.of(), ROW);
        return new ValidationNestObjectProjection.RootKeys(key, key);
    }
    private static ValidationKeyBinding.Component component(String field) { return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")); }
    private static ValidationPhysicalIdentity identity(String alias, long value) { return new ValidationPhysicalIdentity(alias, "db", null, alias, List.of(List.of("id")), key(value)); }
    private static SampledSide sample(String alias, ValidationValues.Row row, String readId) { return new SampledSide(identity(alias, ((Number) row.fields().get("id")).longValue()), row, readId); }
    private static ValidationKey key(long value) { return new ValidationKey(List.of(ValidationKey.Part.number(value))); }
    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, ROW); }
    private static ValidationValues.Row parent() { return row(Map.of("id", 7, "customer_id", 9, "known", 1)); }
    private static ValidationValues.Row customer() { return row(Map.of("id", 9, "region_id", 3, "name", "Ada")); }
    private static ValidationStaticMap.Projection view(ValidationValues.Row row, List<List<Object>> masks) {
        return ValidationStaticMap.compile("identity", new TransformBody.MapProjection(Map.of()), ROW).apply(row, masks);
    }
    private static final class Cursor implements ValidationRowCursor {
        final List<ValidationValues.Row> rows; int next; int released; int opens; int nextCalls; boolean closed; RuntimeException failure;
        Cursor(List<ValidationValues.Row> rows) { this.rows = rows; }
        ValidationRowCursor open() { opens++; return this; }
        @Override public Optional<Row> next() {
            nextCalls++;
            if (closed || next != released) throw new IllegalStateException("cursor contract");
            if (failure != null) throw failure;
            if (next == rows.size()) return Optional.empty();
            int ordinal = next++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return rows.get(ordinal); }
                @Override public void close() { released++; }
            });
        }
        @Override public void close() { closed = true; }
    }
}
