package io.tapstate.runtime.engine.validation;

import static org.assertj.core.api.Assertions.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationRowCursor;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ValidationNestRelationReadTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(8192, 100, 16);
    private static final ValidationRelationRows.Limits LIMITS = new ValidationRelationRows.Limits(65536, ROW);
    private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

    @Test void onlyReadyKeysBuildThePhysicalQueryAndEveryRowReachesNormalEof() {
        var key = new BigInteger("9007199254740993"); var cursor = new Cursor(7); var built = new AtomicInteger();
        var binding = binding(); var observation = observation();
        var result = read(binding, view(Map.of("customer", key), List.of()), request -> {
            built.incrementAndGet();
            assertThat(request.parent()).isEqualTo(observation);
            assertThat(request.physical()).isSameAs(binding.physical());
            assertThat(request.physical().components().getFirst().path()).containsExactly("native_customer_id");
            assertThat(request.key()).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(key))));
            return new ValidationRootReader.Query("customer-query", "original-model", () -> cursor);
        }, () -> {});
        assertThat(built).hasValue(1); assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isEqualTo(7);
        var rows = result.rows().orElseThrow();
        assertThat(rows.included()).isEqualTo(7); assertThat(rows.includedSamples()).hasSize(2);
        assertThat(rows.atLeastTwo()).isTrue(); assertThat(rows.unique()).isEmpty();
        assertThat(rows.read().readId()).isEqualTo("customer-query");
    }

    @Test void nullMissingAndUnknownReferencesNeverOpenOrFabricateAnEmptyRead() {
        var absent = new HashMap<String, Object>(); absent.put("customer", null);
        var queries = new AtomicInteger();
        var views = List.of(view(absent, List.of()), view(Map.of(), List.of()), view(Map.of("customer", 7), List.of(List.of("customer"))));
        var kinds = List.of(ValidationNestLookupBinding.NoReference.class, ValidationNestLookupBinding.Invalid.class,
                ValidationNestLookupBinding.Unresolved.class);
        for (int i = 0; i < views.size(); i++) {
            var result = read(binding(), views.get(i), request -> { queries.incrementAndGet(); throw new AssertionError("must not query"); }, () -> {});
            assertThat(result.preparation()).isInstanceOf(kinds.get(i));
            assertThat(result.rows()).isEmpty(); assertThat(result.parent()).isEqualTo(observation());
        }
        assertThat(queries).hasValue(0);
    }

    @Test void actualEmptyQueryIsDistinctFromNoReferenceAndQueryFailure() {
        var empty = new Cursor(0);
        var result = read(binding(), view(Map.of("customer", 7), List.of()), request -> query(empty), () -> {});
        assertThat(result.preparation()).isInstanceOf(ValidationNestLookupBinding.Ready.class);
        assertThat(result.rows().orElseThrow().definitelyEmpty()).isTrue(); assertThat(empty.closed).isTrue();
        var failed = new Cursor(0); failed.failure = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "customers"), null);
        assertThat(catchThrowable(() -> read(binding(), view(Map.of("customer", 7), List.of()), request -> query(failed), () -> {})))
                .isSameAs(failed.failure);
        assertThat(failed.closed).isTrue();
    }

    @Test void authorityLostDuringQueryPreparationPreventsOpeningAndLossInsideReadStillClosesIt() {
        var authorized = new AtomicBoolean(true); var opens = new AtomicInteger();
        var lost = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
        Runnable guard = () -> { if (!authorized.get()) throw lost; };
        assertThat(catchThrowable(() -> read(binding(), view(Map.of("customer", 7), List.of()), request -> {
            authorized.set(false);
            return new ValidationRootReader.Query("child", "version", () -> { opens.incrementAndGet(); return new Cursor(1); });
        }, guard))).isSameAs(lost);
        assertThat(opens).hasValue(0);
        authorized.set(true); var cursor = new Cursor(1);
        assertThat(catchThrowable(() -> read(binding(), view(Map.of("customer", 7), List.of()), request ->
                new ValidationRootReader.Query("child", "version", () -> { authorized.set(false); return cursor; }), guard))).isSameAs(lost);
        assertThat(cursor.closed).isTrue(); assertThat(cursor.released).isZero();
    }

    private static ValidationNestRelationRead.Result read(ValidationNestLookupBinding binding, ValidationStaticMap.Projection view,
            ValidationNestRelationRead.Queries queries, Runnable guard) {
        return ValidationNestRelationRead.read(observation(), binding, view, queries, LIMITS, CLOCK, guard,
                row -> new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED, row, Optional.empty()));
    }
    private static ValidationRootReader.Query query(Cursor cursor) { return new ValidationRootReader.Query("child", "version", () -> cursor); }
    private static ValidationStaticMap.Projection view(Map<String, ?> fields, List<List<Object>> unavailable) {
        return ValidationStaticMap.compile("parent", new TransformBody.MapProjection(Map.of()), ROW)
                .apply(ValidationValues.ownRow(fields, ROW), unavailable);
    }
    private static ValidationNestLookupBinding binding() {
        var embed = new Embed("customer", Map.of("id", "customer"), EmbedAs.OBJECT, "customer_doc", List.of("id"), null, null, null, List.of());
        var plan = ValidationNestRelations.compile("nest", new NestRoot("order", List.of("id"), null, null, List.of(embed)),
                Set.of("order", "customer"), Map.of(), new ValidationNestRelations.Limits(8, 8, 65536));
        var physical = new ValidationKeyBinding("physical", List.of(component("native_customer_id")), Set.of(), ROW);
        return new ValidationNestLookupBinding("nest", plan.children().getFirst(), List.of(component("customer")), physical, ROW);
    }
    private static ValidationKeyBinding.Component component(String field) {
        return new ValidationKeyBinding.Component(List.of(field), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"));
    }
    private static ValidationObservation observation() {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(102))), List.of()), 1);
    }
    private static final class Cursor implements ValidationRowCursor {
        final int count; int delivered; int released; boolean closed; RuntimeException failure;
        Cursor(int count) { this.count = count; }
        @Override public Optional<Row> next() {
            if (closed || released != delivered) throw new IllegalStateException("unreleased row");
            if (failure != null) throw failure;
            if (delivered == count) return Optional.empty();
            int ordinal = delivered++;
            return Optional.of(new Row() {
                @Override public long ordinal() { return ordinal; }
                @Override public ValidationValues.Row values() { return ValidationValues.ownRow(Map.of("id", ordinal), ROW); }
                @Override public void close() { released++; }
            });
        }
        @Override public void close() { closed = true; }
    }
}
