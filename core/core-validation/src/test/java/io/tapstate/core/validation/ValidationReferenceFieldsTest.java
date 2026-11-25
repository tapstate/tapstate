package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationReferenceFields.*;
import static org.assertj.core.api.Assertions.*;

class ValidationReferenceFieldsTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(64 * 1024, 1024, 32);

    @Test void nullIsNoReferenceAndMissingIsSourceFieldProblem() {
        var fields = new ValidationReferenceFields(List.of("customer_id"), LIMITS);
        var nullValue = new HashMap<String, Object>();
        nullValue.put("customer_id", null);
        assertThat(fields.read(row(nullValue))).isEqualTo(NoReference.INSTANCE);
        assertThat(fields.read(row(Map.of()))).isEqualTo(new Missing(List.of("customer_id")));
        assertThat(((Complete) fields.read(row(Map.of("customer_id", 19)))).components()).containsExactly(19);
    }

    @Test void compositeReferencesInspectEveryPresenceBeforeTreatingAnyNullAsNoReference() {
        var data = new HashMap<String, Object>();
        data.put("tenant", null);
        for (var fields : List.of(List.of("tenant", "customer"), List.of("customer", "tenant"))) {
            var extractor = new ValidationReferenceFields(fields, LIMITS);
            assertThat(extractor.read(row(data))).isEqualTo(new Missing(List.of("customer")));
        }
        data.put("customer", 7);
        assertThat(new ValidationReferenceFields(List.of("tenant", "customer"), LIMITS).read(row(data)))
                .isEqualTo(NoReference.INSTANCE);
        data.put("tenant", 11);
        assertThat(((Complete) new ValidationReferenceFields(List.of("customer", "tenant"), LIMITS).read(row(data)))
                .components()).containsExactly(7, 11);
    }

    @Test void typedNullIsAbsentButZeroFalseAndEmptyTextRemainActualReferences() {
        var extractor = new ValidationReferenceFields(List.of("ref"), LIMITS);
        var typedNull = new ValidationValues.Typed("number", "decimal", "pdk", null);
        assertThat(extractor.read(row(Map.of("ref", typedNull)))).isEqualTo(NoReference.INSTANCE);
        for (Object value : List.of(0, false, "")) {
            assertThat(((Complete) extractor.read(row(Map.of("ref", value)))).components()).containsExactly(value);
        }
    }

    @Test void namesAreLiteralColumnsAndComponentsKeepTheirOriginalTypeProvenance() {
        var value = new ValidationValues.Typed("number", "decimal", "custom-v1", "custom-codec", new java.math.BigDecimal("9007199254740993"));
        var fields = new ValidationReferenceFields(List.of("customer.id"), LIMITS);
        assertThat(fields.read(row(Map.of("customer", Map.of("id", 1)))))
                .isEqualTo(new Missing(List.of("customer.id")));
        var complete = (Complete) fields.read(row(Map.of("customer.id", value)));
        assertThat(complete.components()).containsExactly(value);
        assertThatThrownBy(() -> complete.components().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void declarationsAreOwnedBoundedAndRejectDuplicateColumns() {
        var names = new ArrayList<>(List.of("id"));
        var fields = new ValidationReferenceFields(names, LIMITS);
        names.clear();
        assertThat(fields.fields()).containsExactly("id");
        assertThatThrownBy(() -> new ValidationReferenceFields(List.of("id", "id"), LIMITS)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationReferenceFields(List.of(), LIMITS)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationReferenceFields(java.util.Collections.nCopies(33, "id"), LIMITS))
                .isInstanceOf(TapstateException.class);
    }

    @Test void resourceFailureCannotBecomeAbsentOrMissing() {
        var fields = new ValidationReferenceFields(List.of("missing"), new ValidationValues.Limits(1, 1, 1));
        assertThatThrownBy(() -> fields.read(row(Map.of("unrelated", "large"))))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    private static ValidationValues.Row row(Map<String, ?> fields) { return ValidationValues.ownRow(fields, LIMITS); }
}
