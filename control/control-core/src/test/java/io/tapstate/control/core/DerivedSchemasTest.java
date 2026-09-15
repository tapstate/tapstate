package io.tapstate.control.core;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class DerivedSchemasTest {

    @ParameterizedTest
    @CsvSource(value = {
            "NULL, STRING NULL, true",
            "STRING NULL, NULL, true",
            "INT64 NULL, DECIMAL NULL, true",
            "STRING NULL, STRING NULL, false",
            "NULL, NULL, false"
    }, nullValues = "NULL")
    void reportsAddedRemovedAndRetypedColumnsAsDrift(String recorded, String derived, boolean drifted) {
        assertThat(new DerivedSchemas.ColumnReport("region", recorded, derived, null).drifted())
                .isEqualTo(drifted);
    }
}
