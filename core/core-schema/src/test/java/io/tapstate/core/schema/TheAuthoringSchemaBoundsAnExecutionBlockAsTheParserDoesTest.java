package io.tapstate.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.core.model.ExecutionSpec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The authoring schema states the bounds of a node's execution block that the parser enforces, so an editor
 * validating a document against it refuses what the server would refuse instead of accepting it: a width from
 * one to the most an author may ask for, the rows of a batch from one to their limit, and a wait spelled as a
 * whole number and a unit, no longer than the longest allowed.
 *
 * <p>The wait is the one a schema can state only as a pattern, so the pattern is held to the parser spelling by
 * spelling rather than read for plausibility: every wait it accepts the parser accepts, and the other way round.
 */
class TheAuthoringSchemaBoundsAnExecutionBlockAsTheParserDoesTest {

    @Test
    void theWidthAndTheRowsOfABatchAreBoundedAsTheParserBoundsThem() {
        Map<?, ?> parallelism = property("ExecutionSpec", "parallelism");
        Map<?, ?> maxRecords = property("BatchSpec", "max_records");

        assertThat(number(parallelism, "minimum")).isEqualTo(1L);
        assertThat(number(parallelism, "maximum")).isEqualTo((long) ExecutionSpec.MAX_PARALLELISM);
        assertThat(number(maxRecords, "minimum")).isEqualTo(1L);
        assertThat(number(maxRecords, "maximum")).isEqualTo((long) BatchSpec.MAX_RECORDS_LIMIT);
    }

    @Test
    void aWaitIsAcceptedByTheSchemaExactlyWhereTheParserAcceptsIt() {
        Object pattern = property("BatchSpec", "max_wait").get("pattern");
        assertThat(pattern).as("the wait's spelling and bound are stated").isInstanceOf(String.class);
        // A schema pattern matches anywhere in the value unless it anchors itself, so it is searched for, as a
        // validator would, rather than matched against the whole value.
        Pattern stated = Pattern.compile((String) pattern);

        List<String> spellings = new ArrayList<>(List.of("", "ms", "1", "00ms", "01s", "007m", "1h", "-1s", "1.5s",
                " 1s", "1s ", "1S", "1MS", "9999999999m", "10000000000ms", "61s", "2m", "60001ms"));
        for (int millis = 0; millis <= 70_000; millis++) {
            spellings.add(millis + "ms");
        }
        for (int seconds = 0; seconds <= 100; seconds++) {
            spellings.add(seconds + "s");
        }
        for (int minutes = 0; minutes <= 5; minutes++) {
            spellings.add(minutes + "m");
        }

        List<String> disagreeing = new ArrayList<>();
        for (String spelling : spellings) {
            long millis = BatchSpec.durationMillis(spelling);
            boolean parsed = millis >= 0 && millis <= BatchSpec.MAX_WAIT_LIMIT_MILLIS;
            if (stated.matcher(spelling).find() != parsed) {
                disagreeing.add(spelling);
            }
        }
        assertThat(disagreeing).as("spellings the schema and the parser answer differently, of %s", spellings.size())
                .isEmpty();
    }

    private static Map<?, ?> property(String definition, String name) {
        Map<?, ?> root = (Map<?, ?>) JsonReader.parse(TapstateSchema.json());
        Map<?, ?> defs = (Map<?, ?>) root.get("$defs");
        Map<?, ?> properties = (Map<?, ?>) ((Map<?, ?>) defs.get(definition)).get("properties");
        return (Map<?, ?>) properties.get(name);
    }

    private static Long number(Map<?, ?> schema, String keyword) {
        Object value = schema.get(keyword);
        return value instanceof Number number ? number.longValue() : null;
    }
}
