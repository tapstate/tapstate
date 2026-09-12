package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The config map a connector is handed carries the types the connector's own connection form declares,
 * whatever spelling the workspace wrote. A workspace generated from the bundled catalog spells every
 * value as text — {@code port: "3306"} — while a connector's config bean holds a {@code Number} and
 * casts to it, so an uncoerced map reaches the connector as a {@code ClassCastException} thrown out of
 * its own code. The declaration coerced from is the connector's, never the catalog's: the catalog is
 * the side that is wrong here.
 *
 * <p>The seam under test is {@link PdkConnector#open}, the one place every connector-facing config map
 * is built — discovery, the connection test, the data browser and both runtime ports all reach the
 * connector through it — so proving it here proves all of them.
 */
class ConfigTypeCoercionTest {

    /**
     * A connection form in the shape connectors actually ship. {@code port} is the shape at issue: an
     * upstream numeric field carries the Formily schema type {@code string} and declares its numeric
     * nature through the {@code InputNumber} component it renders with, so the component is the
     * connector's own numeric declaration. {@code ssl} is a boolean, which upstream does type honestly.
     * {@code host} is text, and {@code timeout} shows the minority spelling of a declared number.
     */
    private static final String SPEC = """
            {
              "properties": {"id": "demo"},
              "configOptions": {
                "connection": {
                  "type": "object",
                  "properties": {
                    "host": {"type": "string", "x-component": "Input"},
                    "port": {"type": "string", "x-component": "InputNumber", "default": 3306},
                    "ssl": {"type": "boolean", "x-component": "Switch"},
                    "OPTIONAL_FIELDS": {
                      "type": "void",
                      "properties": {
                        "timeout": {"type": "integer", "x-component": "InputNumber"}
                      }
                    }
                  }
                }
              }
            }
            """;

    @Test
    void aDeclaredNumberSpelledAsTextReachesTheConnectorAsANumber(@TempDir Path dir) {
        Object port = open(dir, "port", "3306").get("port");

        // The generated workspace's spelling. The connector casts what it finds to Number, so text here
        // is the ClassCastException the operator sees instead of a connection.
        assertThat(port).isInstanceOf(Number.class);
        assertThat(((Number) port).intValue()).isEqualTo(3306);
    }

    @Test
    void aDeclaredNumberAlreadyANumberIsHandedOnUntouched(@TempDir Path dir) {
        // A hand-written workspace writes the YAML integer, which is already what the connector wants.
        // Coercion must leave it alone rather than round-trip it through text.
        assertThat(open(dir, "port", 3306).get("port")).isEqualTo(3306);
    }

    @Test
    void aDeclaredNumberNestedUnderAFormContainerIsCoercedToo(@TempDir Path dir) {
        // A connection form groups optional fields under a container node; the connector still reads
        // them by their leaf name, so the declaration has to be found through the container.
        Object timeout = open(dir, "timeout", "30").get("timeout");

        assertThat(timeout).isInstanceOf(Number.class);
        assertThat(((Number) timeout).intValue()).isEqualTo(30);
    }

    @Test
    void aDeclaredNumberThatIsNotOneIsACodedRefusalNamingTheField(@TempDir Path dir) {
        // The value cannot be converted, so the connector would cast and crash. That has to surface as a
        // diagnosable refusal naming the field, its declared type and the value — never as a bare cast.
        TapstateException thrown = catchThrowableOfType(
                () -> open(dir, "port", "not-a-number"), TapstateException.class);

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("connector.config-type-mismatch");
        assertThat(thrown.args())
                .containsEntry("connector", "demo")
                .containsEntry("field", "port")
                .containsEntry("expected", "number")
                .containsEntry("value", "not-a-number");
    }

    @Test
    void aDeclaredNumberOutsideTheFiniteRangeIsACodedRefusal(@TempDir Path dir) {
        TapstateException thrown = catchThrowableOfType(
                () -> open(dir, "port", "1e309"), TapstateException.class);

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("connector.config-type-mismatch");
        assertThat(thrown.args()).containsEntry("field", "port").containsEntry("value", "1e309");
    }

    @Test
    void aDeclaredBooleanSpelledAsTextReachesTheConnectorAsABoolean(@TempDir Path dir) {
        // The same defect in the shape that surfaces later: a string boolean passes the connection test
        // and only fails at write time, after the pipeline has already reported itself running.
        assertThat(open(dir, "ssl", "true").get("ssl")).isEqualTo(Boolean.TRUE);
        assertThat(open(dir, "ssl", "false").get("ssl")).isEqualTo(Boolean.FALSE);
    }

    @Test
    void aDeclaredBooleanThatIsNotOneIsACodedRefusal(@TempDir Path dir) {
        TapstateException thrown = catchThrowableOfType(
                () -> open(dir, "ssl", "yes"), TapstateException.class);

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("connector.config-type-mismatch");
        assertThat(thrown.args()).containsEntry("field", "ssl").containsEntry("expected", "boolean");
    }

    @Test
    void aDeclaredStringThatLooksNumericStaysAString(@TempDir Path dir) {
        // The regression a blunt "convert anything numeric-looking" fix would cause: a database name or
        // a user may be all digits, and the connector reads those as text.
        assertThat(open(dir, "host", "3306").get("host")).isEqualTo("3306");
    }

    @Test
    void anUndeclaredFieldIsHandedOnUntouched(@TempDir Path dir) {
        // A field the connection form does not declare has no declared type to coerce to, so the value
        // reaches the connector exactly as the workspace wrote it.
        assertThat(open(dir, "extra", "42").get("extra")).isEqualTo("42");
    }

    @Test
    void aConnectorWithNoSpecHandsEveryValueOnUntouched(@TempDir Path dir) {
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of("port", "3306"))) {
            // Nothing declares a type, so nothing is coerced — the synthetic-connector paths that carry
            // no spec keep behaving exactly as they did.
            assertThat(connector.context().getConnectionConfig().get("port")).isEqualTo("3306");
        }
    }

    /** Opens a synthetic connector carrying {@link #SPEC} and returns the config map it was handed. */
    private static Map<String, Object> open(Path dir, String field, Object value) {
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, SPEC);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put(field, value);
        try (PdkConnector connector = PdkConnector.open("demo", ref, settings)) {
            return new LinkedHashMap<>(connector.context().getConnectionConfig());
        }
    }
}
