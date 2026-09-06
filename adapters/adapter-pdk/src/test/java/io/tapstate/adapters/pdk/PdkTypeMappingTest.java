package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapstate.core.common.TapstateType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Pins the mapping from a connector's own declared column type onto the tapstate type namespace.
 *
 * <p>The input is never hand-built: each case drives the real fill from a real connector's verbatim
 * {@code dataTypes} declaration, so the case also witnesses that the fill actually carries the
 * distinguishing attribute across. Asserting on a hand-built type would pass even if the fill stopped
 * setting it, which is exactly the way the mapping would silently go wrong.
 */
class PdkTypeMappingTest {

    /** A fresh directory per case: the synthetic connector's source is written into it. */
    @TempDir
    private Path dir;

    /** The mysql connector's own declaration for its decimal column type, verbatim from its spec. */
    private static final String DECIMAL_SPEC = """
            {"dataTypes": {"decimal[($precision,$scale)][unsigned]": {"to": "TapNumber",\
             "precision": [1, 65], "scale": [0, 30], "defaultPrecision": 10, "defaultScale": 0,\
             "unsigned": "unsigned", "fixed": true}}}""";

    @Test
    void aFixedPointColumnMapsOntoDecimal() {
        TapField amount = filled(DECIMAL_SPEC, "amount", "decimal(18,4)");

        assertThat(PdkTypeMapping.of(amount.getTapType()))
                .as("a decimal column carries a scale no binary floating point type holds losslessly")
                .isEqualTo(TapstateType.DECIMAL);
    }

    /** The mysql connector's own declaration for its widest integer column type, verbatim from its spec. */
    private static final String BIGINT_SPEC = """
            {"dataTypes": {"bigint[($zerofill)]": {"to": "TapNumber", "bit": 64, "precision": 19,\
             "value": [-9223372036854775808, 9223372036854775807]}}}""";

    @Test
    void anIntegerColumnMapsOntoInt64() {
        TapField id = filled(BIGINT_SPEC, "id", "bigint");

        assertThat(PdkTypeMapping.of(id.getTapType()))
                .as("an integer column holds no scale, so it maps losslessly onto the 64-bit integer")
                .isEqualTo(TapstateType.INT64);
    }

    /** The mysql connector's own declaration for its binary floating point column type, verbatim. */
    private static final String DOUBLE_SPEC = """
            {"dataTypes": {"double": {"to": "TapNumber", "precision": [1, 17], "preferPrecision": 11,\
             "preferScale": 4, "scale": [0, 17], "fixed": false}}}""";

    @Test
    void aBinaryFloatingPointColumnMapsOntoDouble() {
        TapField rate = filled(DOUBLE_SPEC, "rate", "double");

        assertThat(PdkTypeMapping.of(rate.getTapType()))
                .as("the source itself already holds this column as binary floating point, so nothing is lost")
                .isEqualTo(TapstateType.DOUBLE);
    }

    /**
     * A real connector's own declaration for a scaled numeric column, verbatim from its spec. It states
     * a scale and says nothing about {@code fixed} - which most connectors declaring a number do not, so
     * the absence is the ordinary case rather than a malformed one.
     */
    private static final String UNMARKED_SCALED_SPEC = """
            {"dataTypes": {"FLOAT": {"to": "TapNumber", "value": ["-3.4E38", "3.4E38"],\
             "scale": 2, "precision": 39}}}""";

    @Test
    void aScaledColumnTheConnectorDidNotMarkEitherWayMapsOntoUnknown() {
        // Reading the absence as "integer" is how a real decimal column comes out computable: the gate
        // then admits arithmetic over it, and the value that reaches the expression at run time is the
        // exact decimal the driver delivered - so the pipeline fails live, which is the state this
        // whole check exists to end. A declared scale is the column saying it is scaled; which scaled
        // kind it is nobody said, and unknown is what says so.
        TapField amount = filled(UNMARKED_SCALED_SPEC, "amount", "FLOAT");

        assertThat(PdkTypeMapping.of(amount.getTapType()))
                .as("a scale with no exact/approximate marking is unresolved, never a lossless integer")
                .isEqualTo(TapstateType.UNKNOWN);
    }

    /**
     * A connector's own declaration for a plain 64-bit integer, verbatim: it marks {@code scale} as
     * zero and says nothing about {@code fixed} either. Zero scale is a column stating it holds no
     * fractional part, which is the one thing that tells an unmarked integer from an unmarked decimal.
     */
    private static final String ZERO_SCALE_LONG_SPEC = """
            {"dataTypes": {"Long": {"to": "TapNumber", "scale": 0, "bit": 64,\
             "value": [-9223372036854775808, 9223372036854775807]}}}""";

    @Test
    void anIntegerColumnDeclaringAZeroScaleStillMapsOntoInt64() {
        TapField id = filled(ZERO_SCALE_LONG_SPEC, "id", "Long");

        assertThat(PdkTypeMapping.of(id.getTapType()))
                .as("a stated scale of zero is no scale, so the column stays the integer it is")
                .isEqualTo(TapstateType.INT64);
    }

    /**
     * A real connector's declaration for a binary floating point column, verbatim: it names the type
     * and says nothing else at all — no {@code fixed}, no {@code scale}, no width, no range. Its own
     * integer columns in the same spec do say something ({@code bit}), which is what tells them apart.
     */
    private static final String BARE_SPEC = """
            {"dataTypes": {"double": {"to": "TapNumber"}, "float": {"to": "TapNumber"},\
             "int": {"bit": 64, "to": "TapNumber"}, "long": {"bit": 64, "to": "TapNumber"}}}""";

    @Test
    void aNumberTheConnectorSaysNothingElseAboutMapsOntoUnknown() {
        // Reading silence as "integer" is how a real double column comes out computable: the gate then
        // admits int arithmetic over it, and what reaches the expression at run time is the Double the
        // driver delivered - so the pipeline fails live, which is the state this check exists to end.
        // A number is taken as an integer only where the connector said something integral about it.
        TapField rate = filled(BARE_SPEC, "rate", "double");

        assertThat(PdkTypeMapping.of(rate.getTapType()))
                .as("a number with nothing declared about it is unresolved, never a lossless integer")
                .isEqualTo(TapstateType.UNKNOWN);
    }

    @Test
    void anIntegerColumnDeclaringOnlyItsWidthStillMapsOntoInt64() {
        // The counterweight: the same connector's integer columns declare a width and nothing else,
        // and they must stay computable - a rule that refused every unmarked number would take these
        // with it, which is most integer columns across the connector set.
        TapField id = filled(BARE_SPEC, "id", "long");

        assertThat(PdkTypeMapping.of(id.getTapType()))
                .as("a declared width is the connector saying this column holds a whole number")
                .isEqualTo(TapstateType.INT64);
    }

    /**
     * A real connector's declaration for a 64-bit integer, verbatim: no {@code fixed}, no {@code scale},
     * no width - only the range of values it holds. Several connectors in the set describe their integer
     * columns this way and nothing else, so the range has to count as the connector describing a whole
     * number or their integer columns all become unresolved.
     */
    private static final String RANGE_ONLY_SPEC = """
            {"dataTypes": {"long": {"to": "TapNumber",\
             "value": [-9223372036854775808, 9223372036854775807]}}}""";

    @Test
    void anIntegerColumnDeclaringOnlyItsValueRangeStillMapsOntoInt64() {
        TapField id = filled(RANGE_ONLY_SPEC, "id", "long");

        assertThat(PdkTypeMapping.of(id.getTapType()))
                .as("a declared range of whole numbers is the connector describing a whole number")
                .isEqualTo(TapstateType.INT64);
    }

    /** The mysql connector's own declaration for its variable-length text column type, verbatim. */
    private static final String VARCHAR_SPEC = """
            {"dataTypes": {"varchar($byte)": {"name": "varchar", "to": "TapString", "byte": 16358,\
             "defaultByte": 1, "byteRatio": 3}}}""";

    @Test
    void aTextColumnMapsOntoString() {
        TapField customer = filled(VARCHAR_SPEC, "customer", "varchar(64)");

        assertThat(PdkTypeMapping.of(customer.getTapType()))
                .as("text is the one column shape row expressions already read without losing anything")
                .isEqualTo(TapstateType.STRING);
    }

    @Test
    void aColumnTheConnectorDeclaresNothingAboutMapsOntoUnknown() {
        // The connector declares a mapping for bigint only. A column of any other type is one its own
        // machinery cannot resolve, and what comes back is the framework's raw fallback - not an absent
        // type, which is what makes the fallback worth pinning here.
        TapField amount = filled(BIGINT_SPEC, "amount", "decimal(18,4)");
        assertThat(amount.getTapType()).as("an unresolvable column falls back to raw").isInstanceOf(TapRaw.class);

        assertThat(PdkTypeMapping.of(amount.getTapType()))
                .as("an unresolved column must be nameably unknown, never quietly one of the safe types")
                .isEqualTo(TapstateType.UNKNOWN);
    }

    @Test
    void aColumnWithNoResolvedTypeAtAllMapsOntoUnknown() {
        assertThat(PdkTypeMapping.of(null))
                .as("a connector declaring no mapping at all leaves the type absent, and absent is not safe")
                .isEqualTo(TapstateType.UNKNOWN);
    }

    /**
     * Every remaining column shape a real connector declares, each declaration verbatim from a connector's
     * own spec. Numbers get a case each above - their split is what this mapping exists for - while these
     * are one table because each is a plain projection with nothing to weigh. The binary entry is the one
     * that earns its place twice: it declares {@code fixed} as well, so it also witnesses that the exact /
     * approximate split stays scoped to numbers rather than firing on any type that declares the word.
     */
    static Stream<Arguments> declaredColumnShapes() {
        return Stream.of(
                arguments("date", """
                        {"dataTypes": {"date": {"to": "TapDate", "range": ["1000-01-01", "9999-12-31"],\
                         "pattern": "yyyy-MM-dd"}}}""", TapstateType.DATE),
                arguments("time", """
                        {"dataTypes": {"time[($fraction)]": {"to": "TapTime", "fraction": [0, 6],\
                         "defaultFraction": 0, "range": ["-838:59:59", "838:59:59"], "pattern": "HH:mm:ss"}}}""",
                        TapstateType.TIME),
                arguments("datetime", """
                        {"dataTypes": {"datetime[($fraction)]": {"to": "TapDateTime",\
                         "range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "pattern": "yyyy-MM-dd HH:mm:ss",\
                         "fraction": [0, 6], "defaultFraction": 0}}}""", TapstateType.DATETIME),
                arguments("year", """
                        {"dataTypes": {"year[($fraction)]": {"to": "TapYear", "range": ["1901", "2155"],\
                         "fraction": [0, 4], "defaultFraction": 4, "pattern": "yyyy"}}}""", TapstateType.YEAR),
                arguments("bit(1)", """
                        {"dataTypes": {"bit(1)": {"to": "TapBoolean", "bit": 1, "queryOnly": true}}}""",
                        TapstateType.BOOLEAN),
                arguments("binary(16)", """
                        {"dataTypes": {"binary[($byte)]": {"to": "TapBinary", "byte": 255, "defaultByte": 1,\
                         "fixed": true}}}""", TapstateType.BINARY),
                arguments("json", """
                        {"dataTypes": {"json": {"to": "TapJson", "byte": "4g", "pkEnablement": false}}}""",
                        TapstateType.JSON),
                arguments("OBJECT", """
                        {"dataTypes": {"OBJECT": {"to": "TapMap"}}}""", TapstateType.MAP),
                arguments("ARRAY", """
                        {"dataTypes": {"ARRAY": {"to": "TapArray"}}}""", TapstateType.ARRAY),
                arguments("NULL", """
                        {"dataTypes": {"NULL": {"to": "TapRaw"}}}""", TapstateType.UNKNOWN));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("declaredColumnShapes")
    void aDeclaredColumnShapeMapsOntoItsNamedType(String dataType, String spec, TapstateType expected) {
        TapField column = filled(spec, "value", dataType);

        assertThat(PdkTypeMapping.of(column.getTapType())).isEqualTo(expected);
    }

    @Test
    void aColumnWhoseDeclaredBoundsExceedADoubleStillResolvesToItsNamedType() {
        // Verbatim from a real connector's spec: a 128-bit decimal states bounds far past a double. Read
        // as doubles they become infinities, and the type's own parser gives up on the whole entry at the
        // "I" - so the column resolves to nothing while every column with ordinary bounds still resolves,
        // which reads as "that one type is unsupported" rather than as a parse fault.
        TapField column = filled("""
                {"dataTypes": {"DECIMAL128": {
                    "to": "TapNumber",
                    "value": [-1E+6145, 1E+6145],
                    "precision": [1, 65],
                    "scale": [0, 30],
                    "defaultPrecision": 30,
                    "defaultScale": 10,
                    "fixed": true
                }}}""", "amount", "DECIMAL128");

        assertThat(PdkTypeMapping.of(column.getTapType())).isNotEqualTo(TapstateType.UNKNOWN);
    }

    /** Discovers one column of the given database type through a connector declaring {@code spec}. */
    private TapField filled(String spec, String column, String dataType) {
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null, spec);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            TapTable table = new TapTable("orders");
            table.add(new TapField(column, dataType));
            connector.fillFieldTypes(table);
            return table.getNameFieldMap().get(column);
        }
    }
}
