package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapCoefficientFloat;
import io.tapdata.entity.schema.type.TapDouble;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapDoubleValue;
import io.tapdata.entity.schema.value.TapFloatValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ConvertedValue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** The new shared entity contract must survive the existing portable conversion boundary. */
class Pdk211CompatibilityTest {
    @Test
    void theNewSharedContractHasItsOwnLevelWithoutRenumberingOldVersions() {
        assertThat(PdkApiLevels.ENGINE_LEVEL).isEqualTo(2);
        for (String version : new String[] {"2.0.11", "2.0.11-SNAPSHOT", "2.0.11-20261019.093332-6"}) {
            assertThat(PdkLevelResolver.resolve(version, null).requiredLevel()).as(version).isEqualTo(2);
            assertThat(PdkLevelResolver.resolve(version, null).outcome()).as(version).isEqualTo(LevelOutcome.COMPATIBLE);
        }
        for (String version : new String[] {"2.0.5", "2.0.6", "2.0.7", "2.0.8", "2.0.9"}) {
            assertThat(PdkLevelResolver.resolve(version, null).requiredLevel()).as(version).isEqualTo(1);
        }
        assertThat(PdkLevelResolver.resolve("2.0.10-SNAPSHOT", null).outcome()).isEqualTo(LevelOutcome.UNKNOWN_VERSION);
        assertThat(PdkLevelResolver.resolve("2.0.12-SNAPSHOT", null).outcome()).isEqualTo(LevelOutcome.UNKNOWN_VERSION);
        assertThat(PdkLevelResolver.resolve("2.0.11", "3").outcome()).isEqualTo(LevelOutcome.INCOMPATIBLE);
    }

    @Test
    void newFloatingDescriptorsKeepTheExistingApproximateNumericProjection() {
        for (TapNumber type : new TapNumber[] {new TapFloat(), new TapDouble()}) {
            assertThat(PdkTypeMapping.of(type)).as(type.getClass().getSimpleName()).isEqualTo(TapstateType.DOUBLE);
            var numeric = PdkTypeMapping.numericType(type);
            assertThat(numeric.fixed()).isFalse();
            assertThat(PdkTypeMapping.of(PdkTypeMapping.targetType(TapstateType.DOUBLE, numeric)))
                    .isEqualTo(TapstateType.DOUBLE);
            assertThat(PdkTypeMapping.targetType(TapstateType.DOUBLE, numeric)).isInstanceOf(type.getClass());
        }
        // This intermediate mapping type has no concrete precision until the PDK resolves its rule.
        assertThat(PdkTypeMapping.of(new TapCoefficientFloat())).isEqualTo(TapstateType.UNKNOWN);
    }

    @Test
    void coefficientRulesResolveToConcreteFloatingTypesBeforeTargetTypeSelection(@TempDir Path dir) {
        String spec = """
                {"dataTypes":{"float[($precision)]":{"to":"TapCoefficientFloat",
                  "precision":[1,53],"defaultPrecision":53,
                  "coefficient":{"TapFloat":[1,24],"TapDouble":[25,53]}}}}
                """;
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.discoverableSource(dir)),
                "synthetic.Discoverable", "2.0.11-SNAPSHOT", null, spec);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            TapTable source = new TapTable("measurements");
            source.add(new TapField("narrow", "float(24)"));
            source.add(new TapField("wide", "float(53)"));
            connector.fillFieldTypes(source);
            assertThat(source.getNameFieldMap().get("narrow").getTapType()).isInstanceOf(TapFloat.class);
            assertThat(source.getNameFieldMap().get("wide").getTapType()).isInstanceOf(TapDouble.class);
            TapTable target = new TapTable("copy");
            source.getNameFieldMap().forEach((name, field) -> target.add(new TapField(name, null)
                    .tapType(PdkTypeMapping.targetType(PdkTypeMapping.of(field.getTapType()),
                            PdkTypeMapping.numericType(field.getTapType())))));
            connector.resolveTargetTypes(target);
            // Golden: the PDK removes the optional precision group from its target SQL spelling.
            // Keep that upstream result; this bridge must not silently rewrite the SQL contract.
            assertThat(target.getNameFieldMap().get("narrow").getDataType()).isEqualTo("float");
            assertThat(target.getNameFieldMap().get("wide").getDataType()).isEqualTo("float");
        }
    }

    @Test
    void theExistingNumericProjectionDoesNotInventUnrepresentedBinaryPrecision(@TempDir Path dir) {
        TapFloat declared = new TapFloat().binaryPrecision(17);
        TapFloat projected = (TapFloat) PdkTypeMapping.targetType(TapstateType.DOUBLE,
                PdkTypeMapping.numericType(declared));
        assertThat(projected.getBinaryPrecision()).isNull();
        // Golden: the portable numeric descriptor has no binaryPrecision field. Do not overload
        // decimal precision or infer this source-specific property from a database type token.
        String spec = """
                {"dataTypes":{"float($precision)":{"to":"TapCoefficientFloat",
                  "precision":[1,53],"coefficient":{"TapFloat":[1,24],"TapDouble":[25,53]}}}}
                """;
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.discoverableSource(dir)),
                "synthetic.Discoverable", "2.0.11-SNAPSHOT", null, spec);
        try (PdkConnector connector = PdkConnector.open("demo", ref, Map.of())) {
            TapTable table = new TapTable("measurements");
            table.add(new TapField("declared", null).tapType(declared));
            table.add(new TapField("projected", null).tapType(projected));
            connector.resolveTargetTypes(table);
            assertThat(table.getNameFieldMap().get("declared").getDataType()).isEqualTo("float(17)");
            assertThat(table.getNameFieldMap().get("projected").getDataType()).isEqualTo("float(24)");
        }
    }

    @Test
    void floatingValueKindsAndMetadataReachTheirOwnTargetCodecsAfterSerialization() throws Exception {
        for (double value : new double[] {0d, -0d, 1.5d, (double) Float.MAX_VALUE, Double.MIN_VALUE,
                Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            TapFloat floatType = new TapFloat().storageBytes(4).binaryPrecision(24)
                    .effectivePrecision(7).supportsNaN(true).supportsInfinity(true);
            // The PDK float box stores binary32 values; compare transport after that conversion.
            double floatInput = (double) (float) value;
            TapFloatValue floatValue = new TapFloatValue(floatInput);
            floatValue.setTapType(floatType);
            checkRoundTrip(floatValue, floatType, "TapFloatValue", floatInput);

            TapDouble doubleType = new TapDouble().storageBytes(8).binaryPrecision(53)
                    .effectivePrecision(15).supportsNaN(true).supportsInfinity(true);
            TapDoubleValue doubleValue = new TapDoubleValue(value);
            doubleValue.setTapType(doubleType);
            checkRoundTrip(doubleValue, doubleType, "TapDoubleValue", value);
        }
    }

    private static void checkRoundTrip(TapValue<?, ?> source, TapNumber type, String kind, double expected)
            throws Exception {
        TapTable table = new TapTable("measurements");
        table.add(new TapField("reading", "floating").tapType(type));
        var conversion = new PdkValueConversions(new TapCodecsRegistry());
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("reading", source);
        input.put("absent", null);
        Map<String, Object> portable = conversion.read(input, PdkRowSchema.of(table),
                (value, path) -> "floating", (original, value) -> value).orElseThrow();
        ConvertedValue carrier = (ConvertedValue) portable.get("reading");
        assertThat(carrier.valueKind()).isEqualTo(kind);
        assertThat(Double.doubleToRawLongBits((Double) carrier.value()))
                .isEqualTo(Double.doubleToRawLongBits(expected));
        assertThat(portable).containsEntry("absent", null);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(portable);
        }
        Map<String, Object> restored;
        try (ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) inputStream.readObject();
            restored = row;
        }
        TapCodecsRegistry target = new TapCodecsRegistry()
                .registerFromTapValue(TapFloatValue.class, value -> {
                    TapFloat metadata = (TapFloat) value.getTapType();
                    assertThat(metadata.getStorageBytes()).isEqualTo(4);
                    assertThat(metadata.getBinaryPrecision()).isEqualTo(24);
                    assertThat(metadata.getEffectivePrecision()).isEqualTo(7);
                    assertThat(metadata.getSupportsNaN()).isTrue();
                    return "TapFloatValue:" + Double.doubleToRawLongBits(value.getValue());
                })
                .registerFromTapValue(TapDoubleValue.class, value -> {
                    TapDouble metadata = (TapDouble) value.getTapType();
                    assertThat(metadata.getStorageBytes()).isEqualTo(8);
                    assertThat(metadata.getBinaryPrecision()).isEqualTo(53);
                    assertThat(metadata.getEffectivePrecision()).isEqualTo(15);
                    assertThat(metadata.getSupportsInfinity()).isTrue();
                    return "TapDoubleValue:" + Double.doubleToRawLongBits(value.getValue());
                })
                .registerFromTapValue(TapNumberValue.class, value -> "wrong legacy codec");
        Map<String, Object> written = new PdkValueConversions(target).write(restored,
                value -> { throw new AssertionError("a typed carrier must not enter the legacy fallback"); })
                .orElseThrow();
        assertThat(written).containsEntry("reading", kind + ":" + Double.doubleToRawLongBits(expected))
                .containsEntry("absent", null);
    }
}
