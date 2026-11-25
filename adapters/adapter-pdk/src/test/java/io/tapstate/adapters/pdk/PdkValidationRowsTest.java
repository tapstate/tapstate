package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapJsonValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkValidationRowsTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(16384, 250, 24);
    private record DriverText(String value) {}
    private record DriverContainer(Map<String, Object> value) {}

    @Test void nativeObjectIdsDoNotBecomeOrdinaryLookalikeStringsAfterTheirCustomCodec(){
        var id=new org.bson.types.ObjectId("0123456789abcdef01234567");var registry=new TapCodecsRegistry();var calls=new AtomicInteger();
        registry.registerToTapValue(org.bson.types.ObjectId.class,(value,type)->{
            calls.incrementAndGet();return new TapStringValue(((org.bson.types.ObjectId)value).toHexString());
        });
        var reader=decoder(registry);var nativeRow=reader.decode(Map.of("identifier",id));var textRow=reader.decode(Map.of("identifier",id.toHexString()));
        assertThat(typed(nativeRow.fields().get("identifier")).logicalType()).isEqualTo("object-id");
        assertThat(typed(textRow.fields().get("identifier")).logicalType()).isEqualTo("text");
        assertThat(typed(nativeRow.fields().get("identifier")).conversionOrigin()).isEqualTo("custom");
        assertThat(calls).hasValue(1);
    }
    @Test void anObjectIdCodecCannotInventADifferentCanonicalIdentity(){
        var id=new org.bson.types.ObjectId("0123456789abcdef01234567");var registry=new TapCodecsRegistry();var calls=new AtomicInteger();
        registry.registerToTapValue(org.bson.types.ObjectId.class,(value,type)->{
            calls.incrementAndGet();return new TapStringValue("000000000000000000000000");
        });
        assertUnavailable(()->decoder(registry).decode(Map.of("identifier",id)));
        assertThat(calls).hasValue(1);
    }
    @Test void aBoundBsonDateProfileUsesTheSelectedCodecAndRetainsItsExactNativeInstant(){
        var registry=new TapCodecsRegistry();var calls=new AtomicInteger();var instant=Instant.parse("2026-10-21T01:02:03.123Z");
        registry.registerToTapValue(java.util.Date.class,(value,type)->{
            calls.incrementAndGet();return new TapDateTimeValue(new DateTime(((java.util.Date)value).toInstant()));
        });
        var table=bsonDateTable();
        var reader=new PdkValidationRows(table,codecs(registry),path->PdkValidationRows.Rule.AUTO,LIMITS,true);
        var value=typed(reader.decode(Map.of("time",java.util.Date.from(instant))).fields().get("time"));
        assertThat(value.logicalType()).isEqualTo("instant");assertThat(value.value()).isEqualTo(instant);
        assertThat(value.conversionOrigin()).isEqualTo("custom");assertThat(calls).hasValue(1);
    }
    @Test void aBsonDateCodecCannotRoundOrShiftTheInstantAndSqlCarriersRemainUnadmitted(){
        var registry=new TapCodecsRegistry();var calls=new AtomicInteger();
        registry.registerToTapValue(java.util.Date.class,(value,type)->{
            calls.incrementAndGet();return new TapDateTimeValue(new DateTime(Instant.EPOCH));
        });
        var reader=new PdkValidationRows(bsonDateTable(),codecs(registry),path->PdkValidationRows.Rule.AUTO,LIMITS,true);
        assertUnavailable(()->reader.decode(Map.of("time",new java.util.Date(1))));assertThat(calls).hasValue(1);
        registry.registerToTapValue(java.sql.Timestamp.class,(value,type)->new TapDateTimeValue(new DateTime(((java.sql.Timestamp)value).toInstant())));
        assertUnavailable(()->reader.decode(Map.of("time",java.sql.Timestamp.from(Instant.EPOCH))));
    }
    @Test void aMutatingBsonDateCodecCannotRewriteTheValueUsedForItsOwnFidelityCheck(){
        var registry=new TapCodecsRegistry();
        registry.registerToTapValue(java.util.Date.class,(value,type)->{
            ((java.util.Date)value).setTime(0);return new TapDateTimeValue(new DateTime(Instant.EPOCH));
        });
        var reader=new PdkValidationRows(bsonDateTable(),codecs(registry),path->PdkValidationRows.Rule.AUTO,LIMITS,true);
        assertUnavailable(()->reader.decode(Map.of("time",new java.util.Date(1))));
    }

    @Test void rawContainerChildrenRunTheirCustomCodecsOnceAndKeepNestedTypedFacts() {
        var registry = new TapCodecsRegistry();
        var calls = new AtomicInteger();
        registry.registerToTapValue(DriverText.class, (raw, type) -> {
            calls.incrementAndGet(); return new TapStringValue(((DriverText)raw).value());
        });
        var input = new LinkedHashMap<String, Object>();
        input.put("body", Map.of("items", List.of(new DriverText("001"), new DriverText(" Two "))));
        input.put("nullable", null);
        var row = decoder(registry).decode(input);
        var body = (Map<?, ?>) typed(row.fields().get("body")).value();
        var items = (List<?>) typed(body.get("items")).value();
        assertThat(typed(items.get(0)).logicalType()).isEqualTo("text");
        assertThat(typed(items.get(0)).conversionOrigin()).isEqualTo("custom");
        assertThat(typed(items.get(0)).value()).isEqualTo("001");
        assertThat(typed(items.get(1)).value()).isEqualTo(" Two ");
        assertThat(row.fields()).containsEntry("nullable", null).doesNotContainKey("absent");
        assertThat(calls).hasValue(2);
    }

    @Test void customAndAlreadyConvertedContainersDoNotApplyAChildCodecAgain() {
        var registry = new TapCodecsRegistry();
        var parentCalls = new AtomicInteger();
        registry.registerToTapValue(DriverContainer.class, (raw, type) -> {
            parentCalls.incrementAndGet(); return new TapMapValue(((DriverContainer)raw).value());
        });
        registry.registerToTapValue(String.class, (raw, type) -> { throw new AssertionError("child codec repeated"); });
        var reader = decoder(registry);
        for (Object container : List.of(new DriverContainer(Map.of("name", "raw-looking")),
                new TapMapValue(Map.of("name", "raw-looking")),
                new ConvertedValue(Map.of("name", "raw-looking"), "native-object"))) {
            var body = (Map<?, ?>) typed(reader.decode(Map.of("body", container)).fields().get("body")).value();
            assertThat(typed(body.get("name")).value()).isEqualTo("raw-looking");
            assertThat(typed(body.get("name")).conversionOrigin()).isEqualTo("already-converted");
        }
        assertThat(parentCalls).hasValue(1);
        var unknown = new TapMapValue(Map.of("name", new DriverText("needs-a-codec")));
        assertUnavailable(() -> reader.decode(Map.of("body", unknown)));
    }

    @Test void alreadyPortableJsonIsNotParsedAgainAndItsBindingSurvives() {
        var registry = new TapCodecsRegistry();
        registry.registerToTapValue(String.class, (raw, type) -> { throw new AssertionError("portable value recoded"); });
        var portable = new ValidationValues.Typed("json", "jsonb", "binding", "001");
        var result = decoder(registry).decode(Map.of("json", portable));
        assertThat(result.fields().get("json")).isEqualTo(portable);
        var stale = new ValidationValues.Typed("json", "jsonb", "old-binding", Map.of("x", 1));
        assertUnavailable(() -> decoder(registry).decode(Map.of("json", new TapArrayValue(List.of(stale)))));
    }

    @Test void nestedCustomNumericDoublesNeedAnExplicitPhysicalFloatingRule() {
        var registry = new TapCodecsRegistry();
        registry.registerToTapValue(DriverText.class, (raw, type) -> new TapNumberValue(9007199254740992d));
        assertUnavailable(() -> decoder(registry).decode(Map.of("body", List.of(new DriverText("9007199254740993")))));
        var reader = decoder(registry, path -> path.equals(List.of("body", 0))
                ? PdkValidationRows.Rule.of(PdkValidationRows.Shape.FLOAT64) : PdkValidationRows.Rule.AUTO, LIMITS);
        var output = (List<?>) typed(reader.decode(Map.of("body", List.of(new DriverText("native-floating")))).fields().get("body")).value();
        assertThat(typed(output.getFirst()).logicalType()).isEqualTo("float64");
    }

    @Test void exactNumbersAndExplicitFloatWidthsNeverUseTruncationOrTextParsing() {
        var reader = decoder(new TapCodecsRegistry());
        var row = reader.decode(Map.of("big", new BigInteger("18446744073709551615"), "decimal", new BigDecimal("1.2300"),
                "text", "001", "float", 0.1f, "double", 0.1d));
        assertThat(typed(row.fields().get("big")).value()).isEqualTo(new BigInteger("18446744073709551615"));
        assertThat(typed(row.fields().get("decimal")).value()).isEqualTo(new BigDecimal("1.2300"));
        assertThat(typed(row.fields().get("text")).logicalType()).isEqualTo("text");
        assertThat(typed(row.fields().get("float")).logicalType()).isEqualTo("float32");
        assertThat(typed(row.fields().get("double")).logicalType()).isEqualTo("float64");
        var narrow = decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.of(PdkValidationRows.Shape.FLOAT32), LIMITS);
        assertUnavailable(() -> narrow.decode(Map.of("float", 0.1d)));
        assertThat(typed(narrow.decode(Map.of("float", 0.5d)).fields().get("float")).value()).isEqualTo(0.5f);
        var exact = decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER), LIMITS);
        assertUnavailable(() -> exact.decode(Map.of("number", "1")));
    }

    @Test void jsonAndLegacyTimeRequireTheirDeclaredInterpretationAndPreservePrecision() {
        var instant = Instant.parse("2026-10-21T00:00:00.123456789Z");
        var rules = (PdkValidationRows.Rules) path -> path.equals(List.of("time"))
                ? new PdkValidationRows.Rule(PdkValidationRows.Shape.INSTANT, PdkValidationTemporal.LegacyEncoding.EPOCH_SECONDS)
                : PdkValidationRows.Rule.AUTO;
        var input = Map.<String, Object>of("json", new TapJsonValue("{\"n\":9007199254740993,\"a\":[null,\"001\"]}"),
                "time", new TapDateTimeValue(new DateTime(instant)));
        var row = decoder(new TapCodecsRegistry(), rules, LIMITS).decode(input);
        assertThat(typed(row.fields().get("time")).value()).isEqualTo(instant);
        var json = (Map<?, ?>) typed(row.fields().get("json")).value();
        assertThat(json.get("n")).isEqualTo(new BigInteger("9007199254740993"));
        assertUnavailable(() -> decoder(new TapCodecsRegistry()).decode(Map.of("time", input.get("time"))));
        var jsonRule = decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.of(PdkValidationRows.Shape.JSON), LIMITS);
        assertUnavailable(() -> jsonRule.decode(Map.of("json", new TapStringValue("{}"))));
    }

    @Test void literalDotsAndArrayOrdinalsRemainDistinctRulePaths() {
        var seen = new ArrayList<List<Object>>();
        var reader = decoder(new TapCodecsRegistry(), path -> { seen.add(path); return PdkValidationRows.Rule.AUTO; }, LIMITS);
        reader.decode(Map.of("literal.dot", List.of(Map.of("0", 7))));
        assertThat(seen).contains(List.of("literal.dot"), List.of("literal.dot", 0), List.of("literal.dot", 0, "0"));
        assertThat(seen).doesNotContain(List.of("literal", "dot", "0"));
    }

    @Test void originalModelIsPassedToTheCustomCodecWithoutLossyTypeCloning() {
        var number = new TapNumber().fixed(true).precision(38).scale(9);
        var model = new TapTable("records").add(new TapField("amount", "decimal(38,9)").tapType(number));
        var registry = new TapCodecsRegistry();
        registry.registerToTapValue(DriverText.class, (raw, type) -> {
            assertThat(type).isSameAs(number);
            assertThat(((TapNumber)type).getFixed()).isTrue();
            return new TapStringValue("declared interpretation");
        });
        var reader = new PdkValidationRows(model, codecs(registry), path -> PdkValidationRows.Rule.AUTO, LIMITS);
        var value = typed(reader.decode(Map.of("amount", new DriverText("native"))).fields().get("amount"));
        assertThat(value.nativeType()).isEqualTo("decimal(38,9)");
        assertThat(value.logicalType()).isEqualTo("text");
        assertThat(value.value()).isEqualTo("declared interpretation");
    }

    @Test void rowOwnershipSurvivesInputMutationAndDoesNotSilentlyDropExtraFields() {
        byte[] bytes = { 1, 2 };
        var children = new ArrayList<Object>(); children.add("before");
        var input = new LinkedHashMap<String, Object>(); input.put("binary", bytes); input.put("extra", children);
        var row = decoder(new TapCodecsRegistry()).decode(input);
        bytes[0] = 9; children.clear(); input.clear();
        assertThat(row.fields()).containsOnlyKeys("binary", "extra");
        assertThat(((ValidationValues.Binary)typed(row.fields().get("binary")).value()).bytes()).containsExactly((byte)1, (byte)2);
        assertThat((List<?>)typed(row.fields().get("extra")).value()).hasSize(1);
        assertThatThrownBy(row.fields()::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void cyclesNodeDepthAndAggregateBytesFailBeforeUnlimitedCopies() {
        var cycle = new LinkedHashMap<String, Object>(); cycle.put("self", cycle);
        assertThatThrownBy(() -> decoder(new TapCodecsRegistry()).decode(cycle)).isInstanceOf(TapstateException.class);
        for (var limits : List.of(new ValidationValues.Limits(16384, 3, 24), new ValidationValues.Limits(16384, 250, 2),
                new ValidationValues.Limits(300, 250, 24))) {
            assertThatThrownBy(() -> decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.AUTO, limits)
                    .decode(Map.of("one", "a", "two", List.of("b", "c"))))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        }
    }

    @Test void sourceAndTargetRowsCompareStructureAndTypedValuesWithoutBorrowingBusinessAnswers() {
        var reader = decoder(new TapCodecsRegistry());
        var left = reader.decode(Map.of("id", 1, "items", List.of(Map.of("amount", new BigDecimal("1.00")))));
        var right = reader.decode(Map.of("items", List.of(Map.of("amount", BigDecimal.ONE)), "id", 1L));
        assertThat(ValidationValueComparison.compare(left, right)).isEmpty();
        var wrong = reader.decode(Map.of("id", 1L, "items", List.of(Map.of("amount", new BigDecimal("1.01")))));
        assertThat(ValidationValueComparison.compare(left, wrong)).get().extracting(ValidationValueComparison.Difference::path)
                .isEqualTo(List.of("items", 0, "amount"));
    }

    @Test void missingDeclaredWrapperKindIsRejectedBeforeItsPayloadIsRead() {
        var accesses = new AtomicInteger();
        var malformed = new TapValue<Object, TapType>() {
            @Override public TapType createDefaultTapType() { return null; }
            @Override public Class<? extends TapType> tapTypeClass() { return null; }
            @Override public Object getValue() { accesses.incrementAndGet(); return "untyped payload"; }
        };
        assertUnavailable(() -> decoder(new TapCodecsRegistry()).decode(Map.of("body", malformed)));
        assertThat(accesses).hasValue(0);
    }

    @Test void nestedPortableFailureReportsItsActualFieldPath() {
        var stale = new ValidationValues.Typed("text", "varchar", "stale-binding", "value");
        var body = new TapMapValue(Map.of("items", new TapArrayValue(List.of(
                new ValidationValues.Typed("object", "document", "binding", Map.of("leaf", stale))))));
        assertThatThrownBy(() -> decoder(new TapCodecsRegistry()).decode(Map.of("body", body)))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE);
                    assertThat(failure.args()).containsEntry("field", "body/items/0/leaf");
                });
    }

    @Test void aKnownWrongWrapperIsRejectedBeforeItsGetterRuns() {
        var accesses = new AtomicInteger();
        var wrong = new TapStringValue("1") {
            @Override public String getValue() { accesses.incrementAndGet(); return super.getValue(); }
        };
        var exact = decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER), LIMITS);
        assertUnavailable(() -> exact.decode(Map.of("amount", wrong)));
        assertThat(accesses).hasValue(0);
    }

    @Test void anInnerTypedValueCannotBypassTheOuterWrapperOrPhysicalRule() {
        var contradiction = new TapValue<Object, TapNumber>() {
            @Override public TapNumber createDefaultTapType() { return new TapNumber(); }
            @Override public Class<? extends TapType> tapTypeClass() { return TapNumber.class; }
            @Override public Object getValue() { return new ValidationValues.Typed("text", "varchar", "binding", "abc"); }
        };
        var exact = decoder(new TapCodecsRegistry(), path -> PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER), LIMITS);
        assertUnavailable(() -> exact.decode(Map.of("amount", contradiction)));
        assertUnavailable(() -> decoder(new TapCodecsRegistry()).decode(Map.of("amount", contradiction)));
    }

    @Test void alreadyPortableExactNumbersCannotContainAFloatingPayload() {
        var invalid = new ValidationValues.Typed("exact-number", "decimal", "binding", 0.1d);
        assertUnavailable(() -> decoder(new TapCodecsRegistry()).decode(Map.of("amount", invalid)));
    }

    @Test void portableContainerChildrenStillHaveToMeetAnExplicitPhysicalRepresentation() {
        var object = new ValidationValues.Typed("object", "document", "binding", Map.of("amount", 0.1d));
        var reader = decoder(new TapCodecsRegistry(), path -> path.equals(List.of("body", "amount"))
                ? PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER) : PdkValidationRows.Rule.AUTO, LIMITS);
        assertUnavailable(() -> reader.decode(Map.of("body", object)));
    }

    @Test void convertedContainerStringsNeedAJsonMarkerBeforeAnyParsing() {
        var reader = decoder(new TapCodecsRegistry(), path -> path.equals(List.of("body", "json"))
                ? PdkValidationRows.Rule.of(PdkValidationRows.Shape.JSON) : PdkValidationRows.Rule.AUTO, LIMITS);
        assertUnavailable(() -> reader.decode(Map.of("body", new TapMapValue(Map.of("json", "{}")))));
        var accepted = reader.decode(Map.of("body", new TapMapValue(Map.of("json", new TapJsonValue("{}")))));
        var body = (Map<?, ?>) typed(accepted.fields().get("body")).value();
        assertThat(typed(body.get("json")).value()).isEqualTo(Map.of());
    }

    @Test void portableJsonCannotHideFloatingOrNonJsonPayloadsInItsTree() {
        var reader = decoder(new TapCodecsRegistry());
        for (Object child : List.of(0.1d, new ValidationValues.Binary(0, new byte[] { 1 }), Instant.EPOCH)) {
            var invalid = new ValidationValues.Typed("json", "jsonb", "binding", Map.of("child", child));
            assertUnavailable(() -> reader.decode(Map.of("json", invalid)));
        }
        var exact = new ValidationValues.Typed("json", "jsonb", "binding", Map.of("child", new BigDecimal("0.1")));
        assertThat(reader.decode(Map.of("json", exact)).fields().get("json")).isEqualTo(exact);
    }

    @Test void anUnregisteredPortableLogicalTagCannotBypassTheReaderRules() {
        var unknown = new ValidationValues.Typed("opaque-time", "vendor-type", "binding", Instant.EPOCH);
        assertUnavailable(() -> decoder(new TapCodecsRegistry()).decode(Map.of("time", unknown)));
    }

    private static PdkValidationCodecs codecs(TapCodecsRegistry registry) { return new PdkValidationCodecs("fixture", "binding", registry); }
    private static TapTable bsonDateTable(){return new TapTable("documents").add(new TapField("time","DATE_TIME")
            .tapType(new io.tapdata.entity.schema.type.TapDateTime()));}
    private static PdkValidationRows decoder(TapCodecsRegistry registry) {
        return decoder(registry, path -> PdkValidationRows.Rule.AUTO, LIMITS);
    }
    private static PdkValidationRows decoder(TapCodecsRegistry registry, PdkValidationRows.Rules rules, ValidationValues.Limits limits) {
        var table = new TapTable("records").add(new TapField("body", "object").tapType(new TapMap()))
                .add(new TapField("nullable", "text").tapType(new TapString()));
        return new PdkValidationRows(table, codecs(registry), rules, limits);
    }
    private static ValidationValues.Typed typed(Object value) { return (ValidationValues.Typed)value; }
    private static void assertUnavailable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }
}
