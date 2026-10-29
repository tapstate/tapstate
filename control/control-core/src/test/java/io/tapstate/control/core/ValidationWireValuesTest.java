package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.math.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationWireValuesTest {
    @Test void exactNumbersNeverTravelAsJsonNumbersOrExplosivePlainDecimalStrings(){
        var encoder=new ValidationWireValues.Encoder();
        assertThat(encoder.value(9007199254740993L)).isEqualTo(new ValidationWireValues.Value("int64","9007199254740993",Map.of()));
        var exact=encoder.value(new BigDecimal("9007199254740993.000000001"));
        assertThat(exact.type()).isEqualTo("decimal");assertThat(exact.value()).isEqualTo("9007199254740993000000001");assertThat(exact.metadata()).containsEntry("scale","9");
        assertThat(encoder.value(new BigDecimal(BigInteger.ONE,Integer.MIN_VALUE)).metadata()).containsEntry("scale","-2147483648");
        assertThat(encoder.value(-0.0d).value()).isEqualTo("8000000000000000");
        assertThat(encoder.value(Double.longBitsToDouble(0x7ff8000000000001L)).value()).isEqualTo("7ff8000000000001");
    }
    @Test void objectsKeepLiteralNamesAndDistinguishAbsentNullEmptyAndTypedCodecProvenance(){
        var input=new LinkedHashMap<String,Object>();input.put("a.b",null);input.put("empty","");input.put("array",List.of());
        input.put("__proto__",new ValidationValues.Typed("object-id","ObjectId","original-codec","pdk-custom","507f1f77bcf86cd799439011"));
        var encoded=new ValidationWireValues.Encoder().value(input);assertThat(encoded.type()).isEqualTo("object");
        @SuppressWarnings("unchecked") var members=(List<ValidationWireValues.Entry>)encoded.value();
        assertThat(members).extracting(ValidationWireValues.Entry::name).containsExactly("a.b","empty","array","__proto__");
        assertThat(members.getFirst().value().type()).isEqualTo("null");assertThat(members.get(1).value().value()).isEqualTo("");
        assertThat(members.get(2).value().type()).isEqualTo("array");assertThat(members.get(2).value().value()).isEqualTo(List.of());
        assertThat(members.get(3).value().metadata()).containsEntry("logicalType","object-id").containsEntry("conversionOrigin","pdk-custom").containsEntry("nativeType","ObjectId");
    }
    @Test void binarySubtypesTemporalDomainsAndCompositeKeyOrderRemainExplicit(){
        var encoder=new ValidationWireValues.Encoder();var binary=encoder.value(new ValidationValues.Binary(128,new byte[]{(byte)255,0}));
        assertThat(binary.value()).isEqualTo("/wA=");assertThat(binary.metadata()).containsEntry("subtype","128");
        var instant=Instant.parse("2026-10-29T07:00:01.123456789Z");assertThat(encoder.value(instant)).isEqualTo(new ValidationWireValues.Value("instant",instant.toString(),Map.of()));
        assertThat(encoder.value(LocalDateTime.parse("2026-10-29T15:00:01.123456789")).type()).isEqualTo("local-date-time");
        var key=new ValidationKey(List.of(ValidationKey.Part.number(9007199254740993L),ValidationKey.Part.text("literal.with.dot"),ValidationKey.Part.binary(128,new byte[]{0,1})));
        var root=encoder.root(new ValidationRoot("mapping","output",key,List.of()));
        assertThat(root.key()).extracting(ValidationWireValues.KeyPart::domain).containsExactly("EXACT_NUMBER","TEXT","BINARY");
        assertThat(root.key().getFirst().value().value()).isEqualTo("9007199254740993");
    }
    @Test void unsupportedCyclesAndExcessAreRejectedBeforeAnUnboundedWireProjectionIsBuilt(){
        var cycle=new ArrayList<Object>();cycle.add(cycle);
        assertThatThrownBy(()->new ValidationWireValues.Encoder().value(cycle)).isInstanceOf(TapstateException.class).hasMessageContaining("wire.valueCycle");
        assertThatThrownBy(()->new ValidationWireValues.Encoder().value(Collections.nCopies(65536,null))).isInstanceOf(TapstateException.class).hasMessageContaining("reportWire");
        assertThatThrownBy(()->new ValidationWireValues.Encoder().value("x".repeat(6*1024*1024))).isInstanceOf(TapstateException.class).hasMessageContaining("reportWire");
        assertThatThrownBy(()->new ValidationWireValues.Encoder().value(new Object())).isInstanceOf(TapstateException.class).hasMessageContaining("value-type-unsupported");
    }
}
