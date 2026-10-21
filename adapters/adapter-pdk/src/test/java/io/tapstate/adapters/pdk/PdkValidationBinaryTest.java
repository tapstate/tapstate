package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValueComparison;
import io.tapstate.core.validation.ValidationValues;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationBinaryTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(4096, 50, 8);
    private record DriverBytes(byte[] bytes) {}

    @Test void customBinaryResultRunsOnceAndOwnedBytesKeepItsUnsignedSubtype() {
        var registry = new TapCodecsRegistry();
        var calls = new AtomicInteger();
        registry.registerToTapValue(DriverBytes.class, (value, model) -> {
            calls.incrementAndGet();
            assertThat(model).isInstanceOf(TapBinary.class);
            return new TapBinaryValue(new ByteData((byte)0x80, ((DriverBytes)value).bytes()));
        });
        var decoder = decoder(registry);
        byte[] raw = { 0, 1, -1 };
        var value = binary(decoder, new DriverBytes(raw), 128);
        raw[0] = 99;
        assertThat(value.subtype()).isEqualTo(128);
        assertThat(value.bytes()).containsExactly((byte)0, (byte)1, (byte)-1);
        byte[] borrowed = value.bytes(); borrowed[1] = 42;
        assertThat(value.bytes()).containsExactly((byte)0, (byte)1, (byte)-1);
        assertThat(calls).hasValue(1);
    }

    @Test void directReadOnlyBufferCopiesOnlyItsRemainingSliceWithoutMovingIt() {
        var decoder = decoder(new TapCodecsRegistry());
        ByteBuffer writable = ByteBuffer.allocateDirect(5);
        writable.put(new byte[] { 10, 20, 30, 40, 50 }).position(1).limit(4);
        ByteBuffer input = writable.asReadOnlyBuffer();
        var value = binary(decoder, input, 128);
        assertThat(input.position()).isEqualTo(1);
        assertThat(input.limit()).isEqualTo(4);
        writable.put(2, (byte)99);
        assertThat(value.bytes()).containsExactly((byte)20, (byte)30, (byte)40);
        assertThat(value.subtype()).isEqualTo(-1);
    }

    @Test void byteBudgetIsCheckedBeforeAnOwnedCopyAndBuffersRemainUnchangedOnFailure() {
        var decoder = decoder(new TapCodecsRegistry());
        ByteBuffer input = ByteBuffer.wrap(new byte[10]);
        input.position(2);
        assertThatThrownBy(() -> binary(decoder, input, 71)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
        assertThat(input.position()).isEqualTo(2);
        assertThat(input.limit()).isEqualTo(10);
        assertThat(binary(decoder, input, 72).length()).isEqualTo(8);
    }

    @Test void subtypeNullEmptyAndDifferentByteValuesRemainDistinctPortableFacts() {
        var decoder = decoder(new TapCodecsRegistry());
        var generic = binary(decoder, new byte[] { 1 }, 128);
        var subtypeZero = binary(decoder, new TapBinaryValue(new ByteData((byte)0, new byte[] { 1 })), 128);
        var subtypeOne = binary(decoder, new TapBinaryValue(new ByteData((byte)1, new byte[] { 1 })), 128);
        assertThat(ValidationValueComparison.compare(row(generic), row(subtypeZero))).isPresent();
        assertThat(ValidationValueComparison.compare(row(subtypeZero), row(subtypeOne))).isPresent();
        assertThat(ValidationValueComparison.compare(row(generic), row(binary(decoder, new byte[] { 2 }, 128)))).isPresent();
        assertThat(binary(decoder, new byte[0], 64).length()).isZero();
        assertThat(binary(decoder, null, 64)).isNull();
        assertThat(binary(decoder, new TapBinaryValue((ByteData)null), 64)).isNull();
        assertThatThrownBy(() -> binary(decoder, new TapBinaryValue(new ByteData()), 64))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }

    @Test void convertedAndTypedBinariesDoNotRunAnotherCodecOrLoseTheirBinding() {
        var registry = new TapCodecsRegistry();
        registry.registerToTapValue(byte[].class, (value, model) -> { throw new AssertionError("second codec"); });
        var decoder = decoder(registry);
        var owned = new ValidationValues.Binary(255, new byte[] { 7 });
        var typed = new ValidationValues.Typed("binary", "native-bytes", "binding", owned);
        assertThat(binary(decoder, typed, 128)).isSameAs(owned);
        assertThat(binary(decoder, new ConvertedValue(new byte[] { 7 }, "blob"), 128).bytes()).containsExactly((byte)7);
        assertThatThrownBy(() -> binary(decoder,
                new ValidationValues.Typed("binary", "native-bytes", "stale", owned), 128))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> binary(decoder,
                new ValidationValues.Typed("text", "native-bytes", "binding", owned), 128))
                .isInstanceOf(TapstateException.class);
    }

    @Test void customTextResultAndEncodedLookingStringsAreNeverDecodedAsBytes() {
        var registry = new TapCodecsRegistry();
        registry.registerToTapValue(DriverBytes.class, (value, model) -> new TapStringValue("0102"));
        var decoder = decoder(registry);
        for (Object value : new Object[] { "0102", "AQI=", new DriverBytes(new byte[] { 1, 2 }) }) {
            assertThatThrownBy(() -> binary(decoder, value, 128)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        }
    }

    @Test void byteDataAccessFailuresAreCodedAndFatalErrorsStayBare() {
        var decoder = decoder(new TapCodecsRegistry());
        var cause = new IllegalStateException("binary payload failure");
        var broken = new ByteData() { @Override public byte[] getValue() { throw cause; } };
        assertThatThrownBy(() -> binary(decoder, new TapBinaryValue(broken), 128))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_FAILED);
                    assertThat(failure.getCause()).isSameAs(cause);
                });
        var fatal = new AssertionError("fatal binary payload");
        var fatalData = new ByteData() { @Override public byte[] getValue() { throw fatal; } };
        assertThatThrownBy(() -> binary(decoder, new TapBinaryValue(fatalData), 128)).isSameAs(fatal);
    }

    private static PdkValidationCodecs decoder(TapCodecsRegistry registry) {
        return new PdkValidationCodecs("fixture", "binding", registry);
    }
    private static ValidationValues.Binary binary(PdkValidationCodecs decoder, Object value, long bytes) {
        return decoder.binary("payload", decoder.select("payload", value, new TapBinary()), bytes);
    }
    private static ValidationValues.Row row(Object value) { return ValidationValues.ownRow(Map.of("bytes", value), LIMITS); }
}
