package io.tapstate.core.common;

import java.util.*;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PreparedTargetModelTest {
    private static final String GOLDEN="AAAAAQAAAAYAbwByAGQAZQByAHMAAAACAAAAAwBfAGkAZAAAAAYAQgBJAEcASQBOAFQBAAAABQBJAE4AVAA2ADQBAAAAAgA2ADQAAAAEAHQAcgB1AGUAAAAFAGYAYQBsAHMAZf////8AAAAGAC0AMQAwAC4AMAAwAAAABQAxADAALgAwADAAAAABADQAAAABADIAAAAABAB22ADYPd4AAAAACgB2AGEAcgBjAGgAYQByACgANQApAAAAAAYAUwBUAFIASQBOAEcAAQAAAAEANQAAAAUAZgBhAGwAcwBlAAAABAB0AHIAdQBl/////wAAAAEAMgAAAAEAAAABAAAAAwBfAGkAZAEB";
    @Test void originalPreimageIsByteExactAndPreservesEveryAttributeAndUtf16Unit() {
        var bytes=Base64.getDecoder().decode(GOLDEN);var model=PreparedTargetModel.decode(bytes);
        assertThat(model.canonical()).isEqualTo(bytes);
        assertThat(model.modelHash()).isEqualTo("5aa6dffaa32f309174cace4db6648fc572ea2c4fa34cbde736dcb0e275625e37");
        assertThat(model.fields().get(0).numericType().minValue().toString()).isEqualTo("-10.00");
        assertThat(model.fields().get(1).name()).isEqualTo("v\ud800\ud83d\ude00");
        assertThat(model.fields().get(1).stringType()).isEqualTo(new StringType(5L,false,true,null,2));
    }
    @Test void malformedLengthsBooleansVersionsAndTrailingBytesRefuseWithoutRepair() {
        var original=Base64.getDecoder().decode(GOLDEN);
        var count=original.clone();ByteBuffer.wrap(count).putInt(20,Integer.MAX_VALUE);
        var version=original.clone();ByteBuffer.wrap(version).putInt(0,2);
        var bool=original.clone();bool[50]=2;
        for(var bytes:List.of(count,version,bool,Arrays.copyOf(original,original.length+1),Arrays.copyOf(original,10),new byte[1048577]))
            assertThatThrownBy(() -> PreparedTargetModel.decode(bytes)).isInstanceOf(IllegalArgumentException.class);
    }
}
