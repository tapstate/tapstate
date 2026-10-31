package io.tapstate.app;

import io.tapstate.core.common.*;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.*;
import java.math.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeValidationMetadataTest {
    @Test void numericMetadataChargesOwnedPrecisionRatherThanAFixedGuess(){
        var bound=new BigDecimal(BigInteger.ONE.shiftLeft(8*1024*1024));
        var numeric=new NumericType(null,true,false,null,bound.negate(),bound,null,null);
        var model=new SourceTable("records",List.of(new SourceField("id","numeric",TapstateType.DECIMAL,null,numeric)),List.of(),List.of());
        assertThat(NativeValidationMetadata.modelBytes(model)).isGreaterThan(2L*1024*1024).isLessThan(4L*1024*1024);
    }
    @Test void oversizedMetadataStopsAtTheBoundWithoutRenderingAValueOrGuessingAType(){
        var model=new SourceTable("records",List.of(new SourceField("id","x".repeat(2*1024*1024),TapstateType.STRING)),List.of(),List.of());
        assertThatThrownBy(()->NativeValidationMetadata.modelBytes(model)).isInstanceOfSatisfying(TapstateException.class,
                failed->assertThat(failed.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }
}
