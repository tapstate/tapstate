package io.tapstate.adapters.pdk;

import io.tapstate.core.common.*;
import io.tapstate.spi.sink.*;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TargetPreparationHashGoldenTest {
    @Test void originalHashRetainsNullableAttributesDecimalScaleAndEveryUtf16CodeUnit() {
        var number=new NumericType(64,true,false,null,new BigDecimal("-10.00"),new BigDecimal("10.00"),4,2);
        var model=new TargetTable("orders",List.of(new TargetField("_id","BIGINT",true,TapstateType.INT64,number),
                new TargetField("v\ud800\ud83d\ude00","varchar(5)",false,TapstateType.STRING,null,
                        new StringType(5L,false,true,null,2))),List.of(new TargetIndex(List.of("_id"),true,true)));
        assertThat(PdkSinkPreparations.modelHash(model)).isEqualTo("5aa6dffaa32f309174cace4db6648fc572ea2c4fa34cbde736dcb0e275625e37");
    }
}
