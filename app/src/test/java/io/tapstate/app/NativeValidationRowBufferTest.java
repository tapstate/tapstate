package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.validation.ValidationError;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeValidationRowBufferTest {
    private static final ConnectorRef POSTGRES=new ConnectorRef(List.of(),"io.tapdata.connector.postgres.PostgresConnector",null,null);
    @Test void theIndependentReadCapsRawBytesWithoutChangingSavedConnectionOrSecurityOptions(){
        var settings=Map.<String,Object>of("host","localhost","extParams","?sslmode=verify-full&maxResultBuffer=999999999&binaryTransfer=false");
        var source=new SourceResource("input",null,"postgres",settings,null,null,null,null);
        var independent=NativeValidationPdkRows.rowConnection(source,POSTGRES);
        assertThat(independent.settings().get("extParams")).isEqualTo("?sslmode=verify-full&binaryTransfer=false&maxResultBuffer=16777216");
        assertThat(independent.settings()).doesNotContainKey("additionalString");
        assertThat(source.config()).isEqualTo(settings);
        assertThat(NativeValidationPdkRows.rowReadBytes(POSTGRES,1)).isGreaterThan(3*NativeValidationPdkRows.POSTGRES_RESULT_BYTES);
        assertThat(NativeValidationPdkRows.rowReadBytes(POSTGRES,2)-NativeValidationPdkRows.rowReadBytes(POSTGRES,1)).isEqualTo(64L*256);
    }
    @Test void encodedDuplicateAndStricterOriginalBoundsCannotRaiseTheReadCap(){
        var source=new SourceResource("input",null,"postgres",Map.of("extParams","?%6daxResultBuffer=2000000&maxResultBuffer=1000000&ssl=true"),null,null,null,null);
        assertThat(NativeValidationPdkRows.rowConnection(source,POSTGRES).settings().get("extParams"))
                .isEqualTo("?ssl=true&maxResultBuffer=1000000");
    }
    @Test void legacySchemaAliasIsNotPromotedIntoDifferentNativeConnectionOptions(){
        var source=new SourceResource("input",null,"postgres",Map.of("additionalString","?ssl=true"),null,null,null,null);
        var independent=NativeValidationPdkRows.rowConnection(source,POSTGRES);
        assertThat(independent.settings().get("additionalString")).isEqualTo("?ssl=true");
        assertThat(independent.settings().get("extParams")).isEqualTo("?maxResultBuffer=16777216");
    }
    @Test void explicitZeroBoundsRemainZeroInBothUrlAndProperties(){
        for(var settings:List.of(Map.<String,Object>of("extParams","?maxResultBuffer=0"),Map.<String,Object>of("properties",Map.of("maxResultBuffer","0")))){
            var source=new SourceResource("input",null,"postgres",settings,null,null,null,null);
            assertThat(NativeValidationPdkRows.rowConnection(source,POSTGRES).settings().get("extParams")).isEqualTo("?maxResultBuffer=0");
        }
    }
    @Test void uncertainUrlOverridesAreRefusedBeforeOpeningAQuery(){
        for(var settings:List.of(Map.<String,Object>of("databaseUrlPattern","foreign"),Map.<String,Object>of("extParams","?maxResultBuffer=unknown"),
                Map.<String,Object>of("extParams",4))){
            var source=new SourceResource("input",null,"postgres",settings,null,null,null,null);
            assertThatThrownBy(()->NativeValidationPdkRows.rowConnection(source,POSTGRES)).isInstanceOfSatisfying(TapstateException.class,
                    failure->assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        }
    }
}
