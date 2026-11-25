package io.tapstate.app;

import io.tapstate.core.common.*;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationKeyMetadataTest {
    @Test void exactAndApproximateModelsKeepSeparateNumericPoliciesWithoutNativeStringGuessing(){
        var model=model(new SourceField("whole","opaque spelling",TapstateType.INT64),new SourceField("decimal","opaque spelling",TapstateType.DECIMAL),
                new SourceField("approximate","opaque spelling",TapstateType.DOUBLE),new SourceField("text","NUMBER",TapstateType.STRING),new SourceField("flag","opaque spelling",TapstateType.BOOLEAN));
        var rules=ValidationKeyMetadata.describe(model);
        assertThat(rules.fields().get("whole").numericPolicy()).isEqualTo(ValidationKeyBinding.NumericPolicy.EXACT_ONLY);
        assertThat(rules.fields().get("decimal").logicalTypes()).containsExactly("exact-number");
        assertThat(rules.fields().get("approximate").numericPolicy()).isEqualTo(ValidationKeyBinding.NumericPolicy.EXACT_BINARY_FLOAT);
        assertThat(rules.fields().get("approximate").logicalTypes()).contains("float32","float64","exact-number");
        assertThat(rules.fields().get("text").logicalTypes()).containsExactly("text");
        assertThat(rules.fields().get("flag").domain()).isEqualTo(ValidationKey.Kind.BOOLEAN);
    }
    @Test void textKeysUseOnlyThePortableModelType(){
        var model=model(new SourceField("native_id","OBJECT_ID",TapstateType.STRING),new SourceField("ordinary","VARCHAR",TapstateType.STRING));
        var fields=ValidationKeyMetadata.describe(model).fields();
        assertThat(fields.get("native_id").logicalTypes()).containsExactly("text");
        assertThat(fields.get("ordinary").logicalTypes()).containsExactly("text");
    }
    @Test void unknownTemporalBinarySubtypesAndContainersDoNotAcquireGuessedKeyDomains(){
        var model=model(new SourceField("binary","BINARY",TapstateType.BINARY),new SourceField("calendar","TIMESTAMP",TapstateType.DATETIME),
                new SourceField("document","JSON",TapstateType.JSON),new SourceField("raw","NUMBER"),new SourceField("nested.path","VARCHAR",TapstateType.STRING));
        var rules=ValidationKeyMetadata.describe(model);
        assertThat(rules.fields()).isEmpty();assertThat(rules.unavailable()).containsOnlyKeys("binary","calendar","document","raw","nested.path");
        assertThat(rules.unavailable().get("raw")).isEqualTo("key-model-unknown");
        assertThat(rules.unavailable().get("binary")).isEqualTo("binary-key-subtype-unavailable");
    }
    private static SourceTable model(SourceField... fields){return new SourceTable("records",List.of(fields),List.of(),List.of());}
}
