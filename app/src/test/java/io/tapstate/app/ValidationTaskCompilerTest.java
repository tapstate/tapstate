package io.tapstate.app;

import io.tapstate.adapters.pdk.PdkValidationCounts;
import io.tapstate.core.validation.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationTaskCompilerTest {
    @Test void evaluationIdentityChangesWithTheArtifactMethodAndNativeProtocolEvenWhenCountSpecDoesNot(){
        var comparison=new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"source","b".repeat(64),"target","c".repeat(64)),1,ValidationTaskCompiler.RULES_VERSION,
                List.of(new ValidationTaskSpec.Mapping("mapping",ValidationTaskSpec.OutputKind.SYNC,"out",
                        new ValidationTaskSpec.Table("source","source","db",null,"records",null,null),
                        new ValidationTaskSpec.Table("out","target","db",null,"checked_records",null,null),null)));
        var source=new NativeValidationMetadata.Side(null,PdkValidationCounts.Operation.PARTITION_FILTER,"d".repeat(64));
        var target=new NativeValidationMetadata.Side(null,PdkValidationCounts.Operation.DOCUMENT_COUNT,"e".repeat(64));
        var original=List.of(new NativeValidationMetadata.Mapping("mapping",source,target));
        var fingerprint=ValidationTaskCompiler.fingerprint(comparison,ValidationMethod.COUNT,original);
        assertThat(ValidationTaskCompiler.fingerprint(comparison,ValidationMethod.COUNT,original)).isEqualTo(fingerprint);
        var changedArtifact=new NativeValidationMetadata.Side(null,source.countOperation(),"f".repeat(64));
        assertThat(ValidationTaskCompiler.fingerprint(comparison,ValidationMethod.COUNT,List.of(new NativeValidationMetadata.Mapping("mapping",changedArtifact,target)))).isNotEqualTo(fingerprint);
        var changedProtocol=new NativeValidationMetadata.Side(null,PdkValidationCounts.Operation.DOCUMENT_COUNT,source.artifactHash());
        assertThat(ValidationTaskCompiler.fingerprint(comparison,ValidationMethod.COUNT,List.of(new NativeValidationMetadata.Mapping("mapping",changedProtocol,target)))).isNotEqualTo(fingerprint);
        assertThat(ValidationTaskCompiler.fingerprint(comparison,ValidationMethod.FULL_COLUMN,original)).isNotEqualTo(fingerprint);
    }
}
