package io.tapstate.app;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.execution.TaskletExecutionException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.validation.ValidationError;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeValidationReportFailureTest {
    @Test void aJetWrapperRetainsTheFirstCodedCauseAndShowsItsHumanMessage(){
        var coded=new TapstateException(ValidationError.CODEC_VALUE_UNAVAILABLE,Map.of("connector","mongodb","field","amount"),null);
        var outer=new JetException("outer tasklet wrapper",new TaskletExecutionException("full-scan#0",coded));
        var failure=NativeValidationReportFailure.capture(outer,new SecretRedactor());
        assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE.code());
        assertThat(failure.params()).containsEntry("connector","mongodb").containsEntry("field","amount");
        assertThat(failure.message()).contains("mongodb","amount").doesNotContain("outer tasklet wrapper","TapstateException","codec-value-unavailable");
        assertThat(failure.exceptionType()).isEqualTo(JetException.class.getName());
        assertThat(failure.technical()).contains(JetException.class.getName(),TaskletExecutionException.class.getName());assertThat(failure.fatal()).isFalse();
    }
    @Test void registeredSecretSubstringsNeverReplaceTheCanonicalCode(){
        var redactor=new SecretRedactor();redactor.replace("source",List.of("validation","sentinel-password"));
        var coded=new TapstateException(ValidationError.CODEC_VALUE_UNAVAILABLE,Map.of("connector","mongodb","field","sentinel-password"),null);
        var failure=NativeValidationReportFailure.capture(new JetException("validation sentinel-password",coded),redactor);
        assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE.code());
        assertThat(failure.message()).doesNotContain("sentinel-password","validation");
        assertThat(failure.params().toString()).doesNotContain("sentinel-password");assertThat(failure.technical()).doesNotContain("sentinel-password","validation");
    }
    @Test void secretSubstringsMatchingPlaceholderNamesDoNotEraseTheHumanArguments(){
        var redactor=new SecretRedactor();redactor.replace("source",List.of("connector","field"));
        var coded=new TapstateException(ValidationError.CODEC_VALUE_UNAVAILABLE,Map.of("connector","mysql","field","amount"),null);
        var failure=NativeValidationReportFailure.capture(new JetException("carrier",coded),redactor);
        assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE.code());
        assertThat(failure.message()).contains("mysql","amount").doesNotContain("{","}");
        assertThat(failure.params().keySet()).doesNotContain("connector","field");
    }
    @Test void programmerFailuresAndSuppressedCleanupCodesAreNotPresentedAsDomainFailures(){
        var coded=new TapstateException(ValidationError.READ_FAILED,Map.of("table","orders"),null);
        var invariant=new IllegalStateException("broken invariant",coded);invariant.addSuppressed(coded);
        var failure=NativeValidationReportFailure.capture(invariant,new SecretRedactor());
        assertThat(failure.code()).isNull();assertThat(failure.params()).isEmpty();assertThat(failure.message()).isEqualTo("broken invariant");
        var fatal=NativeValidationReportFailure.capture(new AssertionError("broken assertion",coded),new SecretRedactor());
        assertThat(fatal.code()).isNull();assertThat(fatal.fatal()).isTrue();
    }
    @Test void aLaterCleanupFailureCannotReplaceThePrimaryCode(){
        var first=new TapstateException(ValidationError.READ_FAILED,Map.of("table","orders"),null);
        first.addSuppressed(new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","cleanup"),null));
        var failure=NativeValidationReportFailure.capture(new CompletionException(first),new SecretRedactor());
        assertThat(failure.code()).isEqualTo(ValidationError.READ_FAILED.code());assertThat(failure.params()).containsEntry("table","orders");
    }
    @Test void cyclesAndOverDepthChainsRemainBounded(){
        var left=new Cycle();var right=new Cycle();left.initCause(right);right.initCause(left);
        var cycle=NativeValidationReportFailure.capture(left,new SecretRedactor());assertThat(cycle.code()).isNull();assertThat(cycle.technical().length()).isLessThanOrEqualTo(4096);
        Throwable deep=new TapstateException(ValidationError.READ_FAILED,Map.of("table","orders"),null);
        for(int index=0;index<40;index++)deep=new CompletionException("wrapper",deep);
        var limited=NativeValidationReportFailure.capture(deep,new SecretRedactor());assertThat(limited.code()).isNull();assertThat(limited.message().length()).isLessThanOrEqualTo(1024);assertThat(limited.technical().length()).isLessThanOrEqualTo(4096);
    }
    private static final class Cycle extends CompletionException { Cycle(){super();} }
}
