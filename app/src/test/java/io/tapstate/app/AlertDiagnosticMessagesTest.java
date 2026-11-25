package io.tapstate.app;

import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.SourceBindingIdentity;
import io.tapstate.spi.store.SourceBindingSnapshot;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertDiagnosticMessagesTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void registryOnlyRedactionRemainsVisibleInDiagnosticMetadata(boolean sourceFailure) {
        var redactor=new SecretRedactor();redactor.replace("source",List.of("unlabelled-secret"));
        var messages=new AlertDiagnosticMessages(redactor);
        var failure=new io.tapstate.core.lifecycle.ObservationFailure(io.tapstate.runtime.engine.EngineError.JOB_FAILED.code(),
                Map.of("pipeline","orders","cause","connection reset for unlabelled-secret"));
        var trace=(sourceFailure?messages.source(messages.snapshot(source(Map.of())),new java.net.ConnectException("connection reset for unlabelled-secret"))
                :messages.pipeline(null,failure)).orElseThrow();
        assertThat(trace.text()).doesNotContain("unlabelled-secret");
        assertThat(trace.redacted()).isTrue();
    }
    @Test void aClippedReportedArgumentIsNotExportedAsAnUnrecognizableSecretPrefix() {
        var failure=new io.tapstate.core.lifecycle.ObservationFailure(io.tapstate.runtime.engine.EngineError.JOB_FAILED.code(),
                Map.of("pipeline","orders","cause","private-prefix".repeat(10)+" …"));
        assertThat(new AlertDiagnosticMessages(new SecretRedactor()).pipeline(null,failure)).isEmpty();
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void reportedMessagesAlwaysPassTheSameCredentialSanitization(boolean liveCause) {
        var failure=new io.tapstate.core.lifecycle.ObservationFailure(io.tapstate.runtime.engine.EngineError.JOB_FAILED.code(),
                Map.of("pipeline","orders","cause","connection failed password=old-private-value"));
        var trace=new AlertDiagnosticMessages(new SecretRedactor()).pipeline(liveCause?new IllegalStateException("unclassified"):null,failure).orElseThrow();
        assertThat(trace.text()).contains("connection failed").doesNotContain("old-private-value");
    }
    private static SourceBindingSnapshot source(Map<String,Object> config) {
        return new SourceBindingSnapshot(new SourceResource("erp",null,"test",config,null,null,null,null),"a".repeat(64),
                new SourceBindingIdentity("erp","e6e8d616-985e-49cc-a0f4-9802897abf95",2));
    }
    @Test void originalSourceValuesStayRedactedAfterTheLiveSecretRegistryChanges() {
        var redactor=new SecretRedactor();redactor.replace("erp",List.of("original-secret"));redactor.remove("erp");
        var messages=new AlertDiagnosticMessages(redactor);
        var trace=messages.source(messages.snapshot(source(Map.of("password","original-secret"))),
                new java.net.SocketException("Connection reset for original-secret")).orElseThrow();
        assertThat(trace.text()).contains("Connection reset").doesNotContain("original-secret");
        assertThat(trace.redacted()).isTrue();
    }
    @Test void arbitraryDriverMessagesAreNotPromotedToSafeNetworkDiagnostics() {
        var messages=new AlertDiagnosticMessages(new SecretRedactor());
        var trace=messages.source(messages.snapshot(source(Map.of())),
                new java.sql.SQLException("private row material","08006")).orElseThrow();
        assertThat(trace.text()).contains("SQLException").doesNotContain("private row material");
    }
    @Test void incompleteOriginalSecretCoverageSuppressesMessages() {
        var values=new LinkedHashMap<String,Object>();for(int i=0;i<65;i++)values.put("key"+i,"value"+i);
        var messages=new AlertDiagnosticMessages(new SecretRedactor());
        var trace=messages.source(messages.snapshot(source(values)),
                new java.net.SocketException("unclassified private text")).orElseThrow();
        assertThat(trace.text()).doesNotContain("unclassified private text");
    }
    @Test void pipelineThrowableCaptureKeepsItsOriginalCodeOnlyMessagePolicy() {
        var failure=new io.tapstate.core.common.TapstateException(io.tapstate.runtime.engine.EngineError.JOB_FAILED,
                Map.of("pipeline","private-argument","cause","private-cause"),new IllegalStateException("private-driver-message"));
        var trace=new AlertDiagnosticMessages(new SecretRedactor()).pipeline(failure,null).orElseThrow();
        assertThat(trace.text()).contains(io.tapstate.runtime.engine.EngineError.JOB_FAILED.code())
                .doesNotContain("private-argument","private-cause","private-driver-message");
    }
}
