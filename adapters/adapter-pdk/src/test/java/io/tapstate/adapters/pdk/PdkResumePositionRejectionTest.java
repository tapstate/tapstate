package io.tapstate.adapters.pdk;

import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapstate.core.common.TapstateException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PdkResumePositionRejectionTest {
    @Test
    void anExplicitSdkLogWindowRefusalRetainsItsRequestedOffsetAndEverySourceArgument() {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put("resume", "original-retained-token");
        offset.put("partition", 7L);
        Throwable source = new IllegalStateException("source history no longer contains the requested point");
        TapPdkOffsetOutOfLogEx refusal = new TapPdkOffsetOutOfLogEx("mongodb", offset, source);
        refusal.withServerErrorCode("286");
        refusal.dynamicDescriptionParameters("original argument", "source detail that must remain unchanged");
        TapstateException coded = PdkCapturePort.captureFailure("mongodb", refusal);
        assertThat(coded.code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED);
        assertThat(coded.args()).containsEntry("requested", ConnectorOffsetCodec.toToken("mongodb", offset))
                .containsEntry("pdkId", refusal.getPdkId()).containsEntry("pdkCode", refusal.getCode())
                .containsEntry("serverCode", "286")
                .containsEntry("pdkArgs", List.of("original argument", "source detail that must remain unchanged"));
        assertThat(coded.getCause()).isSameAs(refusal);
        assertThat(refusal.getCause()).isSameAs(source);
    }

    @Test
    void aConnectorLoaderUsesTheExactPreservedHostSdkExceptionType() throws Exception {
        Path sdk = Path.of(TapPdkOffsetOutOfLogEx.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(List.of(sdk))) {
            assertThat(loader.load(TapPdkOffsetOutOfLogEx.class.getName())).isSameAs(TapPdkOffsetOutOfLogEx.class);
        }
    }

    @Test
    void aWrappedSdkRefusalPreservesTheOriginalCauseChain() {
        TapPdkOffsetOutOfLogEx refusal = new TapPdkOffsetOutOfLogEx("mongodb", Map.of("resume", "retained"), null);
        RuntimeException original = new RuntimeException("connector wrapper", refusal);
        TapstateException coded = PdkCapturePort.captureFailure("mongodb", original);
        assertThat(coded.code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED);
        assertThat(coded.getCause()).isSameAs(original);
        assertThat(coded.args()).containsEntry("serverCode", null);
    }

    @Test
    void aGenericFaultWithResumeWordsDoesNotBecomeADeclaredSourceRejection() {
        RuntimeException original = new RuntimeException("resume token is missing or expired");
        TapstateException coded = PdkCapturePort.captureFailure("mongodb", original);
        assertThat(coded.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
        assertThat(coded.getCause()).isSameAs(original);
    }

    @Test
    void aRetainedRequestSurvivesAnSdkOffsetThatCannotBeRenderedAgain() {
        TapPdkOffsetOutOfLogEx refusal = new TapPdkOffsetOutOfLogEx("mongodb", new Object(), null);
        refusal.dynamicDescriptionParameters("the original source argument");
        TapstateException coded = PdkCapturePort.captureFailure("mongodb", refusal, "original-requested-token");
        assertThat(coded.code()).isEqualTo(ConnectorError.RESUME_POSITION_REJECTED);
        assertThat(coded.args()).containsEntry("requested", "original-requested-token")
                .containsEntry("pdkArgs", List.of("the original source argument"));
        assertThat(coded.getCause()).isSameAs(refusal);
    }
}
