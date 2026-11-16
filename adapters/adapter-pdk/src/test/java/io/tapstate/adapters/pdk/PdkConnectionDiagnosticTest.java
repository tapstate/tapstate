package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.error.CoreException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PdkConnectionDiagnosticTest {
    private static final byte[] RESOURCE = """
            {"formatVersion":1,"connectorId":"fixture","definitions":[
              {"rawCode":"043","code":"EXT043","en":{"message":"Access denied","dynamic":"Account {}"}}
            ]}
            """.getBytes(StandardCharsets.UTF_8);

    @Test
    void resolvesACodedNestedCauseAndKeepsItsOriginalDetails() throws Exception {
        var coded = new TapCodeException("043", "Driver denied");
        coded.dynamicDescriptionParameters("test account");
        var outer = new TapTestItemException();
        outer.initCause(coded);

        var nested = (Map<?, ?>) capture(outer).get("cause");

        assertThat(nested.get("code")).isEqualTo("EXT043");
        assertThat(nested.get("message")).isEqualTo("Access denied\nAccount test account");
        assertThat(nested.get("arguments")).isEqualTo(List.of("test account"));
        assertThat(nested.get("rawMessage")).isEqualTo("Driver denied");
        assertThat(nested.containsKey("definitionStatus")).isFalse();
    }

    @Test
    void aSingletonCommonDetailAugmentsTheExistingRawMessage() throws Exception {
        var group = new CoreException(7, "Common failure");
        group.coreException(new CoreException("Supplementary detail"));
        var outer = new TapTestItemException();
        outer.initCause(group);

        var nested = (Map<?, ?>) capture(outer).get("cause");

        assertThat(nested.get("rawMessage")).isEqualTo("Common failure\nSupplementary detail");
        assertThat(nested.get("code")).isEqualTo("7");
        assertThat(nested.get("definitionStatus")).isEqualTo("unverified");
        assertThat(nested.keySet()).noneMatch(key -> "cause".equals(key) || "causes".equals(key));
    }

    @Test
    void aRepeatedCauseIsOmittedAndTheRemainingGroupIsMarkedTruncated() throws Exception {
        var shared = new CoreException("Driver denied");
        var group = new CoreException(shared, "Connector wrapper");
        group.coreException(shared);
        var outer = new TapTestItemException();
        outer.initCause(group);

        var nested = (Map<?, ?>) capture(outer).get("cause");

        assertThat(nested.get("truncated")).isEqualTo(true);
        var remaining = (List<?>) nested.get("causes");
        assertThat(remaining).hasSize(1);
        assertThat(((Map<?, ?>) remaining.getFirst()).get("rawMessage")).isEqualTo("Driver denied");
    }

    @Test
    void aCycleIsMarkedTruncatedWithoutRecreatingTheCause() throws Exception {
        var first = new IllegalStateException("First");
        var second = new IllegalStateException("Second", first);
        first.initCause(second);
        var outer = new TapTestItemException();
        outer.initCause(first);

        var nested = (Map<?, ?>) capture(outer).get("cause");
        var tail = (Map<?, ?>) nested.get("cause");

        assertThat(tail.get("rawMessage")).isEqualTo("Second");
        assertThat(tail.get("truncated")).isEqualTo(true);
        assertThat(tail.containsKey("cause")).isFalse();
    }

    @Test
    void doesNotExposePartialStackTextOrShiftMissingPositionalArguments() throws Exception {
        var oversized = new TapTestItemException();
        oversized.setStack("prefix" + "x".repeat(4096));
        oversized.setDynamicDescriptionParameters(new String[] {null, "argument"});
        var noStack = new TapTestItemException();
        noStack.setStackTrace(new StackTraceElement[0]);

        assertThat(capture(oversized)).containsEntry("truncated", true)
                .doesNotContainKeys("stackTrace", "arguments");
        assertThat(capture(noStack)).doesNotContainKeys("stackTrace", "truncated");
    }

    private static Map<String, Object> capture(TapTestItemException failure) throws Exception {
        return PdkConnectionDiagnostic.capture(failure, "Connection failed", null, null,
                PdkDiagnosticCatalog.read("fixture", RESOURCE), Locale.ENGLISH);
    }
}
