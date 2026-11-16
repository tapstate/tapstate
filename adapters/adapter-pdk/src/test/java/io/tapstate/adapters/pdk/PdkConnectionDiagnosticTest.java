package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.error.CoreException;
import io.tapdata.exception.TapCodeException;
import io.tapdata.exception.TapPdkUserPwdInvalidEx;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PdkConnectionDiagnosticTest {
    private static final byte[] RESOURCE = """
            {"formatVersion":1,"connectorId":"fixture","definitions":[
              {"rawCode":"043","code":"EXT043","en":{"message":"Access denied","dynamic":"Account {}"}},
              {"rawCode":"10002","code":"EXT10002","en":{"message":"Connector-defined code"}},
              {"rawCode":"28P01","code":"EXT28P01","en":{"message":"Connector-defined code"}}
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
        assertThat(nested.get("codeSource")).isEqualTo("connector");
        assertThat(nested.get("message")).isEqualTo("Access denied\nAccount test account");
        assertThat(nested.get("arguments")).isEqualTo(List.of("test account"));
        assertThat(nested.get("rawMessage")).isEqualTo("Driver denied");
        assertThat(nested.containsKey("definitionStatus")).isFalse();
    }

    @Test
    void typedPdkAndJdbcCodesTakePrecedenceOverCollidingConnectorDefinitions() throws Exception {
        var catalog = PdkDiagnosticCatalog.read("fixture", RESOURCE);
        var jdbc = new SQLException("Rejected", "28P01");
        var pdk = new TapPdkUserPwdInvalidEx("fixture", "account", jdbc);

        var diagnostic = PdkConnectionDiagnostic.capture(pdk, "Rejected", null, pdk.getCode(), catalog, Locale.ENGLISH);
        assertThat(diagnostic).containsEntry("code", "10002").containsEntry("codeSource", "pdk");
        assertThat((Map<?, ?>) diagnostic.get("cause"))
                .isEqualTo(PdkConnectionDiagnostic.capture(jdbc, null, null, jdbc.getSQLState(), catalog, Locale.ENGLISH));
        var cause = (Map<?, ?>) diagnostic.get("cause");
        assertThat(cause.get("code")).isEqualTo("28P01");
        assertThat(cause.get("codeSource")).isEqualTo("jdbc-sqlstate");

        var opaque = new TapTestItemException(new TapCodeException("410003", "Original detail"));
        assertThat(PdkConnectionDiagnostic.capture(opaque, opaque.getMessage(), null, opaque.getErrorCode(),
                catalog, Locale.ENGLISH)).containsEntry("code", "410003").doesNotContainKey("codeSource");
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
