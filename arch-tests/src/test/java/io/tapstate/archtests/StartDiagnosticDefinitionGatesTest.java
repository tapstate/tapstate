package io.tapstate.archtests;

import io.tapstate.control.core.StartCheckCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.messages.LifecycleDiagnosticCatalog;
import io.tapstate.messages.StartCheckDiagnosticCatalog;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StartDiagnosticDefinitionGatesTest {
    @Test
    void refusalsAndReportTextKeepIndependentSeverityAndHistoricalMeaning() {
        verify(LifecycleError.values(), "lifecycle", "TS028", Severity.ERROR);
        verify(StartCheckCode.values(), "start-check", "TS029", Severity.WARNING);
    }

    private static void verify(TapstateErrorCode[] owners, String domain, String prefix, Severity severity) {
        var catalog = MessageCatalog.bundled();
        for (int i = 0; i < owners.length; i++) {
            var owner = owners[i];
            String historical = domain + "." + ((Enum<?>) owner).name().toLowerCase(Locale.ROOT).replace('_', '-');
            assertThat(owner.code()).isEqualTo(prefix + String.format("%04d", i + 1));
            assertThat(owner.severity()).isEqualTo(severity);
            Map<String, Object> args = new HashMap<>();
            owner.placeholders().forEach(name -> args.put(name, "value-" + name));
            var before = catalog.render(historical, args);
            var after = catalog.render(owner.code(), args);
            assertThat(after.message()).isEqualTo(before.message());
            if (before.solution() != null) assertThat(after.solution()).isEqualTo(before.solution());
            var lookup = domain.equals("lifecycle") ? LifecycleDiagnosticCatalog.bundled() : StartCheckDiagnosticCatalog.bundled();
            var messages = new HashSet<String>();
            for (var locale : List.of(Locale.ENGLISH, Locale.SIMPLIFIED_CHINESE, Locale.TRADITIONAL_CHINESE)) {
                var resolved = lookup.resolve(owner.code(), args, List.of(), locale).orElseThrow();
                messages.add(resolved.message());
                assertThat(resolved.solution()).isNotBlank();
                owner.placeholders().forEach(name -> assertThat(resolved.message() + resolved.solution())
                        .contains("value-" + name).doesNotContain("{" + name + "}"));
            }
            assertThat(messages).hasSize(3);
            assertThat(lookup.resolve(historical, args, List.of(), Locale.ENGLISH)).isEmpty();
            assertThat(lookup.resolve(owner.code(), Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        }
    }
}
