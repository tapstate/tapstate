package io.tapstate.archtests;

import static org.assertj.core.api.Assertions.assertThat;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertRetentionPolicy;
import io.tapstate.core.alerting.AlertTestCommand;
import io.tapstate.core.alerting.AlertTestQuota;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Locks the client command encoding and full logical identity independently of a persistence codec. */
class AlertCommandContractTest {
    @Test void canonicalTestIdentityAndFixedWindowsMatchThePublishedContract() throws IOException {
        var id = new AlertTestCommand.ClientId(Instant.parse("2026-10-21T00:00:00Z"), "00112233445566778899aabbccddeeff");
        var key = new AlertTestCommand.Key(new AlarmKey.Scope("org", "cluster"), "principal-1", id);
        String actual = "client-command-id=" + id.value() + "\n"
                + "test-id=" + key.testId() + "\n"
                + "admission-age-seconds=" + AlertTestCommand.ADMISSION_AGE.toSeconds() + "\n"
                + "future-skew-seconds=" + AlertTestCommand.FUTURE_SKEW.toSeconds() + "\n"
                + "actor-per-minute=" + AlertTestQuota.ACTOR_LIMIT + "\n"
                + "scope-per-minute=" + AlertTestQuota.SCOPE_LIMIT + "\n"
                + "retention-policy-version=" + AlertRetentionPolicy.POLICY_VERSION + "\n";
        try (var in = getClass().getResourceAsStream("/alert-test-command-v1.golden")) {
            assertThat(in).isNotNull();
            assertThat(actual).isEqualTo(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
