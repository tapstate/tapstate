package io.tapstate.core.alerting;

import static org.assertj.core.api.Assertions.*;
import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class AlertTargetLimitTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlertTargetLimit.Domain DOMAIN = new AlertTargetLimit.Domain(new AlarmKey.Scope("org", "cluster"), "webhook", "semantic-endpoint-1", "auth-principal-1");
    @Test void absoluteRestrictionsOnlyIncreaseAndReplaysDoNotSlideTheClock() {
        var original = new AlertTargetLimit(DOMAIN, T.plusSeconds(120));
        assertThat(original.observe(DOMAIN, T.plusSeconds(120))).isSameAs(original);
        assertThat(original.observe(DOMAIN, T.plusSeconds(60))).isSameAs(original);
        var longer = original.observe(DOMAIN, T.plusSeconds(300));
        assertThat(longer.notBefore()).isEqualTo(T.plusSeconds(300));
        assertThat(longer.merge(original)).isSameAs(longer);
        assertThat(original.merge(longer).notBefore()).isEqualTo(longer.notBefore());
    }
    @Test void newIntentRecoveryManualRetryOrTestCannotBypassTheSameTargetOrExtendOriginalDeadline() {
        var limit = new AlertTargetLimit(DOMAIN, T.plusSeconds(120));
        assertThat(limit.nextPermitAt(T, T.plusSeconds(60))).isEmpty();
        assertThat(limit.nextPermitAt(T.plusSeconds(10), T.plusSeconds(120))).isEmpty();
        assertThat(limit.nextPermitAt(T.plusSeconds(10), T.plusSeconds(300))).contains(T.plusSeconds(120));
        assertThat(limit.nextPermitAt(T.plusSeconds(121), T.plusSeconds(300))).contains(T.plusSeconds(121));
        assertThat(limit.restrictionElapsed(T.plusSeconds(119))).isFalse();
        assertThat(limit.restrictionElapsed(T.plusSeconds(120))).isTrue();
        var rebound = new AlertTargetLimit.Domain(DOMAIN.scope(), DOMAIN.channelType(), DOMAIN.semanticTargetId(), DOMAIN.authenticationIdentity());
        assertThat(rebound).isEqualTo(DOMAIN);
        assertThat(limit.observe(rebound, T.plusSeconds(100))).isSameAs(limit);
    }
    @Test void scopeChannelTargetAndAuthenticationIdentityNeverShareAnUnrelatedLimit() {
        var limit = new AlertTargetLimit(DOMAIN, T.plusSeconds(120));
        for (var foreign : new AlertTargetLimit.Domain[]{
                new AlertTargetLimit.Domain(new AlarmKey.Scope("other", "cluster"), "webhook", "semantic-endpoint-1", "auth-principal-1"),
                new AlertTargetLimit.Domain(DOMAIN.scope(), "email", "semantic-endpoint-1", "auth-principal-1"),
                new AlertTargetLimit.Domain(DOMAIN.scope(), "webhook", "semantic-endpoint-2", "auth-principal-1"),
                new AlertTargetLimit.Domain(DOMAIN.scope(), "webhook", "semantic-endpoint-1", "auth-principal-2")}) {
            assertThat(foreign).isNotEqualTo(DOMAIN);
            assertThatThrownBy(() -> limit.observe(foreign, T.plusSeconds(300))).isInstanceOf(TapstateException.class);
        }
    }
}
