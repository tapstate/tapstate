package io.tapstate.control.restapi;

import io.tapstate.control.core.CredentialAuthentication;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.VerifiedToken;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class StartCredentialAuthenticationTest {
    @Test void selectedStartAdmissionKeepsTheAuthenticatedPrincipalAndOrdinaryRequestsUseTheirVerifier() {
        var ordinary = new AtomicInteger();
        var starts = new AtomicInteger();
        var credentials = CredentialAuthenticator.scoped(raw -> {
            ordinary.incrementAndGet();
            return proof();
        }, raw -> {
            starts.incrementAndGet();
            return proof();
        });
        var provider = new TapstateCredentialAuthenticationProvider(credentials);
        var start = new TapstateCredentialAuthenticationToken("human-jwt");
        start.setDetails(CredentialAuthenticator.Request.PIPELINE_START);
        var accepted = provider.authenticate(start);
        assertThat(accepted.isAuthenticated()).isTrue();
        assertThat(starts).hasValue(1);
        assertThat(ordinary).hasValue(0);
        var other = new TapstateCredentialAuthenticationToken("human-jwt");
        provider.authenticate(other);
        assertThat(starts).hasValue(1);
        assertThat(ordinary).hasValue(1);
    }

    private static Optional<CredentialAuthentication> proof() {
        return Optional.of(new CredentialAuthentication(new VerifiedToken("admin", Scope.ADMIN),
                Optional.empty(), Optional.empty(), () -> {}));
    }
}
