package io.tapstate.control.core;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CredentialEvidenceTest {
    @Test void legacyContextsDoNotAcquireProvenanceByGuessingTheSubject() {
        var context = new CredentialAuthentication(new VerifiedToken("token-looking-name", Scope.ADMIN),
                Optional.empty(), Optional.empty(), () -> {});
        assertThat(context.evidence()).isEmpty();
    }
    @Test void machineEvidenceRequiresItsActualRecordBindingAndHumanEvidenceCannotBorrowOne() {
        assertThatThrownBy(() -> new CredentialEvidence(CredentialType.MACHINE_TOKEN, Optional.empty(), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialEvidence(CredentialType.HUMAN_JWT, Optional.of("a".repeat(64)), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CredentialEvidence(CredentialType.CLOUD_SESSION, Optional.of("a".repeat(64)), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        var calls = new AtomicInteger();
        var evidence = new CredentialEvidence(CredentialType.MACHINE_TOKEN, Optional.of("a".repeat(64)), calls::incrementAndGet);
        evidence.requireLocallyActive().run();
        assertThat(calls).hasValue(1);
        assertThat(evidence.toString()).doesNotContain("a".repeat(64));
    }
}
