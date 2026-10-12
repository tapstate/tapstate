package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TapstatePrincipalTest {

    @Test
    void aHumanJwtCarriesItsSubjectAndEveryImpliedScopeWithoutATokenId() {
        TapstatePrincipal principal = TapstatePrincipal.humanJwt(new VerifiedToken("alice", Scope.WRITE));

        assertThat(principal.subject()).isEqualTo("alice");
        assertThat(principal.credentialType()).isEqualTo(CredentialType.HUMAN_JWT);
        assertThat(principal.scopes()).containsExactlyInAnyOrder(Scope.READ, Scope.WRITE);
        assertThat(principal.tokenId()).isEmpty();
        assertThat(principal.permits(Scope.READ)).isTrue();
        assertThat(principal.permits(Scope.ADMIN)).isFalse();
    }

    @Test
    void aMachineTokenCarriesTheTokenIdAndEveryImpliedScope() {
        TapstatePrincipal principal = TapstatePrincipal.machineToken(new VerifiedToken("tok-17", Scope.ADMIN));

        assertThat(principal.subject()).isEqualTo("tok-17");
        assertThat(principal.credentialType()).isEqualTo(CredentialType.MACHINE_TOKEN);
        assertThat(principal.scopes()).containsExactlyInAnyOrder(Scope.READ, Scope.WRITE, Scope.ADMIN);
        assertThat(principal.tokenId()).contains("tok-17");
        assertThat(principal.permits(Scope.WRITE)).isTrue();
    }

    @Test
    void aLocalCloudSessionCarriesTheVerifiedUserWithoutAMachineTokenOrJwtClassification() {
        TapstatePrincipal principal = TapstatePrincipal.cloudSession(new VerifiedToken("stable-cloud-user", Scope.WRITE));
        assertThat(principal.subject()).isEqualTo("stable-cloud-user");
        assertThat(principal.credentialType()).isEqualTo(CredentialType.CLOUD_SESSION);
        assertThat(principal.scopes()).containsExactlyInAnyOrder(Scope.READ, Scope.WRITE);
        assertThat(principal.tokenId()).isEmpty();
        assertThat(principal.permits(Scope.ADMIN)).isFalse();
    }
}
