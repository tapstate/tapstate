package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class CloudCredentialBoundaryTest {

    @Test
    void aSelectedExternalVerifierIsTheOnlyCredentialAuthorityConsulted() {
        VerifiedToken cloud = new VerifiedToken("cloud-user-id", Scope.ADMIN);
        CredentialAuthenticator authenticator = new CredentialAuthenticator(
                credential -> credential.equals("cloud-jwt") ? Optional.of(cloud) : Optional.empty());

        assertThat(authenticator.authenticate("cloud-jwt")).contains(cloud);
        assertThat(authenticator.authenticate("cyxt_local-machine-token")).isEmpty();
        assertThat(authenticator.authenticate("local-signed-jwt")).isEmpty();
    }

    @Test
    void thePendingCloudBoundaryFailsClosed() {
        CredentialAuthenticator authenticator = CredentialAuthenticator.refusing();

        assertThat(authenticator.authenticate("anything")).isEmpty();
    }
}
