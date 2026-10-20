package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.context.SecurityContextHolder;
import static org.assertj.core.api.Assertions.*;

class StorageControlCallersTest {
    private static final CacheAuthScope ORIGINAL = new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP,
            "8a5bbd9e-e330-4238-a001-98f5f104ef2c", 1);
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation",
            "c3a8a189-2177-49e1-bbf4-6d8a9e1cf65e", 1);

    @AfterEach void clear() { SecurityContextHolder.clearContext(); }

    @Test void selectedRequestKeepsItsOriginalScopeAndRechecksItsOriginalCredential() {
        var active = new AtomicBoolean(true);
        var verified = new CredentialAuthentication(new VerifiedToken("alice", Scope.ADMIN), Optional.of(ORIGINAL),
                Optional.empty(), () -> {
                    if (!active.get()) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
                });
        var credentials = CredentialAuthenticator.scoped(raw -> "credential".equals(raw) ? Optional.of(verified) : Optional.empty());
        var selected = new TapstateCredentialAuthenticationProvider(credentials)
                .authenticate(new TapstateCredentialAuthenticationToken("credential"));
        SecurityContextHolder.getContext().setAuthentication(selected);
        var caller = StorageControlCallers.ordinary(StorageBinding.Mode.OP, () -> SETUP).current();
        assertThat(caller.cache()).isEqualTo(ORIGINAL);
        assertThat(selected.getCredentials()).isEqualTo("");
        active.set(false);
        assertThatThrownBy(() -> caller.guard(ControlOperations.STORAGE_RECOVERY_PLAN).run())
                .isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException)error).code()).isEqualTo(ControlError.UNAUTHENTICATED);
    }

    @Test void authenticatedButUnscopedJwtCannotBorrowCurrentConfigurationAuthority() {
        var verified = new VerifiedToken("alice", Scope.ADMIN);
        var credentials = new CredentialAuthenticator(raw -> Optional.of(verified));
        SecurityContextHolder.getContext().setAuthentication(new TapstateCredentialAuthenticationProvider(credentials)
                .authenticate(new TapstateCredentialAuthenticationToken("old-jwt")));
        assertThatThrownBy(() -> StorageControlCallers.ordinary(StorageBinding.Mode.OP, () -> SETUP).current())
                .isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException)error).code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE);
    }

    @Test void unavailableAuthenticationDependencyUsesTheSecurityFailureChannelWithoutItsDriverCause() throws Exception {
        var credentials = new CredentialAuthenticator(raw -> {
            throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), new IllegalStateException("secret-driver-detail"));
        });
        var failure = catchThrowable(() -> new TapstateCredentialAuthenticationProvider(credentials)
                .authenticate(new TapstateCredentialAuthenticationToken("credential")));
        assertThat(failure).isInstanceOf(org.springframework.security.authentication.AuthenticationServiceException.class);
        assertThat(failure.getCause()).isNull();
        assertThat(failure.toString()).doesNotContain("secret-driver-detail");
        var response = new org.springframework.mock.web.MockHttpServletResponse();
        new CodedAuthenticationEntryPoint(new ApiSecurityErrorWriter(io.tapstate.messages.MessageCatalog.bundled(),
                tools.jackson.databind.json.JsonMapper.builder().build())).commence(
                new org.springframework.mock.web.MockHttpServletRequest(), response,
                (org.springframework.security.core.AuthenticationException)failure);
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getContentAsString()).contains("control.storage-unavailable").doesNotContain("secret-driver-detail");
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
    }

    @Test void verifiedCloudSetupIdentityCanReadSetupButCannotBorrowAFormalCacheScope() {
        var verified = new CredentialAuthentication(new VerifiedToken("cloud-user", Scope.WRITE), Optional.empty(),
                Optional.of("organization"), () -> {});
        var credentials = CredentialAuthenticator.scoped(raw -> Optional.of(verified));
        SecurityContextHolder.getContext().setAuthentication(new TapstateCredentialAuthenticationProvider(credentials)
                .authenticate(new TapstateCredentialAuthenticationToken("tcs_setup-cookie")));
        var caller = StorageControlCallers.ordinary(StorageBinding.Mode.CLOUD, () -> SETUP).current();
        caller.require(ControlOperations.STORAGE_STATUS);
        assertThatThrownBy(caller::cache).isInstanceOf(TapstateException.class);
    }
}
