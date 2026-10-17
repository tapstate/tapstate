package io.tapstate.adapters.mongostore;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A saved endpoint keeps its database, authentication and per-connection trust without temporary files. */
class MongoBoundConnectionTest {
    @Test
    void theExplicitDatabaseDoesNotChangeTheUrisAuthenticationSourceOrOptions() {
        String uri = "mongodb://user:password@localhost:27017/uri_auth?tls=true&authSource=original_auth&retryWrites=false";
        try (var connection = cache(uri, StorageBinding.Connection.Kind.MONGODB, null)) {
            ConnectionString parsed = new ConnectionString(uri);
            var settings = connection.buildClientSettings(parsed);
            assertThat(connection.selectedDatabase(parsed)).isEqualTo("cache_selected");
            assertThat(settings.getCredential().getSource()).isEqualTo("original_auth");
            assertThat(settings.getRetryWrites()).isFalse();
            assertThat(settings.getSslSettings().isEnabled()).isTrue();
            assertThat(settings.getSslSettings().isInvalidHostNameAllowed()).isFalse();
        }
    }

    @Test
    void aUsersAtlasCredentialIsPinnedToScramSha256WithoutDroppingItsAuthSource() {
        String uri = "mongodb://user:password@localhost/uri_auth?tls=true&authSource=admin";
        var settings = settings(uri, StorageBinding.Connection.Kind.ATLAS, null);
        assertThat(settings.getCredential().getAuthenticationMechanism()).isEqualTo(AuthenticationMechanism.SCRAM_SHA_256);
        assertThat(settings.getCredential().getSource()).isEqualTo("admin");
        assertThat(settings.getCredential().getUserName()).isEqualTo("user");
    }

    @Test
    void srvTxtAuthenticationAndDefaultTlsSurviveTheExplicitDatabaseSelection() {
        String uri = "mongodb+srv://user:password@cluster.example.net/uri_auth";
        ConnectionString parsed = new ConnectionString(uri, (name, type) -> List.of("authSource=txt_auth"));
        try (var connection = cache(uri, StorageBinding.Connection.Kind.MONGODB, null)) {
            var settings = connection.buildClientSettings(parsed);
            assertThat(connection.selectedDatabase(parsed)).isEqualTo("cache_selected");
            assertThat(settings.getSslSettings().isEnabled()).isTrue();
            assertThat(settings.getCredential().getSource()).isEqualTo("txt_auth");
            assertThat(settings.getCredential().getAuthenticationMechanism()).isNull();
        }
    }

    @Test
    void aUsersAtlasSrvConnectionKeepsAdminTxtAndPinsSha256() {
        String uri = "mongodb+srv://user:password@cluster.example.net/uri_auth";
        ConnectionString parsed = new ConnectionString(uri, (name, type) -> List.of("authSource=admin"));
        try (var connection = cache(uri, StorageBinding.Connection.Kind.ATLAS, null)) {
            var settings = connection.buildClientSettings(parsed);
            assertThat(settings.getSslSettings().isEnabled()).isTrue();
            assertThat(settings.getCredential().getSource()).isEqualTo("admin");
            assertThat(settings.getCredential().getAuthenticationMechanism()).isEqualTo(AuthenticationMechanism.SCRAM_SHA_256);
        }
    }

    @Test
    void platformConfigurationAuthenticationIsPreservedInsteadOfApplyingTheUserAtlasPolicy() {
        String uri = "mongodb://user:password@localhost/configuration?tls=true&authSource=original_auth&authMechanism=SCRAM-SHA-1";
        try (var connection = MongoConnection.forConfiguration(new MongoConnectionSettings(uri, null, Duration.ofSeconds(2)))) {
            var settings = connection.buildClientSettings(new ConnectionString(uri));
            assertThat(settings.getCredential().getAuthenticationMechanism()).isEqualTo(AuthenticationMechanism.SCRAM_SHA_1);
            assertThat(settings.getCredential().getSource()).isEqualTo("original_auth");
        }
    }

    @Test
    void safeDriverOptionsRemainEffectiveWithSemicolonAndMixedSeparators() {
        String uri = "mongodb://localhost/cache?tls=true;retryWrites=false&appName=bound-client";
        var settings = settings(uri, StorageBinding.Connection.Kind.MONGODB, null);
        assertThat(settings.getSslSettings().isEnabled()).isTrue();
        assertThat(settings.getRetryWrites()).isFalse();
        assertThat(settings.getApplicationName()).isEqualTo("bound-client tapstate/boot-a");
    }

    @Test
    void publicCaUsesTheNormalJvmTrustWithoutAClientKey() {
        var settings = settings("mongodb://localhost/cache?tls=true", StorageBinding.Connection.Kind.MONGODB, null);
        assertThat(settings.getSslSettings().getContext()).isNull();
        assertThat(settings.getSslSettings().isInvalidHostNameAllowed()).isFalse();
    }

    @Test
    void caContentsRemainPerConnectionAndDoNotChangeTheJvmDefaultContext() throws Exception {
        String ca = ca();
        SSLContext original = SSLContext.getDefault();
        var first = settings("mongodb://localhost/cache?tls=true", StorageBinding.Connection.Kind.MONGODB, ca);
        var second = settings("mongodb://localhost/cache?tls=true", StorageBinding.Connection.Kind.MONGODB, ca);
        assertThat(first.getSslSettings().getContext()).isNotNull().isNotSameAs(second.getSslSettings().getContext());
        assertThat(first.getSslSettings().isInvalidHostNameAllowed()).isFalse();
        assertThat(SSLContext.getDefault()).isSameAs(original);
    }

    @Test
    void aConfigurationCaFileIsAvailableBeforeAnyConfigurationDatabaseRead() throws Exception {
        String uri = "mongodb://localhost/configuration?tls=true";
        String path = Path.of(getClass().getResource("/tls/ca.pem").toURI()).toString();
        try (var connection = MongoConnection.forConfiguration(new MongoConnectionSettings(uri, path, Duration.ofSeconds(2)))) {
            assertThat(connection.buildClientSettings(new ConnectionString(uri)).getSslSettings().getContext()).isNotNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"tlsInsecure=true", "tlsAllowInvalidHostnames=true", "sslInvalidHostNameAllowed=true",
            "tlsAllowInvalidCertificates=true", "tls%49nsecure=true", "tlsInsecure=false&tlsInsecure=true",
            "tlsCertificateKeyFile=client.pem", "tlsCertificateKeyFilePassword=private-sentinel", "tlsCAFile=local.pem",
            "tls=true;tlsCertificateKeyFilePassword=private-sentinel", "tls=true;tlsCAFile=local.pem",
            "retryWrites=true;tlsCertificateKeyFile=client.pem&appName=test", "tls=false;tlsAllowInvalidCertificates=true"})
    void insecureOrClientKeyUriOptionsAreRejectedWithoutEchoingSecrets(String option) {
        assertThatThrownBy(() -> cache("mongodb://user:password-sentinel@localhost/cache?tls=true&" + option,
                StorageBinding.Connection.Kind.MONGODB, null)).isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(StoreError.STORAGE_CONNECTION_UNSUPPORTED);
                    assertThat(error.getMessage()).doesNotContain("password-sentinel", "private-sentinel", "client.pem");
                    assertThat(error.getCause()).isNull();
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"mongodb://user:password@localhost/cache?tls=false",
            "mongodb://localhost/cache?tls=true",
            "mongodb://user:password@localhost/cache?tls=true&authMechanism=SCRAM-SHA-1",
            "mongodb://user:password@localhost/cache?tls=true&authSource=not_admin"})
    void aUsersAtlasConnectionCannotUsePlaintextMissingCredentialsOrSha1(String uri) {
        assertThatThrownBy(() -> settings(uri, StorageBinding.Connection.Kind.ATLAS, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.STORAGE_CONNECTION_UNSUPPORTED));
    }

    @Test
    void aCaBundleCannotBeSilentlyIgnoredForAPlaintextBinding() throws Exception {
        assertThatThrownBy(() -> settings("mongodb://localhost/cache", StorageBinding.Connection.Kind.MONGODB, ca()))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.STORAGE_CONNECTION_UNSUPPORTED));
    }

    @Test
    void invalidOrPrivateTrustMaterialIsCodedWithoutIncludingItsContents() throws Exception {
        for (String pem : List.of("invalid-trust-sentinel", ca() + "\n-----BEGIN PRIVATE KEY-----\nprivate-sentinel\n-----END PRIVATE KEY-----")) {
            assertThatThrownBy(() -> settings("mongodb://localhost/cache?tls=true", StorageBinding.Connection.Kind.MONGODB, pem))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(StoreError.TLS_TRUST_INVALID);
                        assertThat(error.getMessage()).doesNotContain("sentinel", "BEGIN");
                        assertThat(error.getCause()).isNull();
                    });
        }
    }

    @Test
    void newConfigurationConnectionsRequireAnExplicitDatabaseAndStrictPeerVerification() {
        try (var connection = MongoConnection.forConfiguration(new MongoConnectionSettings("mongodb://localhost", null, Duration.ofSeconds(2)))) {
            assertThatThrownBy(() -> connection.selectedDatabase(new ConnectionString("mongodb://localhost")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.STORAGE_CONNECTION_UNSUPPORTED));
        }
        assertThatThrownBy(() -> MongoConnection.forConfiguration(new MongoConnectionSettings(
                "mongodb://localhost/configuration?tls=true&tlsAllowInvalidHostnames=true", null, Duration.ofSeconds(2))))
                .isInstanceOf(TapstateException.class);
    }

    private static String ca() throws Exception {
        return Files.readString(Path.of(MongoBoundConnectionTest.class.getResource("/tls/ca.pem").toURI()));
    }

    private static MongoConnection cache(String uri, StorageBinding.Connection.Kind kind, String ca) {
        return MongoConnection.forCache(new StorageBinding.Connection(kind, uri, "cache_selected", ca), Duration.ofSeconds(2), "boot-a");
    }

    private static MongoClientSettings settings(String uri, StorageBinding.Connection.Kind kind, String ca) {
        try (var connection = cache(uri, kind, ca)) {
            return connection.buildClientSettings(new ConnectionString(uri));
        }
    }
}
