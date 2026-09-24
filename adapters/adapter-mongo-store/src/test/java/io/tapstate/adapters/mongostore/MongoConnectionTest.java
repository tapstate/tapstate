package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConfigurationException;
import io.tapstate.core.common.TapstateException;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The connection substrate's fast-fail behavior and TLS wiring — no Docker required. TLS is opt-in:
 * the connection is plaintext by default and uses TLS only when the URI asks for it ({@code ssl=true}
 * / {@code tls=true}), with the URI's own settings honored as-is. Pointed at a dead port it reports
 * the store unreachable as a coded diagnostic (not a bare driver exception). The real replica-set
 * and standalone checks plus the CAS witness live in the Testcontainers integration tests; the
 * mongos hello marker is checked here until a sharded test deployment is available.
 */
class MongoConnectionTest {

    @Test
    void unreachableTargetIsReportedAsACodedDiagnostic() {
        MongoConnectionSettings settings = new MongoConnectionSettings(
                "mongodb://localhost:1/tapstate", null, Duration.ofMillis(300));
        try (MongoConnection connection = new MongoConnection(settings)) {
            TapstateException ex = catchThrowableOfType(connection::verify, TapstateException.class);
            assertThat(ex).as("verify() against a dead port raises a coded diagnostic").isNotNull();
            assertThat(ex.code()).isEqualTo(StoreError.UNREACHABLE);
            assertThat(ex.args()).containsKey("target");
        }
    }

    @Test
    void malformedUriIsReportedAsACodedDiagnostic() {
        MongoConnectionSettings settings = new MongoConnectionSettings(
                "not-a-mongodb-uri", null, Duration.ofMillis(300));
        try (MongoConnection connection = new MongoConnection(settings)) {
            TapstateException ex = catchThrowableOfType(connection::verify, TapstateException.class);
            assertThat(ex).as("verify() with a malformed URI raises a coded diagnostic, not a bare IAE").isNotNull();
            assertThat(ex.code()).isEqualTo(StoreError.INVALID_URI);
        }
    }

    @Test
    void srvDnsFailureIsCodedWithoutEchoingUriCredentials() {
        String uri = "mongodb+srv://test-user:sentinel-password@cluster.example.net/metadata";
        TapstateException ex = catchThrowableOfType(
                () -> MongoConnection.parseConnectionString(uri,
                        ignored -> { throw new MongoConfigurationException("TXT lookup timed out"); }),
                TapstateException.class);

        assertThat(ex).isNotNull();
        assertThat(ex.code()).isEqualTo(StoreError.UNREACHABLE);
        assertThat(ex.args()).containsEntry("target", "cluster.example.net");
        assertThat(ex.getMessage()).doesNotContain("test-user", "sentinel-password");
    }

    @Test
    void acceptsTransactionalReplicaSetAndMongosButRejectsStandaloneHello() {
        assertThat(MongoConnection.isTransactionCapableTopology(new Document("setName", "rs0")))
                .as("a replica-set member can host checkpoint transactions").isTrue();
        assertThat(MongoConnection.isTransactionCapableTopology(new Document("msg", "isdbgrid")))
                .as("a mongos router can host sharded transactions").isTrue();
        assertThat(MongoConnection.isTransactionCapableTopology(new Document("isWritablePrimary", true)))
                .as("a standalone server cannot host checkpoint transactions").isFalse();
        assertThat(MongoConnection.isTransactionCapableTopology(new Document("msg", "other")))
                .as("only the mongos hello marker is accepted without a set name").isFalse();
    }

    @Test
    void aPlaintextUriConnectsByDefaultWithNoFlag() {
        // TLS is opt-in: an unspecified URI is plaintext and reaches the connection with no downgrade
        // flag, so a dead port reports unreachable (the guard that used to refuse plaintext is gone).
        MongoConnectionSettings settings = new MongoConnectionSettings(
                "mongodb://localhost:1/tapstate", null, Duration.ofMillis(300));
        try (MongoConnection connection = new MongoConnection(settings)) {
            TapstateException ex = catchThrowableOfType(connection::verify, TapstateException.class);
            assertThat(ex).as("a plaintext URI reaches the connection with no flag").isNotNull();
            assertThat(ex.code()).isEqualTo(StoreError.UNREACHABLE);
        }
    }

    @Test
    void tlsIsOffByDefaultWhenTheUriDoesNotAskForIt() {
        MongoClientSettings clientSettings = clientSettingsFor("mongodb://localhost:27017/tapstate");
        assertThat(clientSettings.getSslSettings().isEnabled())
                .as("an unspecified URI connects in plaintext by default").isFalse();
    }

    @Test
    void aTlsRequestInTheUriEnablesTls() {
        MongoClientSettings clientSettings = clientSettingsFor("mongodb://localhost:27017/tapstate?ssl=true");
        assertThat(clientSettings.getSslSettings().isEnabled())
                .as("ssl=true in the URI enables TLS (opt-in)").isTrue();
    }

    @Test
    void aUriThatRelaxesHostnameVerificationIsHonoredWhenTlsIsOn() {
        // The URI owns the TLS settings: asking to relax hostname verification is honored, not
        // overridden — needed for a self-signed development chain whose CN may not match the host.
        MongoClientSettings clientSettings =
                clientSettingsFor("mongodb://localhost:27017/tapstate?tls=true&tlsAllowInvalidHostnames=true");
        assertThat(clientSettings.getSslSettings().isEnabled()).isTrue();
        assertThat(clientSettings.getSslSettings().isInvalidHostNameAllowed())
                .as("the URI's hostname-verification setting is honored").isTrue();
    }

    @Test
    void aTlsCaFileIsAppliedAsTheTrustContextWhenTlsIsOn() throws Exception {
        String uri = "mongodb://localhost:27017/tapstate?ssl=true";
        MongoConnectionSettings settings =
                new MongoConnectionSettings(uri, caFixturePath(), Duration.ofMillis(300));
        MongoClientSettings clientSettings =
                new MongoConnection(settings).buildClientSettings(new ConnectionString(uri));
        assertThat(clientSettings.getSslSettings().isEnabled()).isTrue();
        assertThat(clientSettings.getSslSettings().getContext())
                .as("a custom CA file is applied as the TLS trust context (self-signed chain)")
                .isNotNull();
    }

    @Test
    void aTlsCaFileIsIgnoredWhenTlsIsOff() {
        // A CA file is meaningful only when TLS is on. With a plaintext URI it is not consulted, so
        // even an unreadable path raises nothing and the connection stays plaintext.
        String uri = "mongodb://localhost:27017/tapstate";
        MongoConnectionSettings settings =
                new MongoConnectionSettings(uri, "/no/such/directory/ca.pem", Duration.ofMillis(300));
        MongoClientSettings clientSettings =
                new MongoConnection(settings).buildClientSettings(new ConnectionString(uri));
        assertThat(clientSettings.getSslSettings().isEnabled())
                .as("a plaintext URI stays plaintext and the CA file is ignored").isFalse();
    }

    @Test
    void anUnreadableTlsCaFileIsReportedAsACodedDiagnosticWhenTlsIsOn() {
        String uri = "mongodb://localhost:27017/tapstate?ssl=true";
        MongoConnectionSettings settings = new MongoConnectionSettings(
                uri, "/no/such/directory/ca.pem", Duration.ofMillis(300));
        MongoConnection connection = new MongoConnection(settings);
        ConnectionString connectionString = new ConnectionString(uri);
        TapstateException ex = catchThrowableOfType(
                () -> connection.buildClientSettings(connectionString), TapstateException.class);
        assertThat(ex).as("an unreadable CA file surfaces a coded diagnostic, not a bare exception").isNotNull();
        assertThat(ex.code()).isEqualTo(StoreError.TLS_CA_UNREADABLE);
        assertThat(ex.args()).containsKey("path");
    }

    @Test
    void anEmptyCaFileIsReportedAsACodedDiagnosticWhenTlsIsOn() throws Exception {
        // A readable but cert-less CA file parses to zero trust anchors without throwing; left
        // unchecked it would build a trust-nothing context and surface an opaque unreachable. It is
        // an unusable CA file, so it is reported as the coded misconfiguration it is.
        String uri = "mongodb://localhost:27017/tapstate?ssl=true";
        String emptyCa = Path.of(MongoConnectionTest.class.getResource("/tls/empty-ca.pem").toURI()).toString();
        MongoConnectionSettings settings =
                new MongoConnectionSettings(uri, emptyCa, Duration.ofMillis(300));
        MongoConnection connection = new MongoConnection(settings);
        ConnectionString connectionString = new ConnectionString(uri);
        TapstateException ex = catchThrowableOfType(
                () -> connection.buildClientSettings(connectionString), TapstateException.class);
        assertThat(ex).as("an empty CA file surfaces a coded diagnostic, not a silent trust-nothing").isNotNull();
        assertThat(ex.code()).isEqualTo(StoreError.TLS_CA_UNREADABLE);
    }

    @Test
    void theTrustManagerFromTheCaFileTrustsOnlyTheSelfSignedChain() throws Exception {
        TrustManager[] trustManagers = MongoConnection.buildTrustManagers(caFixturePath());
        X509TrustManager x509 = (X509TrustManager) trustManagers[0];
        // Exactly the CA in the file is a trusted issuer — a custom trust store built from the
        // self-signed chain, not the JVM-wide default (which carries ~100 public roots).
        assertThat(x509.getAcceptedIssuers())
                .as("only the self-signed CA is trusted").hasSize(1);
        assertThat(x509.getAcceptedIssuers()[0].getSubjectX500Principal().getName())
                .contains("CN=localhost");
    }

    private static MongoClientSettings clientSettingsFor(String uri) {
        MongoConnectionSettings settings =
                new MongoConnectionSettings(uri, null, Duration.ofMillis(300));
        return new MongoConnection(settings).buildClientSettings(new ConnectionString(uri));
    }

    private static String caFixturePath() throws Exception {
        return Path.of(MongoConnectionTest.class.getResource("/tls/ca.pem").toURI()).toString();
    }
}
