package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoStorageConnectionInputTest {
    private static StorageBinding.Connection input(String uri, StorageBinding.Connection.Kind kind) {
        return new StorageBinding.Connection(kind, uri, "selected", null);
    }
    @Test void freezesOriginalAuthenticationDatabaseAndKeepsOptionsAndSeeds() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:pa%40ss@host1:27017,host2:27017/original?replicaSet=rs&tls=true", StorageBinding.Connection.Kind.MONGODB));
        assertThat(MongoStorageConnectionInput.databaseUri(normalized)).isEqualTo(
                "mongodb://alice:pa%40ss@host1:27017,host2:27017/selected?replicaSet=rs&tls=true&authSource=original");
    }
    @Test void atlasRequiresTlsAndNormalizesSha256WithAdminAuthentication() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:secret@atlas.example/some_database", StorageBinding.Connection.Kind.ATLAS));
        assertThat(MongoStorageConnectionInput.databaseUri(normalized)).isEqualTo(
                "mongodb://alice:secret@atlas.example/selected?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true");
    }
    @Test void rejectsDuplicateConflictingInsecureAndClientCertificateOptionsWithoutEchoingSecrets() {
        for (String option : new String[]{"tls=true&TLS=true", "ssl=true&tls=false", "tls=false", "authSource=other",
                "authMechanism=SCRAM-SHA-1", "tlsAllowInvalidHostnames=true", "tlsAllowInvalidCertificates=true",
                "tlsInsecure=true", "tlsCertificateKeyFile=secret", "tlsCAFile=secret"}) {
            assertThatThrownBy(() -> MongoStorageConnectionInput.normalize(input(
                    "mongodb://alice:sentinel-secret@host/original?" + option, StorageBinding.Connection.Kind.ATLAS)))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("sentinel-secret");
        }
    }
    @Test void logicalDatabaseIsAnEncodedPathComponentAndNotAnotherUriOption() {
        for (String database : new String[]{"tenant+one", "tenant?one", "tenant#one", "tenant%one"}) {
            var normalized = MongoStorageConnectionInput.normalize(new StorageBinding.Connection(
                    StorageBinding.Connection.Kind.MONGODB,
                    "mongodb://alice:secret@host/original?replicaSet=rs&tls=true", database, null));
            var parsed = new com.mongodb.ConnectionString(MongoStorageConnectionInput.databaseUri(normalized));
            assertThat(parsed.getDatabase()).isEqualTo(database);
            assertThat(parsed.getCredential().getSource()).isEqualTo("original");
            assertThat(parsed.getRequiredReplicaSetName()).isEqualTo("rs");
            assertThat(parsed.getSslEnabled()).isTrue();
        }
    }
    @Test void atlasSeedOnlyUriRetainsItsAuthorityWhenOptionsAreAdded() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:secret@atlas.example", StorageBinding.Connection.Kind.ATLAS));
        var parsed = new com.mongodb.ConnectionString(MongoStorageConnectionInput.databaseUri(normalized));
        assertThat(parsed.getHosts()).containsExactly("atlas.example");
        assertThat(parsed.getDatabase()).isEqualTo("selected");
        assertThat(parsed.getCredential().getSource()).isEqualTo("admin");
        assertThat(parsed.getSslEnabled()).isTrue();
    }
    @Test void plainMongoKeepsExistingNonTlsScopeAndExplicitAuthenticationSource() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:secret@host/original?authSource=admin", StorageBinding.Connection.Kind.MONGODB));
        assertThat(MongoStorageConnectionInput.databaseUri(normalized)).isEqualTo(
                "mongodb://alice:secret@host/selected?authSource=admin");
    }
}
