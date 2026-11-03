package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoStorageConnectionInputTest {
    private static StorageBinding.Connection input(String uri, StorageBinding.Connection.Kind kind) {
        return new StorageBinding.Connection(kind, uri, "selected", null);
    }
    @Test void validatesAnUnresolvableDefaultSrvWithoutLookingUpTxtOrRewritingItsIntent() {
        var original = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                "mongodb+srv://alice:secret@unresolvable.example.invalid/selected?tls=true", "selected", null);
        assertThat(MongoStorageConnectionInput.validate(original)).isEqualTo(original);
        assertThat(MongoDatabaseNames.fromUri(original.uri())).isEqualTo("selected");
    }
    @Test void localValidationStillRejectsUnsafeTlsAndMismatchedDatabase() {
        for (String uri : new String[]{"mongodb+srv://alice:secret@unresolvable.example.invalid/other?tls=true",
                "mongodb+srv://alice:secret@unresolvable.example.invalid/selected?tlsInsecure=true"}) {
            assertThatThrownBy(() -> MongoStorageConnectionInput.validate(input(uri, StorageBinding.Connection.Kind.MONGODB)))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("secret");
        }
    }
    @Test void oneNormalizationRoundResolvesSrvTxtOnceWithoutCachingAnotherRound() {
        var lookups = new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.Function<String,com.mongodb.ConnectionString> parser = uri -> new com.mongodb.ConnectionString(uri,
                (name, type) -> { lookups.incrementAndGet(); return java.util.List.of("authSource=admin"); });
        var probe = new MongoStorageConnectionProbe(java.time.Duration.ofSeconds(1), parser);
        var input = input("mongodb+srv://alice:secret@cluster.example.invalid/selected?tls=true", StorageBinding.Connection.Kind.MONGODB);
        var normalized = probe.normalize(input);
        assertThat(lookups).hasValue(1);
        var parsed = MongoStorageConnectionInput.syntaxOnly(normalized.uri());
        assertThat(parsed.getCredential().getSource()).isEqualTo("admin");
        assertThat(parsed.getSslEnabled()).isTrue();
        assertThat(parsed.getDatabase()).isEqualTo("selected");
        probe.normalize(input);
        assertThat(lookups).hasValue(2);
    }

    @Test void freezesOriginalAuthenticationDatabaseAndKeepsOptionsAndSeeds() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:pa%40ss@host1:27017,host2:27017/original?replicaSet=rs&tls=true", StorageBinding.Connection.Kind.MONGODB));
        assertThat(MongoStorageConnectionInput.databaseUri(normalized)).isEqualTo(
                "mongodb://alice:pa%40ss@host1:27017,host2:27017/selected?replicaSet=rs&tls=true&authSource=original");
    }
    @Test void atlasRequiresTlsAndKeepsNegotiatedScramWithAdminAuthentication() {
        var normalized = MongoStorageConnectionInput.normalize(input(
                "mongodb://alice:secret@atlas.example/some_database", StorageBinding.Connection.Kind.ATLAS));
        assertThat(MongoStorageConnectionInput.databaseUri(normalized)).isEqualTo(
                "mongodb://alice:secret@atlas.example/selected?authSource=admin&tls=true");
    }
    @Test void explicitScramMechanismsSurviveNormalizationAndDatabaseRewrite() {
        for (String mechanism : new String[]{"SCRAM-SHA-1", "SCRAM-SHA-256"}) {
            var normalized = MongoStorageConnectionInput.normalize(input(
                    "mongodb://alice:secret@atlas.example/original?authMechanism=" + mechanism, StorageBinding.Connection.Kind.ATLAS));
            var parsed = new com.mongodb.ConnectionString(MongoStorageConnectionInput.databaseUri(normalized));
            assertThat(parsed.getCredential().getAuthenticationMechanism().getMechanismName()).isEqualTo(mechanism);
            assertThat(parsed.getCredential().getSource()).isEqualTo("admin");
            assertThat(parsed.getSslEnabled()).isTrue();
        }
    }
    @Test void rejectsDuplicateConflictingInsecureAndClientCertificateOptionsWithoutEchoingSecrets() {
        for (String option : new String[]{"tls=true&TLS=true", "ssl=true&tls=false", "tls=false", "authSource=other",
                "authMechanism=PLAIN", "tlsAllowInvalidHostnames=true", "tlsAllowInvalidCertificates=true",
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
