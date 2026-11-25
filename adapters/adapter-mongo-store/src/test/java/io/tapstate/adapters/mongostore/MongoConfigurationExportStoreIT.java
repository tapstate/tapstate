package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoConfigurationExportStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final DslParser PARSER = new DslParser();
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "setup", 1);
    private static final User ADMIN = new User("admin", "$2a$04$" + "A".repeat(53), "admin");
    private static SourceResource input(String id) { return new SourceResource(id, null, "mysql", Map.of("host", "host", "password", "source-secret"), SourceMode.CDC, List.of(TableRef.literal("orders")), null, null); }
    @Test void selectedPipelineCarriesItsTypedDefinitionClosureAndLayoutButNoManagedConnectionOrRuntime() {
        withConfiguration(StorageBinding.Mode.OP, (domain, database) -> {
            var transform = PARSER.parse("version: tapstate/v1\nkind: transform\nid: filter_def\ntype: filter\nexpr: \"true\"\n");
            var view = PARSER.parse("version: tapstate/v1\nkind: view\nid: view_def\nprimary_key: id\n");
            var serve = PARSER.parse("version: tapstate/v1\nkind: serve\nid: serve_def\nsync: [{id: out, source: views}]\n");
            var pipeline = PARSER.parse("version: tapstate/v1\nkind: pipeline\nid: p\nsource: [input]\ntransforms: [{id: filtered, use: filter_def, from: orders}]\nview: {id: shaped, use: view_def, from: filtered}\nserve: {use: serve_def, from: shaped}\n");
            domain.artifacts().saveAll(List.of(input("input"), input("unrelated"), transform, view, serve, pipeline));
            domain.layouts().save(new PipelineLayout("p", Map.of("input", new PipelineLayout.NodePosition(1, 2)), null));
            long revision = domain.configurationRevision();
            var snapshot = domain.exports().snapshot(scope(StorageBinding.Mode.OP), new ConfigurationExportStore.Selection(Set.of("p"), Set.of("admin")));
            assertThat(snapshot.payload().resources()).extracting(Resource::id).containsExactlyInAnyOrder("p", "input", "filter_def", "view_def", "serve_def");
            assertThat(snapshot.defaultSourceId()).isEqualTo("views");
            assertThat(snapshot.payload().users()).containsExactly(ADMIN);
            assertThat(snapshot.payload().layouts()).hasSize(1);
            assertThat(snapshot.payload().referenceHashes()).isEmpty(); assertThat(snapshot.payload().defaultSourceMapping()).isEmpty();
            assertThat(domain.configurationRevision()).isEqualTo(revision);
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(database).countDocuments()).isZero();
            assertThat(snapshot.toString()).doesNotContain("source-secret", "master-secret", ADMIN.passwordHash());
            SystemCollections.TRANSFORM_DEFINITIONS.on(database).deleteOne(new Document("_id", "filter_def"));
            assertThatThrownBy(() -> domain.exports().snapshot(scope(StorageBinding.Mode.OP), new ConfigurationExportStore.Selection(Set.of("p"), Set.of())))
                    .isInstanceOf(TapstateException.class);
        });
    }
    @Test void missingTypedDependenciesCloudUsersAndInvalidScopesAreRefusedWithoutAWrite() {
        withConfiguration(StorageBinding.Mode.CLOUD, (domain, database) -> {
            domain.artifacts().save(input("input"));
            long revision = domain.configurationRevision();
            assertThatThrownBy(() -> domain.exports().snapshot(scope(StorageBinding.Mode.CLOUD), new ConfigurationExportStore.Selection(Set.of("input"), Set.of("admin"))))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> domain.exports().snapshot(new ConfigurationImportStore.Scope("foreign", 1, StorageBinding.Mode.CLOUD, "org"), new ConfigurationExportStore.Selection(Set.of("input"), Set.of())))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> domain.exports().snapshot(scope(StorageBinding.Mode.CLOUD), new ConfigurationExportStore.Selection(Set.of("missing"), Set.of())))
                    .isInstanceOf(TapstateException.class);
            assertThat(domain.configurationRevision()).isEqualTo(revision);
            assertThat(SystemCollections.CONFIGURATION_IMPORTS.on(database).countDocuments()).isZero();
        });
    }
    @Test void reopenedManagedSourceRetainsItsCompleteCaAndCannotBeForgedByAnOrdinarySource() {
        withConfiguration(StorageBinding.Mode.OP, (domain, database) -> {
            assertThat(domain.artifacts().managedDefaultSourceId()).contains("views");
            var source = (SourceResource)domain.artifacts().get("views").orElseThrow();
            assertThat(source.config()).containsEntry("tlsCaPem", "retained-ca-material");
            assertThat(SystemCollections.SOURCES.on(database).find(new Document("_id", "views")).first().toJson())
                    .doesNotContain("master-secret", "retained-ca-material");
            try (var reopened = MongoConfigurationDomain.open(settings(database.getName()), "cluster")) {
                assertThat(reopened.artifacts().managedDefaultSourceId()).contains("views");
                assertThat(((SourceResource)reopened.artifacts().get("views").orElseThrow()).config()).isEqualTo(source.config());
                assertThatThrownBy(() -> reopened.artifacts().save(new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://other/master"), null, null, null, null)))
                        .isInstanceOf(TapstateException.class);
            }
        });
    }
    private static ConfigurationImportStore.Scope scope(StorageBinding.Mode mode) { return new ConfigurationImportStore.Scope("cluster", 1, mode, mode == StorageBinding.Mode.CLOUD ? "org" : null); }
    private static MongoConnectionSettings settings(String name) { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(name), null, Duration.ofSeconds(5)); }
    private static void withConfiguration(StorageBinding.Mode mode, Scenario action) {
        String name = "export_" + UUID.randomUUID().toString().replace("-", "");
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoConfigurationBootstrap.prepare(settings(name), "cluster", "installation", "setup", 1)) {
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache/cache", "cache", null);
            var source = new SourceResource(StorageBinding.defaultSourceId(mode), null, "mongodb", Map.of("uri", "mongodb://user:master-secret@host/master", "tlsCaPem", "retained-ca-material"), null, null, null, null);
            domain.bindings().commit(SETUP, mode, cache, source); domain.bindings().complete(SETUP);
            if (mode == StorageBinding.Mode.OP) domain.firstAdmin().create(SETUP, ADMIN);
            action.run(domain, admin.getDatabase(name));
            admin.getDatabase(name).drop();
        } catch (Exception failed) { throw new AssertionError(failed); }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, com.mongodb.client.MongoDatabase database) throws Exception; }
}
