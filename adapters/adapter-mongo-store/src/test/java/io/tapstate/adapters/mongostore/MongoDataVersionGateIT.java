package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Witnesses the startup gate that refuses a store whose system data is newer than this build knows
 * how to read. It is the whole of what an older line's patch release carries, so it is witnessed
 * against a real server on its own: the read is one document and one field, and every later runner
 * has to stay compatible with exactly this shape.
 *
 * <p>What makes each case discriminating: the refusal case differs from the two passing ones only in
 * the value of that one field, so a gate that read the wrong field, or compared the wrong way round,
 * fails one of the three rather than all of them.
 */
@RequiresDocker
class MongoDataVersionGateIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    /** The one document the gate reads, in the shape this release freezes. */
    private static void seedInstalledVersion(String database, Object installedVersion) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            client.getDatabase(database).getCollection("system_meta").insertOne(
                    new Document("_id", "schema").append("installedVersion", installedVersion));
        }
    }

    private static MongoConnectionSettings settingsFor(String database) {
        String base = REPLICA_SET.getReplicaSetUrl();
        // getReplicaSetUrl() already names a database; address our own so the cases cannot see each
        // other's system_meta.
        String uri = base.substring(0, base.lastIndexOf('/') + 1) + database + "?directConnection=true";
        return new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
    }

    @Test
    void refusesToStartWhenTheStoredVersionIsNewerThanTheBinary() {
        seedInstalledVersion("gate_newer", 99);
        try (MongoConnection connection = new MongoConnection(settingsFor("gate_newer"))) {
            TapstateException refusal =
                    catchThrowableOfType(connection::verify, TapstateException.class);
            assertThat(refusal).isNotNull();
            assertThat(refusal.code().code()).isEqualTo("migration.data-newer-than-binary");
            assertThat(refusal.args()).containsEntry("installed", "99");
        }
    }

    @Test
    void startsAgainstAStoreThatHasNeverBeenMigrated() {
        // No system_meta at all: the overwhelmingly common path, and the one an existing install is on.
        try (MongoConnection connection = new MongoConnection(settingsFor("gate_absent"))) {
            assertThatCode(connection::verify).doesNotThrowAnyException();
        }
    }

    @Test
    void startsWhenTheStoredVersionIsOneTheBinaryKnows() {
        seedInstalledVersion("gate_known", 0);
        try (MongoConnection connection = new MongoConnection(settingsFor("gate_known"))) {
            assertThatCode(connection::verify).doesNotThrowAnyException();
        }
    }

    @Test
    void refusesToStartWhenTheStoredVersionCannotBeReadAsANumber() {
        // Only this gate ever writes the field, so a non-numeric value means something else edited the
        // store. Refusing is the conservative reading: proceeding would be deciding the data is not
        // newer on the strength of a value nothing here can compare.
        seedInstalledVersion("gate_garbage", "three");
        try (MongoConnection connection = new MongoConnection(settingsFor("gate_garbage"))) {
            TapstateException refusal =
                    catchThrowableOfType(connection::verify, TapstateException.class);
            assertThat(refusal).isNotNull();
            assertThat(refusal.code().code()).isEqualTo("migration.data-newer-than-binary");
            assertThat(refusal.args()).containsEntry("installed", "three");
        }
    }
}
