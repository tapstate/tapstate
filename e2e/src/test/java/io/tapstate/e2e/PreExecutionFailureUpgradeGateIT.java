package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.assertThat;

/** The previous immutable binary must refuse the new interpretation before opening Hazelcast or HTTP. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.pre-execution.previous-jar", matches = ".+")
class PreExecutionFailureUpgradeGateIT {
    @Test
    void installationAloneRefusesThePreviousBinaryBeforeRuntimeAdmission() throws Exception {
        Path previous = Path.of(System.getProperty("tapstate.e2e.pre-execution.previous-jar"));
        assertThat(previous).isRegularFile();
        String uri = SharedMongo.replicaSetUrl("e2e_pre_execution_version_gate");
        try (var current = RealProcessServer.start(uri)) {
            assertThat(current.isAlive()).isTrue();
        }
        try (var client = MongoClients.create(uri)) {
            var db = client.getDatabase("e2e_pre_execution_version_gate");
            assertThat(SystemCollections.PIPELINE_OBSERVATION.on(db).countDocuments()).isZero();
            assertThat(SystemCollections.SYSTEM_META.on(db).find(new Document("_id", "schema")).first().getInteger("installedVersion")).isEqualTo(18);
        }
        try (var old = RealProcessServer.launching(uri, previous)) {
            Await.answered("the previous binary to finish its version refusal", Duration.ofSeconds(30), Duration.ofMillis(100),
                    () -> old.isAlive() ? java.util.Optional.empty() : java.util.Optional.of(old.exitValue()));
            assertThat(old.isAlive()).isFalse();
            assertThat(old.exitValue()).isNotZero();
            String output = Files.readString(old.output());
            assertThat(output).contains("migration.data-newer-than-binary");
            assertThat(output).doesNotContain("Hazelcast 5.7.0", "Tomcat started on port");
        }
    }
}
