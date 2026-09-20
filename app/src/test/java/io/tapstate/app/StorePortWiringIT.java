package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the assembly root exposing a working store under {@code --role=all}: with the store
 * enabled and pointed at a real replica-set, the context starts, a single driver-free {@code
 * StorePort} bean is present, and it round-trips a registered connection. This is the store adapter
 * wired through the assembly root (rule R7), end to end. Where Docker is absent this aborts on a
 * developer machine and fails in CI, where a skip would be a green build that ran nothing.
 */
@RequiresDocker
class StorePortWiringIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(StoreConfiguration.class);

    @Test
    void assemblyRootExposesAWorkingStorePortAgainstARealStore() {
        dropDatabases(MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE);
        runner.withPropertyValues(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + REPLICA_SET.getReplicaSetUrl(),
                        // the container speaks plaintext; TLS is opt-in, so no flag is needed here
                        "tapstate.store.mongo.server-selection-timeout=5s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(StorePort.class);
                    StorePort store = context.getBean(StorePort.class);
                    store.catalog().save(new ConnectionConfig("mysql-local", "mysql", Map.of("host", "localhost")));
                    assertThat(store.catalog().get("mysql-local")).isPresent();
                    store.keyedState().save(
                            "default.database", "key", "value".getBytes(StandardCharsets.UTF_8));

                    try (MongoClient raw = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
                        assertThat(raw.getDatabase(MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE)
                                .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isEqualTo(1);
                        String controlDatabase = new ConnectionString(REPLICA_SET.getReplicaSetUrl()).getDatabase();
                        assertThat(raw.getDatabase(controlDatabase)
                                .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isZero();
                    }
                });
    }

    @Test
    void configuredOperatorStateDatabasesIsolateEqualNamespacesAndSurviveARestart() {
        String namespace = "nest.shared.assemble.items";
        String controlA = "issue431_control_a";
        String controlB = "issue431_control_b";
        String stateA = "issue431_state_a";
        String stateB = "issue431_state_b";
        dropDatabases(controlA, controlB, stateA, stateB, MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE);

        withStore(controlA, stateA, store -> {
            store.keyedState().save(namespace, "same-key", "left".getBytes(StandardCharsets.UTF_8));
            store.nestDeadLetters().record(deadLetter(namespace, "left"));
        });

        withStore(controlB, stateB, store -> {
            assertThat(store.keyedState().load(namespace, "same-key")).isEmpty();
            assertThat(store.nestDeadLetters().read(namespace, 10)).isEmpty();
            store.keyedState().save(namespace, "same-key", "right".getBytes(StandardCharsets.UTF_8));
            store.nestDeadLetters().record(deadLetter(namespace, "right"));
        });

        withStore(controlA, stateA, store -> {
            assertThat(store.keyedState().load(namespace, "same-key"))
                    .contains("left".getBytes(StandardCharsets.UTF_8));
            assertThat(store.nestDeadLetters().read(namespace, 10))
                    .extracting(NestDeadLetterRecord::chain)
                    .containsExactly("left");
        });

        try (MongoClient raw = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            assertThat(raw.getDatabase(stateA).getCollection(MongoStorePort.OPERATOR_STATE).countDocuments())
                    .isEqualTo(1);
            assertThat(raw.getDatabase(stateA).getCollection(MongoStorePort.NEST_DEAD_LETTERS).countDocuments())
                    .isEqualTo(1);
            assertThat(raw.getDatabase(stateB).getCollection(MongoStorePort.OPERATOR_STATE).countDocuments())
                    .isEqualTo(1);
            assertThat(raw.getDatabase(stateB).getCollection(MongoStorePort.NEST_DEAD_LETTERS).countDocuments())
                    .isEqualTo(1);
            assertThat(raw.getDatabase(MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE)
                    .getCollection(MongoStorePort.OPERATOR_STATE).countDocuments()).isZero();
            assertThat(raw.getDatabase(MongoProperties.DEFAULT_OPERATOR_STATE_DATABASE)
                    .getCollection(MongoStorePort.NEST_DEAD_LETTERS).countDocuments())
                    .isZero();
        }
    }

    @Test
    void blankOrIllegalOperatorStateDatabaseFailsStartupWithACodedDiagnostic() {
        assertInvalidOperatorStateDatabase("");
        assertInvalidOperatorStateDatabase("invalid/name");
    }

    private void withStore(String controlDatabase, String operatorStateDatabase, Consumer<StorePort> action) {
        runner.withPropertyValues(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + REPLICA_SET.getReplicaSetUrl(controlDatabase),
                        "tapstate.store.mongo.operator-state-database=" + operatorStateDatabase,
                        "tapstate.store.mongo.server-selection-timeout=5s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    action.accept(context.getBean(StorePort.class));
                });
    }

    private static NestDeadLetterRecord deadLetter(String namespace, String chain) {
        return new NestDeadLetterRecord(
                namespace, "[\"items\"]#[1]~i", chain, "1:1", 0L, 9_000L, Map.of("id", 1));
    }

    private void assertInvalidOperatorStateDatabase(String name) {
        runner.withPropertyValues(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + REPLICA_SET.getReplicaSetUrl("issue431_invalid"),
                        "tapstate.store.mongo.operator-state-database=" + name,
                        "tapstate.store.mongo.server-selection-timeout=5s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    TapstateException coded = firstCauseOfType(context.getStartupFailure(), TapstateException.class);
                    assertThat(coded).as("startup failure carries a coded diagnostic").isNotNull();
                    assertThat(coded.code()).isEqualTo(StoreError.INVALID_OPERATOR_STATE_DATABASE);
                });
    }

    private static <T extends Throwable> T firstCauseOfType(Throwable failure, Class<T> type) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
        }
        return null;
    }

    private static void dropDatabases(String... names) {
        try (MongoClient raw = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            List.of(names).forEach(name -> raw.getDatabase(name).drop());
        }
    }
}
