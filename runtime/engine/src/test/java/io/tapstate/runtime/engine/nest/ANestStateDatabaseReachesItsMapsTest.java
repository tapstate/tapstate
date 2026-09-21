package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.OperatorStateStore;
import io.tapstate.spi.store.OperatorStateStores;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** One member routes otherwise equal map keys through the database carried by each exact map config. */
class ANestStateDatabaseReachesItsMapsTest {

    private static final String FIRST = "nest.pipeline.first.$root";
    private static final String SECOND = "nest.pipeline.second.$root";

    private final RecordingStores stores = new RecordingStores();
    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("nest-state-database-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
        member.getConfig().addMapConfig(
                NestSettings.defaults().backedStateMapsForDatabase(stores.defaultDatabase()));
        NestStateMapStoreFactory.bindTo(member, stores);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void equalKeysInTwoNestNamespacesReachTheirOwnDatabases() {
        NestStatePlacement.applyTo(member, Map.of(FIRST, "state_a", SECOND, "state_b"),
                NestSettings.defaults());

        member.getMap(FIRST).put("same-key", "first");
        member.getMap(SECOND).put("same-key", "second");

        String storedKey = NestStateKeys.nameOf("same-key");
        assertThat(stores.state("state_a").load(FIRST, storedKey)).isPresent();
        assertThat(stores.state("state_a").load(SECOND, storedKey)).isEmpty();
        assertThat(stores.state("state_b").load(SECOND, storedKey)).isPresent();
        assertThat(NestStatePlacement.databaseOf(member, FIRST, stores.defaultDatabase()))
                .isEqualTo("state_a");
    }

    @Test
    void theSameResolvedDatabaseCanBeAppliedAgain() {
        NestStatePlacement.applyTo(member, Map.of(FIRST, "state_a"), NestSettings.defaults());

        assertThatCode(() -> NestStatePlacement.applyTo(
                member, Map.of(FIRST, "state_a"), NestSettings.defaults()))
                .doesNotThrowAnyException();
    }

    @Test
    void aLiveNamespaceCannotSilentlyMoveToAnotherDatabase() {
        NestStatePlacement.applyTo(member, Map.of(FIRST, "state_a"), NestSettings.defaults());

        Throwable thrown = catchThrowable(() -> NestStatePlacement.applyTo(
                member, Map.of(FIRST, "state_b"), NestSettings.defaults()));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code().code())
                .isEqualTo("nest.state-database-changed-while-running");
        assertThat(((TapstateException) thrown).args())
                .containsEntry("namespace", FIRST)
                .containsEntry("configured", "state_a")
                .containsEntry("requested", "state_b");
    }

    @Test
    void aDeadLetterFollowsTheSameDatabaseAsItsNamespaceState() {
        NestStatePlacement.applyTo(member, Map.of(FIRST, "state_a"), NestSettings.defaults());
        NestVertex vertex = new NestVertex(List.of(), "root", FIRST,
                List.of("id"), List.of("id"), List.of());
        ReleasedChild released = new ReleasedChild(new NestElement(
                new ElementRef(List.of("items"), 1, List.of(7), 7), Map.of("id", 7),
                new SourceOrder(1L, 7L), Map.of("orders", new ChainPosition(
                        new SourceOrder(1L, 7L), "offset-7"))), Duration.ZERO);

        new DurableNestDeadLetter(() -> 9_000L)
                .bindTo(stores, member, NestDeadLetterGauge.NONE)
                .unassemblable(vertex, released);

        assertThat(stores.deadLetters("state_a").read(FIRST, 10)).hasSize(1);
        assertThat(stores.deadLetters(stores.defaultDatabase()).read(FIRST, 10)).isEmpty();
    }

    private static final class RecordingStores implements OperatorStateStores {

        private final Map<String, HeapKeyedStateStore> stateByDatabase = new LinkedHashMap<>();
        private final Map<String, RecordingDeadLetters> deadLettersByDatabase = new LinkedHashMap<>();

        @Override
        public String defaultDatabase() {
            return "deployment_default";
        }

        @Override
        public OperatorStateStore inDatabase(String database) {
            return new OperatorStateStore(state(database), deadLetters(database));
        }

        HeapKeyedStateStore state(String database) {
            return stateByDatabase.computeIfAbsent(database, ignored -> new HeapKeyedStateStore());
        }

        RecordingDeadLetters deadLetters(String database) {
            return deadLettersByDatabase.computeIfAbsent(database, ignored -> new RecordingDeadLetters());
        }
    }

    private static final class RecordingDeadLetters implements NestDeadLetterStore {

        private final Map<String, NestDeadLetterRecord> records = new LinkedHashMap<>();

        @Override
        public void record(NestDeadLetterRecord record) {
            records.put(record.namespace() + "/" + record.element(), record);
        }

        @Override
        public List<NestDeadLetterRecord> read(String namespace, int limit) {
            return records.values().stream()
                    .filter(record -> record.namespace().equals(namespace))
                    .limit(limit)
                    .toList();
        }

        @Override
        public void dropNamespace(String namespace) {
            records.values().removeIf(record -> record.namespace().equals(namespace));
        }
    }
}
