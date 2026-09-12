package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkTargetPreparationTest {
    private static final TargetTable TARGET = new TargetTable("orders",
            List.of(new TargetField("id", "bigint", true)),
            List.of(new TargetIndex(List.of("id"), true)));
    private static final PipelineNode NODE = new PipelineNode("pipeline", "sink");

    @Test
    void aNewEmptyTableIsCreatedWithoutClearFailOrDuplicatePrimaryKey() throws Throwable {
        for (OnFullLoad policy : OnFullLoad.values()) {
            List<String> calls = new ArrayList<>();
            var preparation = preparation(false, policy, true, calls, new State());
            preparation.prepare(TARGET, TargetTapTable.build(TARGET));
            assertThat(calls).containsExactly("create");
        }
    }

    @Test
    void existingTablesFollowTheDeclaredPolicyAndPreparationRunsOnlyOnce() throws Throwable {
        List<String> calls = new ArrayList<>();
        var preparation = preparation(true, OnFullLoad.CLEAR, true, calls, new State());
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "clear", "index");
        calls.clear();
        preparation(true, OnFullLoad.APPEND, true, calls, new State())
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "index");
    }

    @Test
    void failChecksRowsRatherThanRejectingAnExistingEmptyTable() throws Throwable {
        ConnectorFunctions functions = functions(true, new ArrayList<>());
        functions.supportCountByPartitionFilterFunction((context, table, filter) -> {
            assertThat(filter).as("connectors dereference the count filter even when counting all rows").isNotNull();
            assertThat(filter.getMatch()).isNullOrEmpty();
            assertThat(filter.getOperators()).isNullOrEmpty();
            return 0L;
        });
        new PdkTargetPreparation(null, functions, OnFullLoad.FAIL, true, NODE, new State())
                .prepare(TARGET, TargetTapTable.build(TARGET));
        functions.supportCountByPartitionFilterFunction((context, table, filter) -> 1L);
        assertThatThrownBy(() -> new PdkTargetPreparation(
                null, functions, OnFullLoad.FAIL, true, NODE, new State())
                .prepare(TARGET, TargetTapTable.build(TARGET)))
                .hasMessageContaining("orders").hasMessageContaining("not empty");
    }

    @Test
    void resumeAndRecoveryNeverClearPreviouslyPreparedTables() throws Throwable {
        State state = new State();
        List<String> calls = new ArrayList<>();
        preparation(true, OnFullLoad.CLEAR, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        calls.clear();
        preparation(true, OnFullLoad.CLEAR, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "index");
    }

    @Test
    void cdcOnlyAndAResumedSnapshotOverrideClearEvenWithoutAnEarlierMarker() throws Throwable {
        List<String> calls = new ArrayList<>();
        preparation(true, OnFullLoad.CLEAR, false, calls, new State())
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "index");
    }

    @Test
    void rerunPurgesOnlyPreparationStateAndStillHonorsAppend() throws Throwable {
        State state = new State();
        List<String> calls = new ArrayList<>();
        preparation(true, OnFullLoad.CLEAR, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        state.dropNamespace(io.tapstate.spi.sink.SinkPreparationNamespace.of(NODE));
        calls.clear();
        preparation(true, OnFullLoad.APPEND, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "index");
    }

    @Test
    void aFailedClearIsRetriedAndNeverRecordedAsPrepared() throws Throwable {
        List<String> calls = new ArrayList<>();
        State state = new State();
        ConnectorFunctions functions = functions(true, calls);
        functions.supportClearTable((context, event) -> { throw new IllegalStateException("clear failed"); });
        var preparation = new PdkTargetPreparation(null, functions, OnFullLoad.CLEAR, true, NODE, state);
        assertThatThrownBy(() -> preparation.prepare(TARGET, TargetTapTable.build(TARGET)))
                .hasMessageContaining("clear failed");
        assertThat(state.entries).isEmpty();
        functions.supportClearTable((context, event) -> calls.add("clear"));
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "create", "clear", "index");
    }

    @Test
    void aFailedIndexRetriesWithoutRepeatingAnAlreadySuccessfulClear() throws Throwable {
        State state = new State();
        List<String> calls = new ArrayList<>();
        ConnectorFunctions functions = functions(true, calls);
        functions.supportCreateIndex((context, table, event) -> { throw new IllegalStateException("index failed"); });
        var preparation = new PdkTargetPreparation(null, functions, OnFullLoad.CLEAR, true, NODE, state);
        assertThatThrownBy(() -> preparation.prepare(TARGET, TargetTapTable.build(TARGET)))
                .hasMessageContaining("index failed");
        assertThat(calls).containsExactly("create", "clear");
        functions.supportCreateIndex((context, table, event) -> calls.add("index"));
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "clear", "create", "index");
    }

    @Test
    void failedCreateAndCountDoNotLeaveReceiptsOrSuppressRetry() throws Throwable {
        for (boolean createFails : List.of(true, false)) {
            State state = new State();
            List<String> calls = new ArrayList<>();
            ConnectorFunctions functions = functions(true, calls);
            if (createFails) {
                functions.supportCreateTableV2((context, event) -> { throw new IllegalStateException("create failed"); });
            } else {
                functions.supportCountByPartitionFilterFunction((context, table, filter) -> {
                    throw new IllegalStateException("count failed");
                });
            }
            var preparation = new PdkTargetPreparation(null, functions, OnFullLoad.FAIL, true, NODE, state);
            assertThatThrownBy(() -> preparation.prepare(TARGET, TargetTapTable.build(TARGET)))
                    .hasMessageContaining(createFails ? "create failed" : "count failed");
            assertThat(state.entries).isEmpty();
            functions.supportCreateTableV2((context, event) -> CreateTableOptions.create().tableExists(true));
            functions.supportCountByPartitionFilterFunction((context, table, filter) -> 0L);
            preparation.prepare(TARGET, TargetTapTable.build(TARGET));
            assertThat(state.entries).hasSize(1);
        }
    }

    @Test
    void aConnectorWithoutCreateTableStillGetsItsDeclaredIndex() throws Throwable {
        List<String> calls = new ArrayList<>();
        ConnectorFunctions functions = new ConnectorFunctions()
                .supportCreateIndex((context, table, event) -> calls.add("index"));
        new PdkTargetPreparation(null, functions, OnFullLoad.APPEND, true, NODE, new State())
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("index");
    }

    @Test
    void aCreatedPrimaryKeyRemainsRecognizedWhenAWriterIsReopened() throws Throwable {
        State state = new State();
        List<String> calls = new ArrayList<>();
        preparation(false, OnFullLoad.CLEAR, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        preparation(true, OnFullLoad.CLEAR, true, calls, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("create", "create");
    }

    private static PdkTargetPreparation preparation(boolean exists, OnFullLoad policy, boolean fullLoad,
            List<String> calls, State state) {
        return new PdkTargetPreparation(null, functions(exists, calls), policy, fullLoad, NODE, state);
    }

    private static ConnectorFunctions functions(boolean exists, List<String> calls) {
        ConnectorFunctions functions = new ConnectorFunctions();
        functions.supportCreateTableV2((context, event) -> {
            calls.add("create");
            return CreateTableOptions.create().tableExists(exists);
        });
        functions.supportClearTable((context, event) -> calls.add("clear"));
        functions.supportCreateIndex((context, table, event) -> calls.add("index"));
        return functions;
    }

    private static final class State implements KeyedStateStore {
        final Map<String, byte[]> entries = new HashMap<>();
        public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(entries.get(namespace + "/" + key));
        }
        public void save(String namespace, String key, byte[] state) { entries.put(namespace + "/" + key, state); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return Optional.ofNullable(entries.putIfAbsent(namespace + "/" + key, state));
        }
        public void delete(String namespace, String key) { entries.remove(namespace + "/" + key); }
        public void dropNamespace(String namespace) { entries.keySet().removeIf(key -> key.startsWith(namespace + "/")); }
        public long count(String namespace) { return entries.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count(); }
    }
}
