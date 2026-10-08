package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkPreparationNamespace;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoTargetPreparationTest {
    private static final PipelineNode NODE = new PipelineNode("pipeline", "sink");
    private static final TargetTable TARGET = new TargetTable("orders",
            List.of(new TargetField("id", "bigint", true)), List.of());

    @ParameterizedTest
    @ValueSource(strings = {"mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb"})
    void failCountsBeforeCreateEvenWhenTheConnectorReportsEveryCollectionAsNew(String id) {
        List<String> calls = new ArrayList<>();
        HeapKeyedState state = new HeapKeyedState();
        ConnectorFunctions functions = functions(calls);
        functions.supportCountByPartitionFilterFunction((context, table, filter) -> {
            calls.add("count");
            assertThat(filter.getMatch()).isNullOrEmpty();
            assertThat(filter.getOperators()).isNullOrEmpty();
            return 1L;
        });

        assertThatThrownBy(() -> preparation(id, functions, OnFullLoad.FAIL, true, state)
                .prepare(TARGET, TargetTapTable.build(TARGET)))
                .hasMessageContaining("orders").hasMessageContaining("not empty");
        assertThat(calls).containsExactly("count");
        assertThat(state.count(SinkPreparationNamespace.of(NODE))).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb"})
    void clearDeletesDocumentsBeforeCreateAndTheReceiptPreventsASecondClear(String id) throws Throwable {
        List<String> calls = new ArrayList<>();
        HeapKeyedState state = new HeapKeyedState();
        ConnectorFunctions functions = functions(calls);
        functions.supportExecuteCommandFunction((context, command, consumer) -> {
            calls.add("clear");
            assertThat(command.getCommand()).isEqualTo("execute");
            assertThat(command.getParams()).containsExactlyInAnyOrderEntriesOf(Map.of(
                    "op", "delete", "collection", "orders", "filter", Map.of("$expr", true)));
            command.getParams().put("database", "configured_database");
            consumer.accept(new ExecuteResult<Long>().result(3L));
        });

        var preparation = preparation(id, functions, OnFullLoad.CLEAR, true, state);
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        preparation(id, functions, OnFullLoad.CLEAR, true, state)
                .prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("clear", "create", "create");
        assertThat(state.count(SinkPreparationNamespace.of(NODE))).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"reported-error", "negative-result", "no-result", "thrown-error"})
    void aFailedClearLeavesNoReceiptAndCanBeRetried(String outcome) throws Throwable {
        List<String> calls = new ArrayList<>();
        HeapKeyedState state = new HeapKeyedState();
        ConnectorFunctions functions = functions(calls);
        functions.supportExecuteCommandFunction((context, command, consumer) -> {
            calls.add("clear");
            switch (outcome) {
                case "reported-error" -> {
                    consumer.accept(new ExecuteResult<Long>().result(0L));
                    consumer.accept(new ExecuteResult<Long>().error(new IllegalStateException("clear failed")));
                }
                case "negative-result" -> consumer.accept(new ExecuteResult<Long>().result(-1L));
                case "no-result" -> { }
                case "thrown-error" -> throw new IllegalStateException("clear failed");
                default -> throw new AssertionError(outcome);
            }
        });
        var preparation = preparation("mongodb", functions, OnFullLoad.CLEAR, true, state);
        assertThatThrownBy(() -> preparation.prepare(TARGET, TargetTapTable.build(TARGET)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(calls).containsExactly("clear");
        assertThat(state.count(SinkPreparationNamespace.of(NODE))).isZero();

        functions.supportExecuteCommandFunction((context, command, consumer) -> {
            calls.add("clear");
            consumer.accept(new ExecuteResult<Long>().result(0L));
        });
        preparation.prepare(TARGET, TargetTapTable.build(TARGET));
        assertThat(calls).containsExactly("clear", "clear", "create");
        assertThat(state.count(SinkPreparationNamespace.of(NODE))).isEqualTo(1);
    }

    @Test
    void anUnknownCountIsRefusedBeforeCreateWithoutLeavingAReceipt() {
        List<String> calls = new ArrayList<>();
        HeapKeyedState state = new HeapKeyedState();
        ConnectorFunctions functions = functions(calls);
        functions.supportCountByPartitionFilterFunction((context, table, filter) -> -1L);
        assertThatThrownBy(() -> preparation("mongodb", functions, OnFullLoad.FAIL, true, state)
                .prepare(TARGET, TargetTapTable.build(TARGET))).hasMessageContaining("row count is unknown");
        assertThat(calls).isEmpty();
        assertThat(state.count(SinkPreparationNamespace.of(NODE))).isZero();
    }

    @Test
    void appendAndCdcOnlyDoNotCountOrClear() throws Throwable {
        for (OnFullLoad policy : OnFullLoad.values()) {
            for (boolean fullLoad : List.of(false, true)) {
                if (fullLoad && policy != OnFullLoad.APPEND) {
                    continue;
                }
                List<String> calls = new ArrayList<>();
                preparation("mongodb", functions(calls), policy, fullLoad, new HeapKeyedState())
                        .prepare(TARGET, TargetTapTable.build(TARGET));
                assertThat(calls).containsExactly("create");
            }
        }
    }

    @Test
    void anotherConnectorNeverReceivesAMongoDeleteCommand() {
        List<String> calls = new ArrayList<>();
        ConnectorFunctions functions = functions(calls);
        functions.supportCreateTableV2((context, event) -> CreateTableOptions.create().tableExists(true));
        functions.supportExecuteCommandFunction((context, command, consumer) -> {
            throw new AssertionError("a non-Mongo target must not receive a Mongo command");
        });
        assertThatThrownBy(() -> preparation("postgres", functions, OnFullLoad.CLEAR, true, new HeapKeyedState())
                .prepare(TARGET, TargetTapTable.build(TARGET))).hasMessageContaining("no clear-table function");
    }

    private static PdkTargetPreparation preparation(String id, ConnectorFunctions functions, OnFullLoad policy,
            boolean fullLoad, HeapKeyedState state) {
        return new PdkTargetPreparation(id, null, functions, policy, fullLoad, NODE, state);
    }

    private static ConnectorFunctions functions(List<String> calls) {
        return new ConnectorFunctions().supportCreateTableV2((context, event) -> {
            calls.add("create");
            return CreateTableOptions.create().tableExists(false);
        });
    }
}
