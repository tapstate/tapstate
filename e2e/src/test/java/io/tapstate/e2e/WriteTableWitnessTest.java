package io.tapstate.e2e;

import io.tapstate.e2e.mongowitness.WriteTableWitness;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteTableWitnessTest {
    @TempDir Path directory;

    @Test
    void forwards_the_same_arguments_and_appends_every_invocation_before_the_delegate_mutates_the_table() throws Throwable {
        Path witness = directory.resolve("writes.tsv");
        TapTable table = new TapTable("orders").add(new TapField("id", "int")).add(new TapField("name", "varchar"));
        List<TapRecordEvent> events = List.of(new TapInsertRecordEvent().after(Map.of("id", 1)));
        TapConnectorContext expectedContext = new TapConnectorContext(null, null, null, null);
        AtomicInteger calls = new AtomicInteger();
        Consumer<WriteListResult<TapRecordEvent>> consumer = ignored -> {};
        var decorated = WriteTableWitness.record(witness, (context, received, receivedTable, callback) -> {
            assertThat(context).isSameAs(expectedContext);
            assertThat(received).isSameAs(events);
            assertThat(receivedTable).isSameAs(table);
            assertThat(callback).isSameAs(consumer);
            calls.incrementAndGet();
            receivedTable.getNameFieldMap().remove("name");
        });
        decorated.writeRecord(expectedContext, events, table, consumer);
        decorated.writeRecord(expectedContext, events, table, consumer);
        assertThat(calls).hasValue(2);
        assertThat(Files.readAllLines(witness)).containsExactly("orders\t1\tid,name", "orders\t1\tid");
    }

    @Test
    void preserves_delegate_failure_and_records_the_attempt() {
        Path witness = directory.resolve("writes.tsv");
        AssertionError failure = new AssertionError("delegate failure");
        var decorated = WriteTableWitness.record(witness, (context, events, table, callback) -> { throw failure; });
        assertThatThrownBy(() -> decorated.writeRecord(null, List.of(), new TapTable("orders"), ignored -> {}))
                .isSameAs(failure);
        assertThat(witness).exists();
    }
}
