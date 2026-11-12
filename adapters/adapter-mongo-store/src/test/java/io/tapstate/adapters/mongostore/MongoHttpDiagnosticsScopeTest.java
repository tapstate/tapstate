package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.mongodb.event.*;
import io.tapstate.core.common.JsonWriter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

class MongoHttpDiagnosticsScopeTest {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> values(Object value) { return (Map<String, Object>) value; }

    private CommandStartedEvent started(int id, String database, String table) {
        var event = mock(CommandStartedEvent.class);
        when(event.getRequestId()).thenReturn(id);
        when(event.getCommandName()).thenReturn("find");
        when(event.getDatabaseName()).thenReturn(database);
        when(event.getCommand()).thenReturn(new BsonDocument("find", new BsonString(table))
                .append("filter", new BsonDocument("password", new BsonString("secret-filter-sentinel"))));
        return event;
    }

    @Test void disabledScopesDoNotInspectEventsOrInstallLegacyByteObservers() {
        var event = mock(CommandStartedEvent.class);
        try (var ignored = MongoHttpDiagnosticsScope.phase(MongoHttpDiagnosticsScope.Phase.AUTH_VERIFY)) {
            MongoHttpDiagnosticsScope.started(event);
        }
        verifyNoInteractions(event);
        assertThat(MongoCommandObservation.BYTES.get()).isNull();
    }

    @Test void countsActualDriverCompletionsWithoutRetainingCommandInputs() {
        try (var scope = MongoHttpDiagnosticsScope.open()) {
            assertThat(MongoCommandObservation.BYTES.get()).isNull();
            try (var ignored = MongoHttpDiagnosticsScope.phase(MongoHttpDiagnosticsScope.Phase.AUTH_VERIFY)) {
                MongoCommandObservation.LISTENER.commandStarted(started(1, "config", "sources"));
                var success = mock(CommandSucceededEvent.class);
                when(success.getRequestId()).thenReturn(1);
                when(success.getElapsedTime(TimeUnit.NANOSECONDS)).thenReturn(2_000_000L);
                MongoCommandObservation.LISTENER.commandSucceeded(success);
                MongoCommandObservation.LISTENER.commandStarted(started(2, "config", "sources"));
                var failure = mock(CommandFailedEvent.class);
                when(failure.getRequestId()).thenReturn(2);
                when(failure.getElapsedTime(TimeUnit.NANOSECONDS)).thenReturn(5_000_000L);
                MongoCommandObservation.LISTENER.commandFailed(failure);
            }
            var result = scope.snapshot();
            assertThat(result).containsEntry("complete", true);
            assertThat(values(result.get("mongo"))).containsEntry("started", 2L)
                    .containsEntry("completed", 2L).containsEntry("failures", 1L).containsEntry("driver_ms", 7.0);
            assertThat(JsonWriter.write(result)).contains("config", "sources", "auth_verify")
                    .doesNotContain("secret-filter-sentinel", "password");
        }
        var outside = mock(CommandStartedEvent.class);
        MongoHttpDiagnosticsScope.started(outside);
        verifyNoInteractions(outside);
    }

    @Test void checkoutWaitIsReportedSeparatelyFromDriverCommands() {
        try (var scope = MongoHttpDiagnosticsScope.open()) {
            MongoHttpDiagnosticsScope.POOL_LISTENER.connectionCheckOutStarted(mock(ConnectionCheckOutStartedEvent.class));
            var checked = mock(ConnectionCheckedOutEvent.class);
            when(checked.getElapsedTime(TimeUnit.NANOSECONDS)).thenReturn(4_000_000L);
            MongoHttpDiagnosticsScope.POOL_LISTENER.connectionCheckedOut(checked);
            MongoHttpDiagnosticsScope.POOL_LISTENER.connectionCheckOutStarted(mock(ConnectionCheckOutStartedEvent.class));
            var failed = mock(ConnectionCheckOutFailedEvent.class);
            when(failed.getElapsedTime(TimeUnit.NANOSECONDS)).thenReturn(7_000_000L);
            MongoHttpDiagnosticsScope.POOL_LISTENER.connectionCheckOutFailed(failed);
            var value = scope.snapshot();
            assertThat(value).containsEntry("complete", true);
            assertThat(values(value.get("pool"))).containsEntry("started", 2L).containsEntry("completed", 2L)
                    .containsEntry("failures", 1L).containsEntry("checkout_ms", 11.0);
            assertThat(values(value.get("mongo"))).containsEntry("started", 0L);
        }
    }

    @Test void nestedScopesRestoreTheOriginalRequestAndStage() {
        try (var outer = MongoHttpDiagnosticsScope.open()) {
            try (var ignored = MongoHttpDiagnosticsScope.phase(MongoHttpDiagnosticsScope.Phase.AUTH_ATTEMPT)) {
                try (var inner = MongoHttpDiagnosticsScope.open()) {
                    MongoHttpDiagnosticsScope.started(started(1, "inner", "sources"));
                    MongoHttpDiagnosticsScope.finished(1, 1_000_000, false);
                    assertThat(values(inner.snapshot().get("mongo"))).containsEntry("completed", 1L);
                }
                MongoHttpDiagnosticsScope.started(started(2, "outer", "sources"));
                MongoHttpDiagnosticsScope.finished(2, 2_000_000, false);
            }
            var value = outer.snapshot();
            assertThat(value).containsEntry("complete", true);
            assertThat(JsonWriter.write(value)).contains("outer", "auth_attempt").doesNotContain("inner");
        }
    }

    @Test void missingCompletionsAreVisibleAndBackgroundWorkDoesNotJoinTheRequest() throws Exception {
        try (var scope = MongoHttpDiagnosticsScope.open()) {
            var background = new Thread(() -> {
                MongoHttpDiagnosticsScope.started(started(1, "background", "sources"));
                MongoHttpDiagnosticsScope.finished(1, 2_000_000, false);
            });
            background.start(); background.join();
            MongoHttpDiagnosticsScope.started(started(2, "request", "sources"));
            var value = scope.snapshot();
            assertThat(value).containsEntry("complete", false).containsEntry("pending_commands", 1);
            assertThat(values(value.get("mongo"))).containsEntry("started", 1L);
            assertThat(JsonWriter.write(value)).doesNotContain("background");
        }
    }

    @Test void groupCardinalityAndLogFieldsAreBoundedWithoutLosingTotals() {
        try (var scope = MongoHttpDiagnosticsScope.open()) {
            for (int i = 0; i < 50; i++) {
                MongoHttpDiagnosticsScope.started(started(i, "db\n" + i, "table" + "x".repeat(200)));
                MongoHttpDiagnosticsScope.finished(i, 1, false);
            }
            var value = scope.snapshot();
            assertThat(value).containsEntry("complete", true).containsEntry("grouped_as_other", 18);
            assertThat(values(value.get("mongo"))).containsEntry("completed", 50L);
            assertThat(JsonWriter.write(value)).doesNotContain("\\n", "x".repeat(97));
        }
    }
}
