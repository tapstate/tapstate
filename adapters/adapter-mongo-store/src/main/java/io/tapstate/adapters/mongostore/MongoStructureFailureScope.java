package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.OperationErrorDiagnostics;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;

/** Captures attribution during an operation and inspects only after its transaction has closed. */
final class MongoStructureFailureScope {
    private static final ThreadLocal<MongoStructureFailureScope> CURRENT = new ThreadLocal<>();
    private static final Set<String> COMMANDS = Set.of("find", "aggregate", "insert", "update", "delete", "findAndModify");
    private static final Set<Integer> STRUCTURE_ERRORS = Set.of(26, 27, 48, 85, 86, 121, 166, 171, 197, 11000);
    private final MongoClient client;
    private final MongoReadBudget budget;
    private Target pending;
    private Target failed;
    private int requestId;
    private int failedCode;
    private RuntimeException translated;
    private Throwable failedDriver;
    private record Target(MongoDatabase database, MongoStructureInspection.Namespace namespace) {}

    private MongoStructureFailureScope(MongoClient client, MongoReadBudget budget) {
        this.client = client; this.budget = budget;
    }

    static <T> T call(MongoClient client, MongoReadBudget budget, Supplier<T> operation) {
        if (!OperationErrorDiagnostics.active()) return operation.get();
        var previous = CURRENT.get();
        var scope = new MongoStructureFailureScope(client, budget);
        CURRENT.set(scope);
        try { return operation.get(); }
        catch (RuntimeException original) {
            CURRENT.remove();
            if (original == scope.translated)
                scope.inspect();
            throw original;
        } finally { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); }
    }

    private void inspect() {
        if (failed == null) return;
        try {
            budget.remainingMillis();
            var differences = MongoTargetedStructureInspection.inspect(failed.database(), failed.namespace(), budget);
            if (!differences.isEmpty()) OperationErrorDiagnostics.confirmed(differences.stream().limit(3)
                    .map(MongoStructureInspection.Difference::description).collect(java.util.stream.Collectors.joining("; ")));
        } catch (RuntimeException unavailable) {
            // Supplementary diagnosis must never replace or extend the original operation's allowance.
        }
    }

    static void beginCallback() {
        var scope = CURRENT.get();
        if (scope != null) { scope.failed = null; scope.translated = null; scope.failedDriver = null; scope.failedCode = 0; }
    }

    static void translated(MongoException failure, RuntimeException translated) {
        var scope = CURRENT.get();
        if (scope != null && scope.failed != null && (failure == scope.failedDriver
                || scope.failedDriver == null && (failure instanceof com.mongodb.MongoWriteException write
                && write.getError().getCode() == scope.failedCode
                || failure instanceof com.mongodb.MongoBulkWriteException bulk
                && bulk.getWriteErrors().stream().anyMatch(error -> error.getCode() == scope.failedCode))))
            scope.translated = translated;
    }

    static CommandListener listener(Supplier<MongoClient> originalClient, Supplier<MongoDatabase> originalDatabase,
            SystemCollections.Database domain) {
        return new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                var scope = CURRENT.get();
                if (scope == null || scope.client != originalClient.get()) return;
                scope.pending = null;
                if (!COMMANDS.contains(event.getCommandName())) return;
                scope.failed = null; scope.translated = null; scope.failedDriver = null; scope.failedCode = 0;
                var database = originalDatabase.get();
                if (database == null || !database.getName().equals(event.getDatabaseName())) return;
                var target = event.getCommand().get(event.getCommandName());
                if (target == null || !target.isString()) return;
                String name = target.asString().getValue();
                var descriptors = Arrays.stream(SystemCollections.values())
                        .filter(table -> table.database() == domain && table.physicalNames().contains(name)).toList();
                if (descriptors.size() != 1 || descriptors.getFirst() == SystemCollections.CACHE_PIPELINE_RATE_HISTORY) return;
                var namespaces = MongoStructureInspection.namespaces(descriptors, descriptors, Duration.ofDays(15));
                var namespace = namespaces.stream().filter(item -> item.physicalName().equals(name)).findFirst().orElseThrow();
                scope.pending = new Target(database, namespace);
                scope.requestId = event.getRequestId();
            }
            @Override public void commandFailed(CommandFailedEvent event) {
                var scope = CURRENT.get();
                if (scope != null && scope.client == originalClient.get() && scope.requestId == event.getRequestId()
                        && event.getThrowable() instanceof MongoException failure && STRUCTURE_ERRORS.contains(failure.getCode()))
                    { scope.failed = scope.pending; scope.failedCode = failure.getCode(); scope.failedDriver = failure; }
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                var scope = CURRENT.get();
                if (scope == null || scope.client != originalClient.get() || scope.requestId != event.getRequestId()) return;
                var errors = event.getResponse().getArray("writeErrors", null);
                if (errors != null) for (var value : errors) {
                    if (value.isDocument() && value.asDocument().get("code") instanceof org.bson.BsonNumber number
                            && STRUCTURE_ERRORS.contains(number.intValue())) {
                        scope.failed = scope.pending; scope.failedCode = number.intValue(); break;
                    }
                }
            }
        };
    }
}
