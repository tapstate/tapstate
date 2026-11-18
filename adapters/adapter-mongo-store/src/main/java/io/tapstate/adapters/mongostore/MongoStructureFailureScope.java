package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.OperationErrorDiagnostics;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/** Captures attribution during an operation and inspects only after its transaction has closed. */
final class MongoStructureFailureScope {
    private static final ThreadLocal<MongoStructureFailureScope> CURRENT = new ThreadLocal<>();
    private static final Set<String> COMMANDS = Set.of("find", "aggregate", "insert", "update", "delete", "findAndModify");
    private static final Set<Integer> STRUCTURE_ERRORS = Set.of(26, 27, 48, 85, 86, 121, 166, 171, 197, 11000);
    private final MongoClient client;
    private final MongoReadBudget budget;
    private MongoDatabase operatorDatabase;
    private Target pending;
    private Target failed;
    private int requestId;
    private int failedCode;
    private RuntimeException translated;
    private Throwable failedDriver;
    private record Target(MongoDatabase database, SystemCollections.Database domain, String physicalName, boolean hinted) {
        boolean structural(int code) { return STRUCTURE_ERRORS.contains(code) || hinted && code == 2; }
    }

    private MongoStructureFailureScope(MongoClient client, MongoReadBudget budget) {
        this.client = client; this.budget = budget;
    }

    static <T> T call(MongoClient client, MongoReadBudget budget, Supplier<T> operation) {
        if (!OperationErrorDiagnostics.active()) return operation.get();
        return inspectOnFailure(client, budget, operation);
    }

    /** Runtime operations without an HTTP scope carry the diagnosis in the original coded detail. */
    static <T> T inspectOnFailure(MongoClient client, Supplier<T> operation) {
        return inspectOnFailure(client, null, operation);
    }

    static <T> T inspectOnFailure(MongoClient client, MongoReadBudget budget, Supplier<T> operation) {
        var previous = CURRENT.get();
        var scope = new MongoStructureFailureScope(client, budget);
        CURRENT.set(scope);
        try { return operation.get(); }
        catch (RuntimeException original) {
            CURRENT.remove();
            if (original == scope.translated) throw scope.diagnose(original);
            throw original;
        } finally { if (previous == null) CURRENT.remove(); else CURRENT.set(previous); }
    }

    private RuntimeException diagnose(RuntimeException original) {
        if (failed == null) return original;
        try {
            var allowance = budget == null ? new MongoReadBudget(Duration.ofSeconds(5)) : budget;
            allowance.remainingMillis();
            var descriptors = Arrays.stream(SystemCollections.values())
                    .filter(table -> table.database() == failed.domain() && table.physicalNames().contains(failed.physicalName())).toList();
            if (descriptors.size() != 1 || descriptors.getFirst() == SystemCollections.CACHE_PIPELINE_RATE_HISTORY) return original;
            var namespaces = MongoStructureInspection.namespaces(descriptors, descriptors, Duration.ofDays(15));
            var namespace = namespaces.stream().filter(item -> item.physicalName().equals(failed.physicalName())).findFirst().orElseThrow();
            var differences = MongoTargetedStructureInspection.inspect(failed.database(), namespace, allowance);
            if (differences.isEmpty()) return original;
            String description = differences.stream().limit(3).map(MongoStructureInspection.Difference::description)
                    .collect(java.util.stream.Collectors.joining("; "));
            if (OperationErrorDiagnostics.active()) OperationErrorDiagnostics.confirmed(description);
            else if (original instanceof TapstateException coded && coded.code() == IoError.STORE_UNAVAILABLE)
                return coded.withoutCause(Map.of("detail", coded.args().get("detail") + ". " + description));
        } catch (RuntimeException unavailable) {
            // Supplementary diagnosis must never replace the original operation failure.
        }
        return original;
    }

    static void includeOperatorDatabase(MongoClient client, MongoDatabase database) {
        var scope = CURRENT.get();
        if (scope != null && scope.client == client) scope.operatorDatabase = database;
    }

    static void beginCallback() {
        var scope = CURRENT.get();
        if (scope != null) { scope.failed = null; scope.translated = null; scope.failedDriver = null; scope.failedCode = 0; scope.operatorDatabase = null; }
    }

    static void translated(MongoException failure, RuntimeException translated) {
        var scope = CURRENT.get();
        if (scope != null && scope.failed != null && scope.matchesDriverFailure(failure)) scope.translated = translated;
    }

    private boolean matchesDriverFailure(MongoException failure) {
        if (failure == failedDriver) return true;
        // The find operation re-wraps the same command response as MongoQueryException.
        if (failure instanceof com.mongodb.MongoQueryException query
                && failedDriver instanceof com.mongodb.MongoCommandException original)
            return query.getServerAddress().equals(original.getServerAddress()) && query.getResponse().equals(original.getResponse());
        return failedDriver == null && (failure instanceof com.mongodb.MongoWriteException write
                && write.getError().getCode() == failedCode
                || failure instanceof com.mongodb.MongoBulkWriteException bulk
                && bulk.getWriteErrors().stream().anyMatch(error -> error.getCode() == failedCode));
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
                var operator = scope.operatorDatabase;
                var database = operator != null && operator.getName().equals(event.getDatabaseName())
                        ? operator : originalDatabase.get();
                if (database == null || !database.getName().equals(event.getDatabaseName())) return;
                var target = event.getCommand().get(event.getCommandName());
                if (target == null || !target.isString()) return;
                scope.pending = new Target(database, domain, target.asString().getValue(), event.getCommand().containsKey("hint"));
                scope.requestId = event.getRequestId();
            }
            @Override public void commandFailed(CommandFailedEvent event) {
                var scope = CURRENT.get();
                if (scope != null && scope.client == originalClient.get() && scope.requestId == event.getRequestId()
                        && scope.pending != null && event.getThrowable() instanceof MongoException failure && scope.pending.structural(failure.getCode()))
                    { scope.failed = scope.pending; scope.failedCode = failure.getCode(); scope.failedDriver = failure; }
            }
            @Override public void commandSucceeded(CommandSucceededEvent event) {
                var scope = CURRENT.get();
                if (scope == null || scope.client != originalClient.get() || scope.pending == null || scope.requestId != event.getRequestId()) return;
                var errors = event.getResponse().getArray("writeErrors", null);
                if (errors != null) for (var value : errors) {
                    if (value.isDocument() && value.asDocument().get("code") instanceof org.bson.BsonNumber number
                            && scope.pending.structural(number.intValue())) {
                        scope.failed = scope.pending; scope.failedCode = number.intValue(); break;
                    }
                }
            }
        };
    }
}
