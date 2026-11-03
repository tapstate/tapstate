package io.tapstate.adapters.mongostore;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** One monotonic allowance shared by a read transaction and its non-session metadata checks. */
public final class MongoReadBudget {
    private static final long MAX_OPERATION_MILLIS = 5000;
    private final long started = System.nanoTime();
    private final long nanos;
    private boolean retryCallbacks=true;
    MongoReadBudget withoutCallbackRetries(){retryCallbacks=false;return this;}

    public MongoReadBudget(Duration budget) {
        Objects.requireNonNull(budget, "budget");
        if (budget.isNegative() || budget.isZero()) throw new IllegalArgumentException("read budget must be positive");
        nanos = budget.toNanos();

    }

    public void requireRemaining() { StoreIo.run(this::remainingMillis); }

    long remainingMillis() {
        long left = nanos - (System.nanoTime() - started);
        if (left <= 0) throw new MongoOperationTimeoutException("read budget exhausted");
        return Math.max(1L, TimeUnit.NANOSECONDS.toMillis(left));
    }

    MongoDatabase database(MongoDatabase original) {
        return original.withTimeout(operationMillis(), TimeUnit.MILLISECONDS);
    }

    long operationMillis() { return Math.min(MAX_OPERATION_MILLIS, remainingMillis()); }

    <T> T transaction(MongoClient client, Function<ClientSession, T> read) {
        if(!retryCallbacks)return maintenanceTransaction(client,read);
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    remainingMillis();
                    T value = read.apply(session);
                    remainingMillis();
                    return value;
                }, TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                        .timeout(operationMillis(), TimeUnit.MILLISECONDS).build());
            }
        });
    }
    /** Private control flow carries no driver response, message or cause across the store boundary. */
    static final class MaintenanceConflict extends RuntimeException {
        MaintenanceConflict(){super(null,null,false,false);}
    }
    /** Bounded maintenance yields on a conflict; it never lets callback retries hide work from its budget. */
    <T> T maintenanceTransaction(MongoClient client,Function<ClientSession,T> action) {
        return StoreIo.call(()->{
            try(var session=client.startSession()) {
                session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true)).timeout(operationMillis(),TimeUnit.MILLISECONDS).build());
                try {remainingMillis();T result=action.apply(session);remainingMillis();session.commitTransaction();return result;}
                catch(RuntimeException failure){if(session.hasActiveTransaction())try{session.abortTransaction();}catch(RuntimeException ignored){failure.addSuppressed(ignored);}
                    if(failure instanceof com.mongodb.MongoException mongo && !mongo.hasErrorLabel("UnknownTransactionCommitResult")
                            && (mongo.getCode()==112 || mongo.hasErrorLabel("TransientTransactionError")))throw new MaintenanceConflict();
                    throw failure;}
            }
        });
    }

}
