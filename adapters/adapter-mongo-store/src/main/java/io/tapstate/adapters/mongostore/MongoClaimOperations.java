package io.tapstate.adapters.mongostore;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.bson.BsonDocument;
import org.bson.Document;

/** Actual task and session ownership survives a bounded claim caller. */
final class MongoClaimOperations {
    private final MongoClient client;
    private final ThreadPoolExecutor workers = new ThreadPoolExecutor(4, 4, 100, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), action -> {
                var thread = new Thread(action, "mongo-claim-io");
                thread.setDaemon(true);
                return thread;
            });
    private final Set<Call<?>> calls = ConcurrentHashMap.newKeySet();
    private final Set<Transaction> transactions = ConcurrentHashMap.newKeySet();
    private final ThreadLocal<MongoReadBudget> current = new ThreadLocal<>();
    private final ThreadLocal<Runnable> active = new ThreadLocal<>();
    private boolean closed;
    private boolean stopping;
    private volatile boolean replicaSet;
    private volatile boolean routed;

    MongoClaimOperations(MongoClient client) {
        this.client = client;
        observeTopology();
        workers.allowCoreThreadTimeOut(true);
    }

    MongoReadBudget budget() { return current.get(); }
    private void observeTopology() {
        var topology = client.getClusterDescription();
        if (topology.getType() == com.mongodb.connection.ClusterType.REPLICA_SET) {
            replicaSet = true;
        } else if (topology.getType() != com.mongodb.connection.ClusterType.UNKNOWN) {
            routed = true;
        }
    }
    void check() {
        var budget = current.get();
        if (budget != null) budget.remainingMillis();
        var guard = active.get();
        if (guard != null) guard.run();
    }
    synchronized void requireAdmission() {
        if(stopping || closed)throw MongoCacheWorkloadClaimStore.claimFenced();
    }

    <T> T call(long deadline, Supplier<T> action) {
        return call(deadline,()->{},action);
    }

    <T> T call(long deadline,Runnable guard,Supplier<T> action) {
        requireAdmission();
        var inherited = current.get();
        if (inherited != null) {
            var previous=active.get();
            long selected = deadline - inherited.deadlineNanos() < 0 ? deadline : inherited.deadlineNanos();
            current.set(MongoReadBudget.until(selected));
            active.set(()->{if(previous!=null)previous.run();guard.run();});
            try {check();var result=action.get();check();return result;}
            finally { current.set(inherited);active.set(previous); }
        }
        return result(submit(deadline, guard,action,false), deadline);
    }

    private synchronized <T> Call<T> submit(long deadline,Runnable guard,Supplier<T> action,boolean cleanup) {
        if (closed || stopping&&!cleanup) throw MongoCacheWorkloadClaimStore.claimFenced();
        remaining(deadline);
        var call = new Call<T>(deadline,guard, action);
        calls.add(call);
        try { workers.execute(call); }
        catch (RuntimeException rejected) { calls.remove(call); notifyAll(); throw rejected; }
        return call;
    }

    private <T> T result(Call<T> call, long deadline) {
        try { return call.result.get(remaining(deadline), TimeUnit.NANOSECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            call.cancelPending();
            throw MongoCacheWorkloadClaimStore.claimFenced();
        } catch (TimeoutException expired) {
            call.cancelPending();
            throw MongoCacheWorkloadClaimStore.claimFenced();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof Error error) throw error;
            if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("claim operation produced a checked failure", failed.getCause());
        } catch (RuntimeException expired) {
            call.cancelPending();
            throw expired;
        }
    }

    private static long remaining(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || Thread.currentThread().isInterrupted())
            throw MongoCacheWorkloadClaimStore.claimFenced();
        return remaining;
    }

    private final class Call<T> implements Runnable {
        private final long deadline;
        private final Supplier<T> action;
        private final Runnable guard;
        private final AtomicInteger state = new AtomicInteger();
        private final CompletableFuture<T> result = new CompletableFuture<>();

        private Call(long deadline,Runnable guard, Supplier<T> action) { this.deadline = deadline;this.guard=guard;this.action = action; }

        void cancelPending() {
            if (state.compareAndSet(0, 2)) {
                finished();
                result.completeExceptionally(MongoCacheWorkloadClaimStore.claimFenced());
            }
        }

        private void finished() {
            calls.remove(this);
            synchronized (MongoClaimOperations.this) { MongoClaimOperations.this.notifyAll(); }
        }

        @Override public void run() {
            if (!state.compareAndSet(0, 1)) return;
            T value=null;Throwable failure=null;
            try {
                current.set(MongoReadBudget.until(deadline));
                active.set(guard);check();
                value=action.get();check();
            } catch (Throwable failed) { failure=failed; }
            finally { current.remove();active.remove(); state.set(2); finished(); }
            if(failure==null)result.complete(value);else result.completeExceptionally(failure);
        }
    }

    final class Transaction {
        private final ClientSession session;
        private long number;
        private BsonDocument recoveryToken;
        private Call<Void> cleanup;
        private boolean ended;
        private boolean captured;
        private boolean callbackReturned;
        private boolean released;
        private boolean closeAttempted;

        private Transaction(ClientSession session) { this.session = session; }

        void capture() {
            observeTopology();
            if(session.getPinnedServerAddress()!=null)routed=true;
            if(!captured) {
                number = session.getServerSession().getTransactionNumber();
                captured=true;
            }
            var token = session.getRecoveryToken();
            if (token != null) recoveryToken = token.clone();
        }
        boolean ended() { return ended; }
        boolean released() { return released; }
        boolean closeAttempted() { return closeAttempted; }
        void beginAttempt() {
            if(callbackReturned&&!ended)throw new RetryAfterCommit();
            captured=false;ended=false;callbackReturned=false;recoveryToken=null;
            capture();
        }
        void returned() {capture();callbackReturned=true;}
    }

    Transaction own(ClientSession session) {
        var transaction = new Transaction(session);
        transactions.add(transaction);
        return transaction;
    }

    void committed(Transaction transaction) {
        transaction.ended=true;
        if(current.get()!=null)current.get().remainingMillis();
        transaction.closeAttempted=true;
        transaction.session.close();
        transaction.released=true;
        transactions.remove(transaction);
    }

    /** A driver callback restart after commit first reconciles the previous exact transaction. */
    static final class RetryAfterCommit extends RuntimeException {
        RetryAfterCommit(){super(null,null,false,false);}
    }

    void abortCurrent(Transaction transaction,MongoReadBudget budget) {
        budget.remainingMillis();transaction.capture();
        abortCommand(transaction,budget,true);
    }

    /** An explicit response ends this exact transaction; driver-local state is not evidence. */
    void abort(Transaction transaction, MongoReadBudget budget) {
        budget.remainingMillis();
        if(transaction.ended){committed(transaction);return;}
        transaction.capture();
        if (transaction.session.hasActiveTransaction()) transaction.session.abortTransaction();
        budget.remainingMillis();
        // A failed commit keeps its expired driver timeout. This empty local transaction sends no
        // commands and clears that context; the original server number and recovery token stay owned.
        transaction.session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                .timeout(budget.remainingMillis(),TimeUnit.MILLISECONDS).build());
        transaction.session.abortTransaction();
        budget.remainingMillis();
        abortCommand(transaction,budget,false);
        committed(transaction);
    }

    private void abortCommand(Transaction transaction,MongoReadBudget budget,boolean active) {
        var command = new Document("abortTransaction", 1)
                .append("writeConcern", new Document("w", "majority").append("j", true));
        if(!active)command.append("txnNumber", transaction.number).append("autocommit",false);
        if (transaction.recoveryToken != null) command.append("recoveryToken", transaction.recoveryToken);
        try {
            var database=client.getDatabase("admin").withReadPreference(ReadPreference.primary());
            // An active transaction already owns the original CSOT. Overriding it is invalid.
            var reply = (active?database:budget.database(database)).runCommand(transaction.session, command);
            if (reply.containsKey("writeConcernError"))
                throw new MongoException(64, "transaction cleanup was not acknowledged");
        } catch (MongoCommandException ended) {
            observeTopology();
            // The server has no active transaction under this exact session and transaction number.
            if (ended.getResponse().containsKey("writeConcernError")
                    || ended.getErrorCode() != 251 && ended.getErrorCode() != 256) throw ended;
            // Without a router recovery token, another mongos cannot establish the old outcome.
            if(ended.getErrorCode()==251 && (transaction.recoveryToken==null||transaction.recoveryToken.isEmpty())
                    && (routed||!replicaSet))throw ended;
        }
        transaction.ended=true;
    }

    void await(long deadline) {
        if (current.get() != null) throw MongoCacheWorkloadClaimStore.claimFenced();
        for (;;) {
            synchronized (this) {
                while (!calls.isEmpty()) {
                    try { TimeUnit.NANOSECONDS.timedWait(this, remaining(deadline)); }
                    catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw MongoCacheWorkloadClaimStore.claimFenced();
                    }
                }
            }
            var unresolved = new ArrayList<>(transactions);
            if (unresolved.isEmpty()) return;
            for (var transaction : unresolved) {
                Call<Void> cleanup;
                synchronized (transaction) {
                    if (!transactions.contains(transaction)) continue;
                    if (transaction.cleanup == null || transaction.cleanup.result.isDone())
                        transaction.cleanup = submit(deadline,()->{}, () -> { abort(transaction, budget()); return null; },true);
                    cleanup = transaction.cleanup;
                }
                result(cleanup, deadline);
            }
        }
    }

    synchronized void closeIfIdle() {
        stopping=true;
        if (!calls.isEmpty() || !transactions.isEmpty()) throw MongoCacheWorkloadClaimStore.claimFenced();
        closed = true;
        workers.shutdown();
    }
}
