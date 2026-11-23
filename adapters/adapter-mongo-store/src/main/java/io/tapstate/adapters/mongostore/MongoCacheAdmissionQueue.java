package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.TransactionOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.connection.ConnectionId;
import org.bson.BsonDocument;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicBoolean;
import io.tapstate.spi.store.CacheBusinessScope;
import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/** Local turns for one CACHE identity; Mongo transactions retain every authoritative fence. */
final class MongoCacheAdmissionQueue {
    private record Key(String database, CacheBusinessScope scope) {}
    private static final Map<MongoClient, Map<Key, MongoCacheAdmissionQueue>> QUEUES = new WeakHashMap<>();
    private record TransactionKey(MongoClient client, BsonDocument session, long number) {}
    private record RequestKey(MongoClient client, ConnectionId connection, int request) {}
    private record Finalization(TransactionKey key, MongoClaimOperations.Transaction owner, boolean commit) {}
    private static final Map<TransactionKey, MongoClaimOperations.Transaction> TRACKED = new ConcurrentHashMap<>();
    private static final Map<RequestKey, Finalization> FINALIZATIONS = new ConcurrentHashMap<>();
    private static final java.util.Set<MongoClient> OBSERVED = java.util.Collections.synchronizedSet(
            java.util.Collections.newSetFromMap(new WeakHashMap<>()));
    private static final ThreadLocal<Ticket> CURRENT = new ThreadLocal<>();
    private final WeakReference<MongoClient> client;
    private final WeakReference<MongoClaimOperations> operations;
    private boolean closed;
    private final ArrayDeque<Waiter> first = new ArrayDeque<>(), other = new ArrayDeque<>();
    private Ticket held;
    private boolean lastFirst;
    private static final class Waiter {}

    static synchronized void bind(MongoClient client, String database, CacheBusinessScope scope, MongoClaimOperations operations) {
        var entries = QUEUES.computeIfAbsent(client, ignored -> new HashMap<>());
        var key = new Key(database, scope);
        var existing = entries.get(key);
        if (existing != null) {
            if (existing.operations.get() != operations) throw new IllegalStateException("CACHE admission already belongs to another operation owner");
            return;
        }
        entries.put(key, new MongoCacheAdmissionQueue(client, operations));
    }
    private static synchronized MongoCacheAdmissionQueue find(MongoClient client, String database, CacheBusinessScope scope) {
        var queues = QUEUES.get(client); return queues == null ? null : queues.get(new Key(database, scope));
    }
    static boolean firstCurrent(MongoClient client) { var ticket = current(client); return ticket != null && ticket.firstRole; }
    static int waiting(MongoClient client, String database, CacheBusinessScope scope, boolean priority) {
        var queue = find(client, database, scope); if (queue == null) return 0;
        synchronized (queue) { return priority ? queue.first.size() : queue.other.size(); }
    }
    static boolean bound(MongoClient client, String database, CacheBusinessScope scope) { return find(client, database, scope) != null; }
    static boolean held(MongoClient client, String database, CacheBusinessScope scope) {
        var queue = find(client, database, scope); if (queue == null) return false;
        synchronized (queue) { return queue.held != null; }
    }
    static synchronized void unbind(MongoClient client) {
        var entries = QUEUES.get(client);
        if (entries == null) return;
        for (var queue : entries.values()) synchronized (queue) {
            if (queue.held != null || !queue.first.isEmpty() || !queue.other.isEmpty()) throw MongoCacheWorkloadClaimStore.claimFenced();
            queue.closed = true; queue.notifyAll();
        }
        QUEUES.remove(client);
    }
    private MongoCacheAdmissionQueue(MongoClient client, MongoClaimOperations operations) {
        this.client = new WeakReference<>(client); this.operations = new WeakReference<>(operations);
    }
    static Ticket current(MongoClient original) {
        var held = CURRENT.get(); return held != null && !held.slotReleased.get() && held.owner.client.get() == original ? held : null;
    }
    static Ticket install(Ticket ticket) { var before = CURRENT.get(); if (ticket == null) CURRENT.remove(); else CURRENT.set(ticket); return before; }
    static void restore(Ticket before) { if (before == null) CURRENT.remove(); else CURRENT.set(before); }
    static MongoClaimOperations operations(MongoClient original) {
        var ticket = current(original); return ticket == null ? null : ticket.owner.operations.get();
    }
    static <T> T first(MongoClient client, String database, CacheBusinessScope scope, Runnable active, Supplier<T> action) {
        return first(client, database, scope, Long.MAX_VALUE, active, action);
    }
    static <T> T first(MongoClient client, String database, CacheBusinessScope scope, long deadline, Runnable active, Supplier<T> action) {
        var queue = find(client, database, scope); return queue == null ? action.get() : StoreIo.call(() -> queue.call(true, deadline, active, action));
    }
    static <T> T other(MongoClient client, String database, CacheBusinessScope scope, long deadline, Runnable active, Supplier<T> action) {
        var queue = find(client, database, scope); return queue == null ? action.get() : StoreIo.call(() -> queue.call(false, deadline, active, action));
    }
    private <T> T call(boolean priority, long deadline, Runnable active, Supplier<T> action) {
        var original = CURRENT.get();
        if (original != null && !original.slotReleased.get() && original.owner == this) return action.get();
        var ticket = enter(priority, deadline, active);
        CURRENT.set(ticket);
        try { active.run(); return action.get(); }
        finally { restore(original); ticket.entryFinished(); }
    }
    private Ticket enter(boolean priority, long deadline, Runnable active) {
        var waiter = new Waiter();
        synchronized (this) { if (closed) throw MongoCacheWorkloadClaimStore.claimFenced(); (priority ? first : other).addLast(waiter); }
        boolean entered = false;
        try {
            for (;;) {
                active.run();
                if (deadline != Long.MAX_VALUE) MongoReadBudget.until(deadline).remainingMillis();
                Ticket recovery;
                synchronized (this) {
                    if (closed) throw MongoCacheWorkloadClaimStore.claimFenced();
                    var next = !first.isEmpty() && (!lastFirst || other.isEmpty()) ? first.peekFirst() : other.peekFirst();
                    if (next == null) next = first.peekFirst();
                    if (held == null && next == waiter) {
                        (priority ? first : other).removeFirst(); lastFirst = priority;
                        held = new Ticket(this, priority); entered = true; return held;
                    }
                    recovery = held != null && held.recoverable() ? held : null;
                    if (recovery == null) {
                        long left = deadline == Long.MAX_VALUE ? 50 : Math.min(50, MongoReadBudget.until(deadline).remainingMillis());
                        try { wait(left); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw MongoCacheWorkloadClaimStore.claimFenced(); }
                    }
                }
                if (recovery != null) recovery.recover(Math.min(deadline, System.nanoTime() + TimeUnit.SECONDS.toNanos(5)));
            }
        } finally {
            if (!entered) synchronized (this) { (priority ? first : other).remove(waiter); notifyAll(); }
        }
    }
    private synchronized void released(Ticket expected) { if (held != expected) throw new IllegalStateException("admission ticket changed"); held = null; notifyAll(); }

    static <T> T driverTransaction(MongoClient client, MongoClaimOperations operations, TransactionOptions options,
            Function<ClientSession, T> action) {
        if (operations == null || current(client) == null) {
            try (var session = client.startSession()) { return session.withTransaction(() -> action.apply(session), options); }
        }
        var session = client.startSession();
        var owned = operations.ownAdmissionSession(session, client);
        try {
            T result = session.withTransaction(() -> {
                owned.observeDriverAttempt();
                return action.apply(session);
            }, options);
            operations.acknowledged(owned);
            return result;
        } catch (RuntimeException | Error failure) {
            try {
                owned.capture();
                if (owned.driverProvedEnded()) operations.committed(owned);
            } catch (RuntimeException | Error capture) { if (capture != failure) failure.addSuppressed(capture); }
            throw failure;
        }
    }

    static final class Observer implements CommandListener {
        private volatile MongoClient client;
        void bind(MongoClient original) { if (client != null) throw new IllegalStateException("admission observer already bound"); client = original; OBSERVED.add(original); }
        @Override public void commandStarted(CommandStartedEvent event) {
            if (client == null || TRACKED.isEmpty()) return;
            var command = event.getCommand();
            if (!(command.get("lsid") instanceof BsonDocument session) || !command.containsKey("txnNumber")) return;
            var key = new TransactionKey(client, session, command.getNumber("txnNumber").longValue());
            var owner = TRACKED.get(key); if (owner == null) return;
            owner.driverCommandStarted(key.number());
            if (event.getCommandName().equals("commitTransaction") || event.getCommandName().equals("abortTransaction"))
                FINALIZATIONS.put(new RequestKey(client, event.getConnectionDescription().getConnectionId(), event.getRequestId()),
                        new Finalization(key, owner, event.getCommandName().equals("commitTransaction")));
        }
        @Override public void commandSucceeded(CommandSucceededEvent event) {
            var ending = FINALIZATIONS.remove(new RequestKey(client, event.getConnectionDescription().getConnectionId(), event.getRequestId()));
            if (ending != null && !event.getResponse().containsKey("writeConcernError"))
                ending.owner().driverEnded(ending.key().number(), ending.commit(), 0);
        }
        @Override public void commandFailed(CommandFailedEvent event) {
            var ending = FINALIZATIONS.remove(new RequestKey(client, event.getConnectionDescription().getConnectionId(), event.getRequestId()));
            if (ending != null && !ending.commit() && event.getThrowable() instanceof com.mongodb.MongoCommandException failed
                    && (failed.getCode() == 251 || failed.getCode() == 256) && !failed.getResponse().containsKey("writeConcernError"))
                ending.owner().driverEnded(ending.key().number(), false, failed.getCode());
        }
    }
    static void observe(MongoClaimOperations.Transaction owner, MongoClient client, BsonDocument session, long number) {
        forget(owner);
        TRACKED.put(new TransactionKey(client, session.clone(), number), owner);
    }
    static void forget(MongoClaimOperations.Transaction owner) {
        TRACKED.entrySet().removeIf(entry -> entry.getValue() == owner);
        FINALIZATIONS.entrySet().removeIf(entry -> entry.getValue().owner() == owner);
    }
    static boolean observes(MongoClient client) { return OBSERVED.contains(client); }

    static final class Ticket {
        private final MongoCacheAdmissionQueue owner;
        private final boolean firstRole;
        private final AtomicInteger references = new AtomicInteger(1), calls = new AtomicInteger();
        private final AtomicBoolean slotReleased = new AtomicBoolean();
        private final Map<Object, LongConsumer> transactions = new ConcurrentHashMap<>();
        private volatile boolean entryEnded;
        private Ticket(MongoCacheAdmissionQueue owner, boolean firstRole) { this.owner = owner; this.firstRole = firstRole; }
        void retainCall() { references.incrementAndGet(); calls.incrementAndGet(); }
        void callFinished() { calls.decrementAndGet(); release(); }
        void retainTransaction(Object transaction, LongConsumer cleanup) {
            references.incrementAndGet();
            if (transactions.putIfAbsent(transaction, cleanup) != null) throw new IllegalStateException("transaction already retained");
        }
        void transactionFinished(Object transaction, boolean committed) {
            if (transactions.remove(transaction) != null) {
                if (committed) releaseSlot();
                release();
            }
        }
        void entryFinished() { entryEnded = true; release(); }
        boolean recoverable() { return entryEnded && calls.get() == 0 && !transactions.isEmpty(); }
        void recover(long deadline) { for (var cleanup : transactions.values()) cleanup.accept(deadline); }
        private void release() { int left = references.decrementAndGet(); if (left < 0) throw new IllegalStateException("negative admission references"); if (left == 0) releaseSlot(); }
        private void releaseSlot() { if (slotReleased.compareAndSet(false, true)) owner.released(this); }
    }
}
