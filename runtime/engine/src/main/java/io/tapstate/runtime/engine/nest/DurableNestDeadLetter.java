package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.OperatorStateStores;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Keeps the changes a nest can never place in a document, counts them where the run's other statistics are,
 * and says so.
 *
 * <p>These are rows whose parent is known gone: no later event will give them a document to sit in. A no-op
 * here would look identical from the outside to a nest that is working, because the rows it swallows were
 * never going to appear in a document anyway - no assertion about the output could tell the two apart. So
 * two things happen to each of them, and they answer two different questions.
 *
 * <table>
 *   <caption>What each of the two is for</caption>
 *   <tr><th>Kept in the store</th><td>which rows - the only one that survives a restart and the only one
 *       that can be looked at afterwards</td></tr>
 *   <tr><th>Counted as a run statistic</th><td>how many, on the same face every other number about the
 *       pipeline is read from, so that "is anything being discarded" needs nobody to read a log</td></tr>
 * </table>
 *
 * <p>Saying so while it happens is a third thing and is not here: a log line is written where there is a
 * logging framework to write it with, which this ring has none of by design. Whoever assembles the graph
 * wraps this in one.
 *
 * <p>The count is per member and shared by every processor of a vertex on it, so all of them report the same
 * number and the engine keeps a namespace at its highest reading rather than summing - which is what it does
 * for the state readings, and for the same reason. A metric handle is looked up per call rather than kept:
 * handles belong to the thread that took them, this is reached from every processor thread of the vertex,
 * and a discarded change is by definition not the hot path.
 */
public final class DurableNestDeadLetter implements NestDeadLetter {

    private static final long serialVersionUID = 1L;

    /**
     * Where the member holds what is behind its nest dead-letter channel. Shared between the assembly root
     * that puts it there and the channel that picks it up, because a name agreed in two places would
     * eventually be spelled two ways - and the channel would then find nothing, on a member configured
     * correctly.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.nest.dead-letters";

    private final NestClock clock;

    /** A channel on the system clock, which is what everything but a test runs on. */
    public DurableNestDeadLetter() {
        this(NestClock.SYSTEM);
    }

    public DurableNestDeadLetter(NestClock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Tells {@code member} where its nest's unassemblable changes are to be kept. Called before a job that
     * nests runs on it; a member that was not told is a wiring mistake and says so rather than quietly
     * discarding the changes it was built to stop discarding.
     */
    public static void bindTo(HazelcastInstance member, NestDeadLetterStore store) {
        Objects.requireNonNull(member, "member").getUserContext()
                .put(USER_CONTEXT_KEY, Objects.requireNonNull(store, "store"));
    }

    /** Binds the database-routed state stores used by production Nest vertices. */
    public static void bindTo(HazelcastInstance member, OperatorStateStores stores) {
        Objects.requireNonNull(member, "member").getUserContext()
                .put(USER_CONTEXT_KEY, Objects.requireNonNull(stores, "stores"));
    }

    /**
     * The channel for this member, or this unbound form where the member was told of none.
     *
     * <p><b>An unbound member is not refused here, and that is deliberate.</b> Refusing at this point would
     * stop a job whose data happens to be whole - every row reaching the document it belongs in, the channel
     * never once used - over a channel it had no need of. Refusing when a change is actually handed over
     * stops exactly the job that was about to lose a row, and no other. So the failure is late and loud
     * rather than early and broad: a member that runs nests without being told where discarded rows go is a
     * wiring mistake, and it surfaces the first time that mistake would cost something.
     */
    @Override
    public NestDeadLetter bind(HazelcastInstance member) {
        HazelcastInstance current = Objects.requireNonNull(member, "member");
        Object store = current.getUserContext().get(USER_CONTEXT_KEY);
        if (store instanceof OperatorStateStores routed) {
            return bindTo(routed, current, new JetNestDeadLetterGauge());
        }
        return store == null
                ? this
                : new Bound((NestDeadLetterStore) store, clock, new JetNestDeadLetterGauge());
    }

    /** The channel for a member holding {@code userContext}, taken apart from the member so it is testable. */
    NestDeadLetter boundTo(Map<String, Object> userContext) {
        Object store = userContext.get(USER_CONTEXT_KEY);
        return store == null
                ? this
                : new Bound((NestDeadLetterStore) store, clock, new JetNestDeadLetterGauge());
    }

    /**
     * The channel with {@code store} behind it and nowhere to leave a count, which is what {@link #bind}
     * produces bar the count. Separate from that so what a discarded change is turned into can be witnessed
     * without a member: the two would otherwise only be testable through a running job, which is where this
     * path is least likely to be exercised at all - reaching it takes a source with a dangling reference.
     */
    public NestDeadLetter bindTo(NestDeadLetterStore store) {
        return bindTo(store, NestDeadLetterGauge.NONE);
    }

    /** The channel with {@code store} behind it, leaving its counts with {@code gauge}. */
    NestDeadLetter bindTo(NestDeadLetterStore store, NestDeadLetterGauge gauge) {
        return new Bound(Objects.requireNonNull(store, "store"), clock,
                Objects.requireNonNull(gauge, "gauge"));
    }

    /** Database-routed binding with an explicit gauge, separated for focused tests outside a Jet thread. */
    NestDeadLetter bindTo(
            OperatorStateStores stores, HazelcastInstance member, NestDeadLetterGauge gauge) {
        return new Routed(Objects.requireNonNull(stores, "stores"),
                Objects.requireNonNull(member, "member"), clock,
                Objects.requireNonNull(gauge, "gauge"));
    }

    /**
     * Unbound, so there is nowhere to keep this. It crashes rather than returning quietly, because returning
     * quietly is the one behaviour this whole path exists to remove: the row would be gone, the documents
     * would look exactly as they do now, and nothing anywhere would say so.
     */
    @Override
    public void unassemblable(NestVertex from, ReleasedChild released) {
        throw new IllegalStateException("no nest dead-letter store is bound to this member, and " + from.name()
                + " has a change it cannot assemble and so cannot keep");
    }

    /**
     * The channel once it has a member: a live store, and the counts this member has run up so far.
     *
     * <p>The store is held plainly rather than marked away from serialization on purpose. This is built on
     * the member and never travels, and a live store that somehow found its way into a graph being submitted
     * should fail there and then - which it does, loudly, rather than arriving somewhere as a null nobody
     * notices until a change needs keeping.
     */
    private static final class Bound implements NestDeadLetter {

        private static final long serialVersionUID = 1L;

        private final NestDeadLetterStore store;
        private final NestClock clock;
        private final NestDeadLetterGauge gauge;
        private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();

        private Bound(NestDeadLetterStore store, NestClock clock, NestDeadLetterGauge gauge) {
            this.store = store;
            this.clock = clock;
            this.gauge = gauge;
        }

        @Override
        public void unassemblable(NestVertex from, ReleasedChild released) {
            String namespace = from.mapName();
            NestElement child = released.child();
            store.record(new NestDeadLetterRecord(
                    namespace,
                    elementOf(child),
                    chainOf(child),
                    orderOf(child),
                    released.heldFor().toMillis(),
                    clock.millis(),
                    child.fields()));
            gauge.handedOver(namespace,
                    counts.computeIfAbsent(namespace, ignored -> new AtomicLong()).incrementAndGet());
        }
    }

    /** A member-bound channel resolving the database from the exact map configuration of each namespace. */
    private static final class Routed implements NestDeadLetter {

        private static final long serialVersionUID = 1L;

        private final OperatorStateStores stores;
        private final HazelcastInstance member;
        private final NestClock clock;
        private final NestDeadLetterGauge gauge;
        private final Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
        private final Map<String, NestDeadLetterStore> byNamespace = new ConcurrentHashMap<>();

        private Routed(OperatorStateStores stores, HazelcastInstance member, NestClock clock,
                NestDeadLetterGauge gauge) {
            this.stores = stores;
            this.member = member;
            this.clock = clock;
            this.gauge = gauge;
        }

        @Override
        public void unassemblable(NestVertex from, ReleasedChild released) {
            String namespace = from.mapName();
            NestElement child = released.child();
            deadLetters(namespace).record(new NestDeadLetterRecord(
                    namespace,
                    elementOf(child),
                    chainOf(child),
                    orderOf(child),
                    released.heldFor().toMillis(),
                    clock.millis(),
                    child.fields()));
            gauge.handedOver(namespace,
                    counts.computeIfAbsent(namespace, ignored -> new AtomicLong()).incrementAndGet());
        }

        private NestDeadLetterStore deadLetters(String namespace) {
            return byNamespace.computeIfAbsent(namespace, ignored -> {
                String database = NestMaps.stateDatabase(member.getConfig().findMapConfig(namespace));
                return stores.inDatabase(database == null ? stores.defaultDatabase() : database).deadLetters();
            });
        }
    }

    /**
     * What one discarded element is filed under within its namespace: the embed it belonged to, then its
     * identity inside that embed. Both are needed - a vertex hands over changes from the embeds beneath it
     * as well as its own, and two embeds can carry the same key value for different elements.
     *
     * <p>Named the way the state layer names an address rather than in a way of its own, which is what stops
     * one discarded row being filed over another's record: the injectivity is argued once, where the naming
     * is, and a record here and an entry there can never disagree about what an element is called.
     */
    private static String elementOf(NestElement child) {
        return NestStateKeys.nameOf(child.ref().pathId(), child.ref().elementKey());
    }

    /** The streams the change covered, which is what a reader needs to know where to go looking. */
    private static String chainOf(NestElement child) {
        return String.join(",", child.positions().keySet());
    }

    /** The order the change carried, rendered so the channel holds no type the store has to know about. */
    private static String orderOf(NestElement child) {
        return child.order().epoch() + ":" + child.order().seq();
    }
}
