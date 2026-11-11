package io.tapstate.runtime.engine.aggregate;

import com.hazelcast.core.HazelcastInstance;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * How a check or a repair reaches the vertex that runs an aggregate step.
 *
 * <p>Both are answered by the step itself, on the thread that applies its events and between two drains.
 * That is what makes a check mean something: nothing writes a group while the step reads it, so a
 * difference between a group's totals and its rows is a defect, never a matter of timing. It is also why
 * a repair cannot interleave with an update to the same group or be made from outside the member that
 * holds the step's claim.
 *
 * <p>A running step registers here on its member, and a request is offered to that registration. Only
 * the member running the step has one; every other member answers that it does not.
 */
public final class AggregateRequests {

    private static final String CONTEXT = "tapstate.aggregate-requests";
    /** Requests one step holds unanswered before further ones are refused. */
    static final int WAITING = 16;
    /** Groups one request may ask for. */
    public static final int MAX_GROUPS = 64;

    private AggregateRequests() {
    }

    /** What is asked of a step. */
    public enum Action {
        /** Compare each group's derived state with its rows; change nothing. */
        CHECK,
        /** Write each group's derived state again from its rows, and send the group on. */
        REPAIR
    }

    /**
     * @param action what to do
     * @param group  the one group to do it to, as its key values, or null for a page of the step's groups
     * @param after  for a page: where the page before ended, or null to start from the first group
     * @param groups for a page: how many groups to take, at most {@link #MAX_GROUPS}
     */
    public record Request(Action action, List<Object> group, byte[] after, int groups) implements Serializable {
        private static final long serialVersionUID = 1L;

        public Request {
            Objects.requireNonNull(action, "action");
            group = group == null ? null : Collections.unmodifiableList(new ArrayList<>(group));
            if (group == null && (groups < 1 || groups > MAX_GROUPS)) {
                throw new IllegalArgumentException("a page takes 1 to " + MAX_GROUPS + " groups");
            }
        }

        public static Request one(Action action, List<Object> group) {
            return new Request(action, Objects.requireNonNull(group, "group"), null, 1);
        }

        public static Request page(Action action, byte[] after, int groups) {
            return new Request(action, null, after, groups);
        }
    }

    /**
     * One group that was not simply clean.
     *
     * @param group       the group's key values
     * @param verdict     {@code mismatch} or {@code inconclusive}; {@code clean} only for a group listed
     *                    because it cannot be given
     * @param fields      the fields whose derived state differed from the group's rows
     * @param failure     the code under which the group cannot be given exactly now, or null
     * @param failedField the field that code is about, or null
     */
    public record Finding(List<Object> group, String verdict, List<String> fields, String failure,
            String failedField) implements Serializable {
        private static final long serialVersionUID = 1L;

        public Finding {
            group = Collections.unmodifiableList(new ArrayList<>(group));
            fields = List.copyOf(fields);
        }
    }

    /**
     * @param groups    how many groups were looked at
     * @param findings  the groups that differed, could not be decided, or cannot be given
     * @param next      where the next page starts, or null when no group is left
     * @param conflicts events the step did not apply since it started, because they carried an order
     *                  already stored with other content or came for a row through another input
     * @param withheld  times since it started that the step sent a group at its last good values
     */
    public record Report(int groups, List<Finding> findings, byte[] next, long conflicts, long withheld)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public Report {
            findings = List.copyOf(findings);
        }
    }

    /** A request waiting for its step, and where its answer goes. */
    record Waiting(Request request, CompletableFuture<Report> answer) {
    }

    /** What a running step polls between drains. */
    static final class Inbox {
        private final ConcurrentLinkedQueue<Waiting> waiting = new ConcurrentLinkedQueue<>();
        private final AtomicInteger held = new AtomicInteger();
        private volatile boolean closed;

        Waiting poll() {
            Waiting next = waiting.poll();
            if (next != null) {
                held.decrementAndGet();
            }
            return next;
        }

        private Optional<CompletableFuture<Report>> offer(Request request) {
            if (closed || held.incrementAndGet() > WAITING) {
                if (!closed) {
                    held.decrementAndGet();
                }
                return Optional.empty();
            }
            Waiting entry = new Waiting(request, new CompletableFuture<>());
            waiting.add(entry);
            if (closed) {
                // The step stopped while this was offered; it will not be polled.
                entry.answer().completeExceptionally(new IllegalStateException("the aggregate step stopped"));
            }
            return Optional.of(entry.answer());
        }

        private void close() {
            closed = true;
            for (Waiting left = waiting.poll(); left != null; left = waiting.poll()) {
                left.answer().completeExceptionally(new IllegalStateException("the aggregate step stopped"));
            }
        }
    }

    /** Registers the step running on {@code member}; a later registration under the same name replaces it. */
    static Inbox register(HazelcastInstance member, String pipeline, String step) {
        Inbox inbox = new Inbox();
        Inbox before = inboxes(member).put(name(pipeline, step), inbox);
        if (before != null) {
            before.close();
        }
        return inbox;
    }

    /** Removes {@code inbox} if it is still the one registered, and refuses what it had not answered. */
    static void unregister(HazelcastInstance member, String pipeline, String step, Inbox inbox) {
        inboxes(member).remove(name(pipeline, step), inbox);
        inbox.close();
    }

    /**
     * Offers {@code request} to the step if it runs on {@code member}.
     *
     * @return where the answer will arrive, or empty when the step does not run on this member or
     *         already holds as many requests as it takes
     */
    public static Optional<CompletableFuture<Report>> offer(HazelcastInstance member, String pipeline, String step,
            Request request) {
        Inbox inbox = inboxes(member).get(name(pipeline, step));
        return inbox == null ? Optional.empty() : inbox.offer(request);
    }

    /** Whether the step runs on {@code member}. */
    public static boolean runsOn(HazelcastInstance member, String pipeline, String step) {
        return inboxes(member).containsKey(name(pipeline, step));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Inbox> inboxes(HazelcastInstance member) {
        return (Map<String, Inbox>) member.getUserContext().computeIfAbsent(CONTEXT, key -> new ConcurrentHashMap<String, Inbox>());
    }

    private static String name(String pipeline, String step) {
        // A pipeline id holds no dot, so the two never run together into another pair's name.
        return pipeline + "." + step;
    }
}
