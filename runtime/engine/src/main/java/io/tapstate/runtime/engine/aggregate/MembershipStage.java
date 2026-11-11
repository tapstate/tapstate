package io.tapstate.runtime.engine.aggregate;

import io.tapstate.core.dsl.aggregate.OrderedBytes;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.aggregate.GroupChange.FieldInput;
import io.tapstate.spi.store.OrderedStateStore;
import io.tapstate.spi.store.OrderedStateStore.Entry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The membership side of an aggregate step: remembers, for every input row, which group it is in, so that a
 * change to the row can take it out of the group it was in without the source saying what the row used
 * to be.
 *
 * <p>For each input row the store holds the order of the last event applied, the group the row is in or
 * that it is in none, and the groups it has left whose leaving may not have been told yet. An event older
 * than what is stored is dropped. A newer one is applied, stored, and only then told to the group side. An
 * event carrying the very order stored is told to the group side again and changes nothing, because the
 * first telling may have been lost between the commit and the send.
 *
 * <p><b>A group the row left is remembered until its leaving is known to have been told</b>, which is
 * when the chain's settled order has passed the event that took the row out. One lost send can follow
 * another, and a replay then ties only with the last event applied: were only that event's leaving
 * kept, the row would stay counted in every group it left before it.
 *
 * <p><b>Several events for one row in one drain are told as their net effect</b>, under the order of the
 * last: out of the group the row was in before the drain, into the group it is in after. Telling the
 * steps between would leave a row behind in a group if the drain were lost after its commit, since a
 * replay would then tie only with the last event and send only the last step.
 *
 * <p>An input row belongs to one source chain, because orders compare only within a chain. An event for a
 * stored row from another chain is not applied and is counted.
 */
public final class MembershipStage {

    private final OrderedStateStore store;

    public MembershipStage(OrderedStateStore store) {
        this.store = store;
    }

    /**
     * One event of one input row, already evaluated against the step.
     *
     * @param inputKey the row's identity, as bytes that sort the way the row's key does
     * @param chain    the source chain the event came through
     * @param order    the event's order within that chain
     * @param group    the values of the group the row is now in, or null when it is in none: the row was
     *                 deleted, or does not pass the step's condition
     * @param inputs   what the row gives each field; ignored when {@code group} is null
     */
    public record RowEvent(byte[] inputKey, String chain, SourceOrder order, List<Object> group, List<FieldInput> inputs) {

        public RowEvent {
            Objects.requireNonNull(inputKey, "inputKey");
            Objects.requireNonNull(chain, "chain");
            Objects.requireNonNull(order, "order");
            group = group == null ? null : Collections.unmodifiableList(new ArrayList<>(group));
            inputs = group == null ? null : List.copyOf(inputs);
        }

        public static RowEvent member(byte[] inputKey, String chain, SourceOrder order, List<Object> group,
                List<FieldInput> inputs) {
            return new RowEvent(inputKey, chain, order, Objects.requireNonNull(group, "group"), inputs);
        }

        public static RowEvent gone(byte[] inputKey, String chain, SourceOrder order) {
            return new RowEvent(inputKey, chain, order, null, null);
        }
    }

    /**
     * What a drain comes to.
     *
     * @param changes     what to tell the group side, in an order that keeps one row's leaving before its arriving
     * @param dropped     events older than what is stored
     * @param conflicts   events carrying a stored order with another membership, which were not applied
     * @param spansInputs events for a row stored under another chain, which were not applied
     */
    public record Outcome(List<GroupChange> changes, int dropped, int conflicts, int spansInputs) {
    }

    /** Applies one drain's events, commits, and returns what to tell the group side. */
    public Outcome apply(List<RowEvent> events) {
        TreeMap<byte[], List<RowEvent>> byRow = new TreeMap<>(Arrays::compareUnsigned);
        for (RowEvent event : events) {
            byRow.computeIfAbsent(event.inputKey(), key -> new ArrayList<>()).add(event);
        }
        List<byte[]> keys = new ArrayList<>(byRow.keySet());
        List<byte[]> loaded = store.loadAll(keys);
        List<Entry> saves = new ArrayList<>();
        List<GroupChange> changes = new ArrayList<>();
        int dropped = 0;
        int conflicts = 0;
        int spansInputs = 0;
        for (int i = 0; i < keys.size(); i++) {
            byte[] inputKey = keys.get(i);
            Stored before = loaded.get(i) == null ? null : read(loaded.get(i));
            Stored now = before;
            RowEvent applied = null;
            RowEvent again = null;
            for (RowEvent event : byRow.get(inputKey)) {
                if (now != null && !now.chain.equals(event.chain())) {
                    spansInputs++;
                    continue;
                }
                int order = now == null ? 1 : event.order().compareTo(now.order);
                if (order < 0) {
                    dropped++;
                } else if (order == 0) {
                    if (!sameGroup(event.group(), now.group)) {
                        conflicts++;
                    }
                    if (applied == null) {
                        again = event;
                    }
                } else {
                    now = new Stored(event.chain(), event.order(), event.group(), now == null ? List.of() : now.left);
                    applied = event;
                }
            }
            if (again != null) {
                // What the stored event had to tell may never have been told. It goes first, even when a
                // newer event follows in the same drain: the newer one only knows the group the row is in
                // now, not the one the stored event took it out of.
                tell(changes, inputKey, before, again);
            }
            if (applied != null) {
                List<Left> left = new ArrayList<>(now.left);
                if (before != null && before.group != null && !sameGroup(before.group, now.group)) {
                    left.add(new Left(before.group, now.order));
                }
                now = new Stored(now.chain, now.order, now.group, List.copyOf(left));
                saves.add(new Entry(inputKey, write(now)));
                tell(changes, inputKey, now, applied);
            }
        }
        commit(saves);
        return new Outcome(List.copyOf(changes), dropped, conflicts, spansInputs);
    }

    /** What the group side is told for the event that wrote {@code stored}: the row's leaving, then its arriving. */
    private static void tell(List<GroupChange> changes, byte[] inputKey, Stored stored, RowEvent event) {
        for (Left left : stored.left) {
            changes.add(GroupChange.retract(left.group, inputKey, left.order));
        }
        if (stored.group == null) {
            return;
        }
        if (event.group() == null) {
            // The same order arrived saying the row is gone while it is stored as a member. The group side
            // holds the row under this order and counts the difference; it does not apply it.
            changes.add(GroupChange.retract(stored.group, inputKey, stored.order));
        } else {
            changes.add(GroupChange.set(stored.group, inputKey, stored.order, event.inputs()));
        }
    }

    /** Whether two groups are one group: by the bytes a group is stored under, so 2.5 and 2.50 are the same. */
    private static boolean sameGroup(List<Object> a, List<Object> b) {
        if (a == null || b == null) {
            return a == b;
        }
        return Arrays.equals(OrderedBytes.ofKey(a), OrderedBytes.ofKey(b));
    }

    /**
     * What one page of a sweep came to.
     *
     * @param next     where the next page starts, or null when this page was the last
     * @param rows     input rows forgotten whole: gone, with nothing left to tell
     * @param leavings records of a row leaving a group that were dropped
     */
    public record Reclaimed(byte[] next, int rows, int leavings) {
    }

    /**
     * Sweeps one page of stored rows and forgets what can no longer matter: a group a row left, once the
     * event that took it out cannot be delivered again, and a row that is gone, once the event that
     * removed it cannot be. Both exist only to answer a second delivery, and {@code floor} says where
     * deliveries resume. A chain whose floor is not known keeps everything.
     *
     * <p>The group side forgets first. Stopped between the two, the row still remembers the leaving and
     * the next sweep finds it again; the other way round the group side would keep its record for good.
     *
     * @param after the last key of the page before, or null to start from the beginning
     * @param limit how many rows to look at
     */
    public Reclaimed reclaim(ReplayFloor floor, GroupStage groups, byte[] after, int limit) {
        List<Entry> page = store.scan(FIRST_KEY, PAST_EVERY_KEY, after, limit);
        Map<String, Optional<SourceOrder>> floors = new HashMap<>();
        List<GroupStage.Leaving> told = new ArrayList<>();
        List<Entry> saves = new ArrayList<>();
        List<byte[]> deletes = new ArrayList<>();
        for (Entry entry : page) {
            Stored stored = read(entry.state());
            Optional<SourceOrder> resumesAt = floors.computeIfAbsent(stored.chain, floor::of);
            if (resumesAt.isEmpty()) {
                continue;
            }
            List<Left> kept = new ArrayList<>();
            for (Left left : stored.left) {
                if (left.order.compareTo(resumesAt.get()) < 0) {
                    told.add(new GroupStage.Leaving(left.group, entry.key(), left.order));
                } else {
                    kept.add(left);
                }
            }
            if (stored.group == null && kept.isEmpty() && stored.order.compareTo(resumesAt.get()) < 0) {
                deletes.add(entry.key());
            } else if (kept.size() < stored.left.size()) {
                saves.add(new Entry(entry.key(), write(new Stored(stored.chain, stored.order, stored.group, kept))));
            }
        }
        groups.forget(told);
        // Trimmed rows are small, so a part is bounded by its count alone.
        int most = store.commitLimit().operations();
        for (int at = 0; at < saves.size(); at += most) {
            store.commit(List.copyOf(saves.subList(at, Math.min(saves.size(), at + most))), List.of());
        }
        for (int at = 0; at < deletes.size(); at += most) {
            store.commit(List.of(), List.copyOf(deletes.subList(at, Math.min(deletes.size(), at + most))));
        }
        byte[] next = page.size() < limit ? null : page.getLast().key();
        return new Reclaimed(next, deletes.size(), told.size());
    }

    private static final byte[] FIRST_KEY = {};
    /** Above every key: a key starts with the tag of its first value, and no tag is this high. */
    private static final byte[] PAST_EVERY_KEY = {(byte) 0xFF};

    private void commit(List<Entry> saves) {
        OrderedStateStore.Limit limit = store.commitLimit();
        List<Entry> part = new ArrayList<>();
        long bytes = 0;
        for (Entry save : saves) {
            long weight = save.key().length + save.state().length;
            if (!part.isEmpty() && (part.size() == limit.operations() || bytes + weight > limit.bytes())) {
                store.commit(part, List.of());
                part = new ArrayList<>();
                bytes = 0;
            }
            part.add(save);
            bytes += weight;
        }
        if (!part.isEmpty()) {
            store.commit(part, List.of());
        }
    }

    /**
     * One input row as stored: the chain and order of the last event applied, the group the row is in or
     * null when it is in none, and the groups it left that may not have been told so.
     */
    private record Stored(String chain, SourceOrder order, List<Object> group, List<Left> left) {
    }

    /** A group the row left, and the order of the event that took it out. */
    private record Left(List<Object> group, SourceOrder order) {
    }

    private static byte[] write(Stored stored) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(1);
            out.writeUTF(stored.chain);
            out.writeLong(stored.order.epoch());
            out.writeLong(stored.order.seq());
            group(out, stored.group);
            out.writeInt(stored.left.size());
            for (Left left : stored.left) {
                StoredValues.writeAll(out, left.group);
                out.writeLong(left.order.epoch());
                out.writeLong(left.order.seq());
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static Stored read(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int version = in.readUnsignedByte();
            if (version != 1) {
                throw new IOException("aggregate membership of version " + version + " is not one this build reads");
            }
            String chain = in.readUTF();
            SourceOrder order = new SourceOrder(in.readLong(), in.readLong());
            List<Object> group = group(in);
            List<Left> left = new ArrayList<>();
            for (int i = in.readInt(); i > 0; i--) {
                left.add(new Left(StoredValues.readAll(in), new SourceOrder(in.readLong(), in.readLong())));
            }
            return new Stored(chain, order, group, List.copyOf(left));
        } catch (IOException unreadable) {
            throw new IllegalStateException("aggregate membership cannot be read", unreadable);
        }
    }

    private static void group(DataOutputStream out, List<Object> group) throws IOException {
        out.writeBoolean(group != null);
        if (group != null) {
            StoredValues.writeAll(out, group);
        }
    }

    private static List<Object> group(DataInputStream in) throws IOException {
        return in.readBoolean() ? StoredValues.readAll(in) : null;
    }
}
