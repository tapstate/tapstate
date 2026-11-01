package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** Closed intervals and unresolved singletons partition the issued sequence space. */
final class CaptureOpeningIntervals {
    static final int MAX_OPENINGS = 128;
    static final int MAX_INTERVALS = MAX_OPENINGS + 1;
    record Range(long from, long through) {
        Range {
            if (from < 1 || through < from || through == Long.MAX_VALUE)
                throw unreadable();
        }
        Document document() { return new Document("from", from).append("through", through); }
    }
    record State(long issuedThrough, long closedPrefixThrough, List<Range> ranges, List<Document> unresolved) {
        State {
            ranges = List.copyOf(ranges);
            unresolved = unresolved.stream().map(Document::new).toList();
            if (issuedThrough < 0 || issuedThrough == Long.MAX_VALUE || closedPrefixThrough < 0
                    || closedPrefixThrough > issuedThrough || ranges.size() > MAX_INTERVALS
                    || unresolved.size() > MAX_OPENINGS) throw unreadable();
            var partition = new ArrayList<Range>();
            if (closedPrefixThrough > 0) partition.add(new Range(1, closedPrefixThrough));
            long prior = closedPrefixThrough;
            for (var range : ranges) {
                if (range.from() <= prior + 1) throw unreadable();
                prior = range.through(); partition.add(range);
            }
            for (var opening : unresolved) {
                if (!(opening.get("sequence") instanceof Long sequence)) throw unreadable();
                partition.add(new Range(sequence, sequence));
            }
            partition.sort(Comparator.comparingLong(Range::from));
            long through = 0;
            for (var part : partition) {
                if (part.from() != through + 1) throw unreadable();
                through = part.through();
            }
            if (through != issuedThrough) throw unreadable();
        }
        Document document() {
            return new Document("issuedThrough", issuedThrough).append("closedPrefixThrough", closedPrefixThrough)
                    .append("closedRanges", ranges.stream().map(Range::document).toList()).append("unresolved", unresolved);
        }
    }
    private CaptureOpeningIntervals() {}

    static State empty() { return new State(0, 0, List.of(), List.of()); }
    static State read(Document row) {
        try {
            if (row == null || !row.keySet().equals(Set.of("issuedThrough", "closedPrefixThrough", "closedRanges", "unresolved"))
                    || !(row.get("issuedThrough") instanceof Long issued)
                    || !(row.get("closedPrefixThrough") instanceof Long prefix)) throw unreadable();
            var ranges = new ArrayList<Range>();
            for (var item : row.getList("closedRanges", Document.class)) {
                if (!item.keySet().equals(Set.of("from", "through")) || !(item.get("from") instanceof Long from)
                        || !(item.get("through") instanceof Long through)) throw unreadable();
                ranges.add(new Range(from, through));
            }
            return new State(issued, prefix, ranges, row.getList("unresolved", Document.class));
        } catch (ClassCastException | NullPointerException invalid) { throw unreadable(); }
    }
    static State append(State original, long expectedIssued, Document entry) {
        if (expectedIssued != original.issuedThrough() || expectedIssued >= Long.MAX_VALUE - 1
                || !(entry.get("sequence") instanceof Long sequence) || sequence != expectedIssued + 1)
            throw unreadable();
        if (original.unresolved().size() == MAX_OPENINGS) throw new IllegalStateException("opening capacity must be checked before allocation");
        var entries = new ArrayList<>(original.unresolved()); entries.add(new Document(entry));
        return new State(sequence, original.closedPrefixThrough(), original.ranges(), entries);
    }
    static State close(State original, Document exactEntry) {
        var entries = new ArrayList<>(original.unresolved());
        if (!entries.remove(exactEntry)) return original;
        long sequence = exactEntry.getLong("sequence");
        var all = new ArrayList<>(original.ranges()); all.add(new Range(sequence, sequence));
        all.sort(Comparator.comparingLong(Range::from));
        var merged = new ArrayList<Range>(); long prefix = original.closedPrefixThrough();
        for (var range : all) {
            if (range.from() == prefix + 1) { prefix = range.through(); continue; }
            if (!merged.isEmpty() && merged.getLast().through() + 1 == range.from()) {
                var before = merged.removeLast(); merged.add(new Range(before.from(), range.through()));
            } else merged.add(range);
        }
        return new State(original.issuedThrough(), prefix, merged, entries);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "capture-opening-journal", "field", "sequence-partition"), null);
    }
}
