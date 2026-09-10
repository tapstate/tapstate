package io.tapstate.adapters.transform;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code unwind} port: one row carrying a list becomes one row per element, the list's field
 * holding a single element in each. The parent's other columns travel on every one of them, so
 * lifting a field out of the element is a following {@code map} rather than a second job here.
 *
 * <p>This is the only stateless operator that changes how many rows there are, and everything below
 * follows from that. It is still a pure function of the one event it is handed - an update is paired
 * within itself, never against anything remembered - so nothing here holds state between events.
 *
 * <p><b>An update is paired by key, not by element value.</b> The keys the earlier row expands to,
 * minus the keys the new row expands to, are the rows that have to go; everything the new row
 * expands to is written. Comparing elements instead is wrong in two ways that both leave rows
 * behind for good and report nothing: a parent whose key changes while its list does not looks like
 * nothing happened, and removing an element from the middle of a list numbered by position makes
 * every later element look changed while the last position is left with nobody to delete it.
 *
 * <p><b>Keys are compared unwrapped.</b> A value a source connector converted for travel arrives
 * inside a carrier, and a carrier never equals the plain value inside it - so a key that met a
 * conversion on one side and not the other never matches, and the difference is taken as the whole
 * list having been replaced. Every row that came out is still correct, which is exactly why nothing
 * catches it. Moving the parent's other columns along is the opposite case and deliberately does
 * not unwrap: the write side is owed what the source declared.
 */
final class UnwindPort implements TransformPort {

    private final UnwindSpec spec;
    private final UnwindAlert alert;

    UnwindPort(UnwindSpec spec, UnwindAlert alert) {
        this.spec = spec;
        this.alert = alert;
    }

    @Override
    public List<Envelope> transform(Envelope event) {
        return switch (event.op()) {
            // A schema change carries no row, so there is nothing to expand. Dropping it would break
            // the downstream evolution chain, exactly as it would through a map or a filter.
            case DDL -> List.of(event);
            case INSERT, READ -> expandOneSide(event, event.after(), event.op());
            case DELETE -> {
                requireWholeEarlierRow(event);
                yield expandOneSide(event, event.before(), Op.DELETE);
            }
            case UPDATE -> {
                requireWholeEarlierRow(event);
                yield pairUpdate(event);
            }
        };
    }

    /** Every element of one row's list, as events of {@code op} carrying the row on the right side. */
    private List<Envelope> expandOneSide(Envelope event, Map<String, Object> row, Op op) {
        List<Envelope> out = new ArrayList<>();
        Set<Object> seen = new LinkedHashSet<>();
        for (Map<String, Object> expanded : expand(row)) {
            noteIfKeyIsShared(seen, expanded);
            out.add(op == Op.DELETE ? asDelete(event, expanded) : asRow(event, op, expanded));
        }
        return out;
    }

    /**
     * The rows an update becomes: one delete for each key the earlier row had and the new one does
     * not, an update for each key both carry, and an insert for each key only the new one has.
     *
     * <p>Splitting the last two rather than sending everything as an update keeps each event the
     * shape its op promises - an update carries both rows, an insert only the new one - and a target
     * matching rows to keys treats the two identically anyway.
     */
    private List<Envelope> pairUpdate(Envelope event) {
        Map<Object, Map<String, Object>> was = keyed(expand(event.before()));
        Map<Object, Map<String, Object>> now = keyed(expand(event.after()));
        List<Envelope> out = new ArrayList<>();
        was.forEach((key, row) -> {
            if (!now.containsKey(key)) {
                out.add(asDelete(event, row));
            }
        });
        now.forEach((key, row) -> {
            Map<String, Object> earlier = was.get(key);
            out.add(earlier == null
                    ? asRow(event, Op.INSERT, row)
                    : new Envelope(Op.UPDATE, event.ts(), event.src(), earlier, row, event.schema())
                            .withRemoved(event.removed()));
        });
        return out;
    }

    /** The expanded rows of one side by their key, warning where two of them share one. */
    private Map<Object, Map<String, Object>> keyed(List<Map<String, Object>> rows) {
        Map<Object, Map<String, Object>> byKey = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            Object key = keyOf(row);
            if (byKey.put(key, row) != null) {
                alert.rowsShareAKey(TransformErrors.unwindRowsShareAKey(spec.path(), key));
            }
        }
        return byKey;
    }

    private void noteIfKeyIsShared(Set<Object> seen, Map<String, Object> row) {
        Object key = keyOf(row);
        if (!seen.add(key)) {
            alert.rowsShareAKey(TransformErrors.unwindRowsShareAKey(spec.path(), key));
        }
    }

    /**
     * One row's list as the rows it becomes: the parent's columns with the list's field holding a
     * single element, plus the ordinal where one was asked for.
     *
     * <p>Three shapes arrive as nothing to expand - the field absent, holding null, or holding an
     * empty list - and they are one case because they mean one thing to whoever wrote the row. A
     * value that is not a list at all is the opposite: it is one element, since whether a column
     * holds a list is a fact about the row rather than the declaration, and stopping a pipeline over
     * one dirty row is a worse answer than expanding it as the single thing it is.
     */
    private List<Map<String, Object>> expand(Map<String, Object> row) {
        if (row == null) {
            return List.of();
        }
        Object value = row.get(spec.path());
        if (value == null || value instanceof List<?> list && list.isEmpty()) {
            return spec.preserveNullAndEmptyArrays() ? List.of(rowWith(row, null, null)) : List.of();
        }
        List<?> elements = value instanceof List<?> list ? list : List.of(value);
        List<Map<String, Object>> out = new ArrayList<>(elements.size());
        for (int i = 0; i < elements.size(); i++) {
            out.add(rowWith(row, elements.get(i), (long) i));
        }
        return out;
    }

    /**
     * The parent's row with the list's field replaced by one element, and whatever this expansion
     * writes beside it: the ordinal where one was asked for, and the element's own identifying
     * field where the declaration named one.
     *
     * <p><b>Lifting that field is what makes the row addressable anywhere but here.</b> Inside the
     * element it is reachable by this port and by nothing downstream - a key is matched at the
     * target by column, and no store builds a column for a name that reaches into a value. Left
     * there, every expanded row of one parent arrives keyed on the parent alone and the target
     * keeps the last of them, which is the one failure this whole expansion has to not have.
     *
     * <p>The column is written whether or not the element carries the field, and whether or not
     * there is an element at all. A row short of a column the published model declares is a write
     * that fails on some stores and quietly takes a default on others, and neither is the answer
     * for a row whose identity is simply empty.
     */
    private Map<String, Object> rowWith(Map<String, Object> row, Object element, Long ordinal) {
        Map<String, Object> out = new LinkedHashMap<>(row);
        out.put(spec.path(), element);
        if (spec.includeArrayIndex() != null) {
            out.put(spec.includeArrayIndex(), ordinal);
        }
        if (spec.elementKey() != null) {
            out.put(spec.elementKey(),
                    element instanceof Map<?, ?> map ? map.get(spec.elementKey()) : null);
        }
        return out;
    }

    /**
     * What tells this row from the others the same parent produced: what its rows were already keyed
     * on, plus the one thing that varies per element.
     *
     * <p>Both parts are read off the row as columns, because by the time a row is paired the
     * expansion has already written its locator as one - so what is compared here is the same
     * thing the target is keyed on rather than a second reading of it. Every part is unwrapped,
     * since this is a comparison and a carrier never equals the value inside it.
     */
    private Object keyOf(Map<String, Object> row) {
        List<Object> key = new ArrayList<>(spec.parentKey().size() + 1);
        for (String column : spec.parentKey()) {
            key.add(ConvertedValue.unwrap(row.get(column)));
        }
        // Nothing else can identify an element, and the offline check refuses a declaration naming
        // neither - so a null locator here is a wiring fault, not a row anyone wrote.
        key.add(ConvertedValue.unwrap(row.get(spec.locator())));
        return key;
    }

    // What an event covers is stamped onto every output by the runtime that drives this port, so
    // the port neither reads nor sets it and stays a function of the row it was handed.
    private Envelope asRow(Envelope event, Op op, Map<String, Object> row) {
        return new Envelope(op, event.ts(), event.src(), null, row, event.schema())
                .withRemoved(event.removed());
    }

    private Envelope asDelete(Envelope event, Map<String, Object> row) {
        return new Envelope(Op.DELETE, event.ts(), event.src(), row, null, event.schema())
                .withRemoved(event.removed());
    }

    /**
     * Refuses an update or a delete whose earlier row is not a whole row.
     *
     * <p>The test is what the row carries, never whether it has the expanded column. A whole
     * document that simply has no such column is legitimate - an optional field in a document store
     * is ordinary, and a read of the same row must accept it and produce nothing - so testing for
     * the column would stop a pipeline on exactly the row a snapshot is required to let through.
     * Half a row is instead recognisable by carrying the columns that identify it and nothing else,
     * which is precisely what a change stream with no pre-image and a relational source under its
     * default replica identity each send.
     */
    private void requireWholeEarlierRow(Envelope event) {
        Map<String, Object> was = event.before();
        if (was == null) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(
                    spec.path(), "the event carries no earlier row at all");
        }
        List<String> missing = new ArrayList<>();
        for (String column : spec.parentKey()) {
            if (!was.containsKey(column)) {
                missing.add(column);
            }
        }
        if (!missing.isEmpty()) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(spec.path(),
                    "the earlier row does not carry " + String.join(", ", missing));
        }
        if (was.size() <= spec.parentKey().size()) {
            throw TransformErrors.unwindNeedsACompleteBeforeImage(spec.path(),
                    "the earlier row carries only the columns identifying it ("
                            + String.join(", ", spec.parentKey()) + ")");
        }
    }
}
