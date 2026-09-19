package io.tapstate.runtime.engine.join;

import io.tapstate.core.event.ConvertedValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Reading the values a join key is built from off a row, the same way at every boundary that builds one. */
final class JoinKeys {

    private JoinKeys() {
    }

    /**
     * The values {@code columns} name on {@code row}, in that order, ready to be made into a key.
     *
     * <p><b>A value a source connector converted travels inside a carrier, and what a join matches on is
     * the value.</b> A carrier never equals the plain value inside it, so a join where one side met a
     * conversion and the other did not simply never matches. Two carriers do compare by their parts,
     * which makes this worse rather than better: a join with conversions on both sides works until the
     * two schemas spell the column differently, and then stops matching for a reason nothing on that
     * path names. Nothing reports any of it - the job runs, the rows arrive, and the joined side stays
     * null, which is indistinguishable from a dimension row that is genuinely not there.
     *
     * <p><b>Here rather than at each caller, because a join reads a key at two boundaries.</b> A change
     * is routed to the member holding the state it is about to change before it is matched against that
     * state, and both steps read a key off the raw row. Unwrapping at only one leaves the two sides
     * agreeing on what they match on while the edge still sends one dimension row's changes to two
     * members - one mirror entry written from two places, out of order, with the job running and
     * nothing said. A single-member run cannot show that: it owns every partition, so a key derived two
     * ways still meets itself.
     *
     * <p>Nulls are kept rather than rejected. A null is what makes a key match nothing, and that rule
     * belongs to the key itself, not to reading the values out of the row.
     *
     * <p>No other normalization belongs here. An exact number arriving under two spellings is already
     * one key, because the key encoding drops a decimal's trailing zeros where it takes its bytes.
     */
    static List<Object> valuesOf(Map<String, Object> row, List<String> columns) {
        List<Object> values = new ArrayList<>(columns.size());
        for (String column : columns) {
            values.add(ConvertedValue.unwrap(row.get(column)));
        }
        return Collections.unmodifiableList(values);
    }
}
