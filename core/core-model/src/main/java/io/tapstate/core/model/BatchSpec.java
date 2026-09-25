package io.tapstate.core.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The batch one pipeline node works in: how many rows at most, and how long at most to wait for more once
 * the first row of a batch is in hand. Either limit closes a batch on its own.
 *
 * <p>A wait of nothing is the default and means no waiting at all: a node takes whatever has already arrived,
 * up to the row limit, and goes. Rows still accumulate into larger batches while whatever the node drives is
 * busy, so the default is not one call per row. A positive wait is for an author who accepts the added latency
 * to merge sparse calls, and it never sleeps a thread.
 *
 * <p>The wait keeps the spelling the author wrote. It is read back as milliseconds where it is used, and
 * rewriting it here would make the canonical form say something the author did not.
 */
@Doc("The batch a node forms: at most max_records rows, closed early once max_wait has passed since its first row.")
public record BatchSpec(
        @Doc(value = "The most rows one batch holds, from 1 to 65536.", def = "1024")
        Integer maxRecords,
        @Doc(value = "How long a batch may wait for more rows after its first one: a whole number followed "
                + "by ms, s or m, at most 60s. 0ms means no waiting.", def = "0ms")
        String maxWait) {

    /** The row limit when none is written. */
    public static final int DEFAULT_MAX_RECORDS = 1024;

    /** The largest row limit an author may ask for. */
    public static final int MAX_RECORDS_LIMIT = 65536;

    /** The wait when none is written: none at all. */
    public static final String DEFAULT_MAX_WAIT = "0ms";

    /** The longest wait an author may ask for, in milliseconds. */
    public static final long MAX_WAIT_LIMIT_MILLIS = 60_000L;

    /** A batch that states nothing, so both limits read as their defaults. */
    public static final BatchSpec DEFAULTS = new BatchSpec(null, null);

    private static final Pattern DURATION = Pattern.compile("(0|[1-9][0-9]{0,9})(ms|s|m)");

    public BatchSpec {
        if (maxRecords != null && (maxRecords < 1 || maxRecords > MAX_RECORDS_LIMIT)) {
            throw new IllegalArgumentException(
                    "execution.batch.max_records must be from 1 to " + MAX_RECORDS_LIMIT + ", got " + maxRecords);
        }
        if (maxWait != null) {
            long millis = durationMillis(maxWait);
            if (millis < 0 || millis > MAX_WAIT_LIMIT_MILLIS) {
                throw new IllegalArgumentException(
                        "execution.batch.max_wait must be a duration from 0ms to 60s, got '" + maxWait + "'");
            }
        }
    }

    /** The row limit in force: the one written, or the default. */
    public int effectiveMaxRecords() {
        return maxRecords == null ? DEFAULT_MAX_RECORDS : maxRecords;
    }

    /** The wait in force, in milliseconds: the one written, or none. */
    public long effectiveMaxWaitMillis() {
        return maxWait == null ? 0L : durationMillis(maxWait);
    }

    /**
     * Milliseconds for a duration spelled as a whole number and a unit ({@code ms}, {@code s} or {@code m}),
     * or -1 where the spelling is not one. Kept beside the record so the parser, which refuses a bad spelling
     * with a code, and the record, which refuses one as a defect, read the same grammar.
     */
    public static long durationMillis(String text) {
        if (text == null) {
            return -1L;
        }
        Matcher matcher = DURATION.matcher(text);
        if (!matcher.matches()) {
            return -1L;
        }
        long amount = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "ms" -> amount;
            case "s" -> amount * 1_000L;
            case "m" -> amount * 60_000L;
            default -> -1L;
        };
    }
}
