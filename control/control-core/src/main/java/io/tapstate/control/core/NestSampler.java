package io.tapstate.control.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads a few rows of one source table, and how many it holds, for the checks a nest's shape is put through
 * before it runs. Never writes and never reads more than it was asked for.
 */
public interface NestSampler {

    /** A sampler for a build that reads no source: every table reads as not sampled. */
    NestSampler NONE = (source, table, rows) -> Sample.unavailable("no source can be read from here");

    /** Up to {@code rows} rows of {@code table} on {@code source}, and the table's row count where known. */
    Sample sample(String source, String table, int rows);

    /**
     * What was read.
     *
     * @param tableRows how many rows the table holds, or null where its connector does not say
     * @param rows the rows read
     * @param unavailable why nothing could be read, or null when the rows are what there is
     */
    record Sample(Long tableRows, List<Map<String, Object>> rows, String unavailable) {
        public Sample {
            rows = rows == null ? List.of() : List.copyOf(rows);
        }

        public static Sample unavailable(String why) {
            return new Sample(null, List.of(), Objects.requireNonNull(why, "why"));
        }
    }
}
