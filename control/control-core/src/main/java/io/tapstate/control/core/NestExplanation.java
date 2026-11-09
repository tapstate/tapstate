package io.tapstate.control.core;

import io.tapstate.core.lifecycle.NestTree;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What a nest step will do, said before it runs: the compiled tree with what was inferred, whether its load
 * can take the build path, what a sample of the sources says about each embed, and the warnings that sample
 * raised.
 *
 * @param tree the compiled tree
 * @param build whether the load can take the build path, and why not when it cannot
 * @param samples what was read for each embed, by path and alias, in tree order
 * @param warnings what the sample suggests will go wrong, each naming the embed and the change that avoids it
 */
public record NestExplanation(NestTree tree, String build, List<Sampled> samples, List<Warning> warnings) {

    public NestExplanation {
        Objects.requireNonNull(tree, "tree");
        Objects.requireNonNull(build, "build");
        samples = List.copyOf(samples);
        warnings = List.copyOf(warnings);
    }

    /**
     * What a sample said about one embed.
     *
     * @param path the embed's path
     * @param alias the alias it reads
     * @param source the source it was read from, or null where none could be named
     * @param table the table it was read from, or null where none could be named
     * @param tableRows the table's row count, where its connector says
     * @param sampledRows how many rows were read to count from
     * @param largestInSample for an owned embed, the most sampled rows under one parent; for a shared row, the
     *     most sampled parents pointing at one row
     * @param averagePerParent for an owned embed, rows per parent; for a shared row, parents per row - from the
     *     two tables' row counts, where both are known
     * @param notSampled why nothing was read, or null when something was
     */
    public record Sampled(String path, String alias, String source, String table, Long tableRows, int sampledRows,
            Long largestInSample, Double averagePerParent, String notSampled) {
    }

    /**
     * A warning: its code, the embed it is about, and the code's named arguments.
     */
    public record Warning(MonitorError code, String embed, Map<String, Object> args) {
        public Warning {
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(embed, "embed");
            args = Map.copyOf(args);
        }
    }
}
