package io.tapstate.runtime.engine.nest;

import io.tapstate.runtime.engine.ChainAxes;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * What passing a frontier through a nest node needs from outside it: the job-wide numbering of chains
 * onto the axes bounds travel on, and which chains reach each alias the node reads. Everything else
 * follows from the compiled tree - a cascade carries whatever its subtree carries, and a level waits on
 * every edge compiled to carry a chain before promising anything about it.
 *
 * <p>The chains of an alias are given <b>per producer</b>, in the order the producers are wired, and the
 * alias's whole set is derived from that rather than supplied beside it. Where an alias resolves to
 * several producers they are gathered by a merge vertex first, and that vertex needs to know what each of
 * its own edges carries: told only the alias's total, it would wait on every edge for a chain only one of
 * them ever carries, and would promise nothing ever again. Deriving the total here is also what keeps the
 * two answers from disagreeing about what a chain is called - a numbering taken from one reading of the
 * graph and an expected set taken from another would combine bounds about different chains as though they
 * were one.
 *
 * <p>A node built without one propagates no bound at all - a frontier that stands still rather than one
 * that runs ahead.
 */
public record NestFrontier(ChainAxes axes, Function<String, List<List<String>>> chainsOfAliasByProducer) {

    public NestFrontier {
        Objects.requireNonNull(axes, "axes");
        Objects.requireNonNull(chainsOfAliasByProducer, "chainsOfAliasByProducer");
    }

    /** Every chain the producers of {@code alias} carry between them, in the order they were first seen. */
    public List<String> chainsOfAlias(String alias) {
        Set<String> merged = new LinkedHashSet<>();
        for (List<String> producer : chainsOfAliasByProducer.apply(alias)) {
            merged.addAll(producer);
        }
        return List.copyOf(merged);
    }
}
