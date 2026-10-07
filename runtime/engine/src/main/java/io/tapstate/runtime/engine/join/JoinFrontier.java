package io.tapstate.runtime.engine.join;

import io.tapstate.runtime.engine.ChainAxes;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** The job-wide chain axes and the chains each producer of a Join source can carry. */
public record JoinFrontier(ChainAxes axes, Function<String, List<List<String>>> chainsOfAliasByProducer) {

    public JoinFrontier {
        Objects.requireNonNull(axes, "axes");
        Objects.requireNonNull(chainsOfAliasByProducer, "chainsOfAliasByProducer");
    }

    /** The chains reaching one source ordinal after any producer merge. */
    public List<String> chainsOfAlias(String alias) {
        Set<String> merged = new LinkedHashSet<>();
        for (List<String> producer : chainsOfAliasByProducer.apply(alias)) {
            merged.addAll(producer);
        }
        return List.copyOf(merged);
    }
}
