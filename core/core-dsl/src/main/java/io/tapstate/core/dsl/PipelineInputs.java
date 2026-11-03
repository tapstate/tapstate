package io.tapstate.core.dsl;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Read-only input lineage using the same resolver as authoring validation. */
public final class PipelineInputs {
    private PipelineInputs() { }
    /** A null table is an unresolved table set, never proof of a particular table. */
    public record Input(String source, String table) { }
    public static Set<Input> reaching(PipelineResource pipeline, Map<String, Resource> resources, FromClause from) {
        return new Wiring(pipeline,resources).reaching(from).stream()
                .map(input->new Input(input.source(),input.table())).collect(Collectors.toUnmodifiableSet());
    }
}
