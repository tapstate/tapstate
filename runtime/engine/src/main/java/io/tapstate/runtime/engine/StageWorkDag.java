package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.lifecycle.Stage;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Carries the compiler's explicit business-vertex roster until submission freezes it in job arguments. */
public final class StageWorkDag extends DAG {

    public static final String ARGUMENT = "tapstate.stage-work.vertices";
    public static final String SINGLE_ARGUMENT = "tapstate.stage-work.single-business-vertices";
    private final Map<String, String> stages = new LinkedHashMap<>();
    private final Set<String> singleBusiness = new LinkedHashSet<>();

    public static Vertex measured(DAG dag, Vertex vertex, Stage stage) {
        return measured(dag, vertex, stage, false);
    }

    public static Vertex measured(DAG dag, Vertex vertex, Stage stage, boolean singleBusiness) {
        if (dag instanceof StageWorkDag measured) {
            measured.stages.put(vertex.getName(), stage.attributeValue());
            if (singleBusiness) {
                measured.singleBusiness.add(vertex.getName());
            }
        }
        return vertex;
    }

    static Map<String, String> vertices(DAG dag) {
        return dag instanceof StageWorkDag measured ? Map.copyOf(measured.stages) : Map.of();
    }

    static Set<String> singleVertices(DAG dag) {
        return dag instanceof StageWorkDag measured ? Set.copyOf(measured.singleBusiness) : Set.of();
    }
}
