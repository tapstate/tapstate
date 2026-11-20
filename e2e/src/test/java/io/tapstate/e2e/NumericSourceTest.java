package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NumericSourceTest {

    @Test
    void filterWorkspacePreservesCelQuotesBackslashesAndLineBreaks() {
        String expression = "after.amount > 25 &&\n op != \"\\\\draft\"";
        var workspace = NumericSource.filterWorkspace(Map.of(), "mongodb://localhost/numeric",
                "numeric_filter", expression);
        PipelineResource pipeline = (PipelineResource) new DslParser().parse(workspace.get("pipeline.tap.yml"));
        Step.Inline step = (Step.Inline) pipeline.transforms().getFirst();
        assertThat(((TransformBody.Filter) step.body()).expr()).isEqualTo(expression);
    }
}
