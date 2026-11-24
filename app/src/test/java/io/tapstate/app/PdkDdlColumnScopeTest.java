package io.tapstate.app;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PdkDdlColumnScopeTest {
    @Test void theColumnProjectionProofSurvivesTargetRebindingAndDefaultsToNoProofForUnplannedCallers() {
        var target = new TargetTable("target",List.of(new TargetField("ID","int",true)));
        var factory = new PdkSinkWriterFactory("mongodb",Map.of(),WriteMode.UPSERT,DdlPolicy.APPLY,
                Map.of("source",target),new PipelineNode("pipeline","sync_1"));
        assertThat(factory.config().ddlSourceColumns()).isTrue();
        assertThat(factory.withDdlSourceColumns(false).withTargets(Map.of("source",target)).config().ddlSourceColumns()).isFalse();
        assertThat(new SinkConfig("mongodb",Map.of(),WriteMode.UPSERT,DdlPolicy.APPLY).ddlSourceColumns()).isFalse();
    }
}
