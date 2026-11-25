package io.tapstate.runtime.engine;

import io.tapstate.core.model.*;
import io.tapstate.runtime.engine.nest.*;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class OperatorMapCoordinatesTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache","p",1);
    @Test void structuralPathsKeepTheRootLiteralNamesAndSegmentBoundariesSeparate() {
        assertThat(OperatorMapCoordinates.path(List.of())).isEqualTo("[]");
        assertThat(OperatorMapCoordinates.path(List.of("$root"))).isEqualTo("[\"$root\"]");
        assertThat(OperatorMapCoordinates.path(List.of("a.b"))).isEqualTo("[\"a.b\"]");
        assertThat(OperatorMapCoordinates.path(List.of("a","b"))).isEqualTo("[\"a\",\"b\"]");
    }
    @Test void quotesBackslashesControlUnitsAndUnpairedSurrogatesHaveCanonicalLosslessKeys() {
        assertThat(OperatorMapCoordinates.path(List.of("a\"b","c\\d","\n",new String(new char[]{0xd800}))))
                .isEqualTo("[\"a\\\"b\",\"c\\\\d\",\"\\u000a\",\"\\ud800\"]");
        assertThat(OperatorMapCoordinates.path(List.of(new String(new char[]{0xd800}))))
                .isNotEqualTo(OperatorMapCoordinates.path(List.of(new String(new char[]{0xfffd}))));
    }
    @Test void anActuallyCompiledLiteralRootEmbedCannotShareItsPhysicalStateWithTheAssembler() {
        var topology = literalRootTopology();
        assertThat(topology.vertices()).hasSize(2);
        assertThat(topology.vertices()).extracting(NestVertex::mapName).containsOnly("nest.p.doc.$root");
        var physical = topology.vertices().stream().map(vertex -> OperatorMapCoordinates.nest(SCOPE,"cache","doc",
                vertex.pathId(),OperatorMapCoordinates.NestMap.STATE,null).mapName()).toList();
        assertThat(physical).hasSize(2).doesNotHaveDuplicates();
        assertThat(topology.stateIdentityPaths()).hasSize(topology.streams().size())
                .contains("[]",OperatorMapCoordinates.path(List.of("$root")));
    }
    static NestTopology literalRootTopology() {
        var child = new Embed("claim",Map.of("policy_id","policy_id"),EmbedAs.ARRAY,"child",List.of("claim_id"),null,null,null);
        var parent = new Embed("policy",Map.of("customer_id","customer_id"),EmbedAs.ARRAY,"$root",List.of("policy_no"),null,null,List.of(child));
        var tree = new TransformBody.Nest(null,null,new NestRoot("customer",List.of("customer_id"),null,null,List.of(parent)));
        var tables = Map.of("customer",new NestTable("customers",List.of("customer_id")),
                "policy",new NestTable("policies",List.of("policy_id")),"claim",new NestTable("claims",List.of("claim_id")));
        return NestTopology.compile("p","doc",tree,tables::get);
    }
}
