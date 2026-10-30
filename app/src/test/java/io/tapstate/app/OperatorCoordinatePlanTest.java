package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.*;
import io.tapstate.runtime.engine.OperatorMapBinding;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.runtime.engine.OperatorMapCoordinates.NestMap;
import io.tapstate.runtime.engine.nest.*;
import io.tapstate.spi.store.*;
import io.tapstate.spi.store.OperatorPlacement.Resource;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class OperatorCoordinatePlanTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",1,"cache-a","p",7);
    private static final Map<String,NestTable> TABLES = Map.of(
            "customer",new NestTable("customers",List.of("customer_id")),
            "policy",new NestTable("policies",List.of("policy_id")),
            "claim",new NestTable("claims",List.of("claim_id")),
            "country",new NestTable("countries",List.of("country_id")));

    @Test void compiledRolesCoverLiteralRootEmbedsLookupsReferencesAndEveryJoinResource() {
        var plan = plan(); var nest = plan.nests().get("doc");
        assertThat(nest.topology().vertices()).hasSize(2);
        assertThat(nest.topology().lookups()).hasSize(1);
        assertThat(nest.topology().vertices()).extracting(NestVertex::mapName).containsOnly("nest.p.doc.$root");
        assertThat(nest.shape()).containsExactlyInAnyOrder("[]","[\"$root\"]","[\"$root\",\"child\"]","[\"country\"]");
        assertThat(plan.placements().stream().filter(p -> p.stepId().equals("doc")).toList()).containsExactlyInAnyOrder(
                nest("doc","nested",List.of(),NestMap.STATE,null),nest("doc","nested",List.of(),NestMap.PARKING,null),
                nest("doc","nested",List.of("$root"),NestMap.STATE,null),nest("doc","nested",List.of("$root"),NestMap.PARKING,null),
                nest("doc","nested",List.of("country"),NestMap.LOOKUP,"country"),nest("doc","nested",List.of("country"),NestMap.REFERENCES,"country"));
        assertThat(plan.placements().stream().filter(p -> p.stepId().equals("joined")).toList()).containsExactlyInAnyOrder(
                join(Resource.JOIN_FACTS,null),join(Resource.JOIN_WRITERS,null),
                join(Resource.JOIN_DIMENSIONS,"a.b"),join(Resource.JOIN_INDEXES,"a.b"),
                join(Resource.JOIN_DIMENSIONS,"a"),join(Resource.JOIN_INDEXES,"a"));
        assertThat(plan.placements()).hasSize(14).doesNotHaveDuplicates();
        assertThat(plan.placements()).extracting(OperatorPlacement::mapName).doesNotHaveDuplicates();
        assertThat(plan.blockingVertices()).containsEntry("doc",3).containsEntry("other",1).containsEntry("empty",0);
        assertThat(plan.nestDatabases()).containsEntry("empty","empty-cache");
        assertThat(plan.nests().get("empty").shape()).isEmpty();
        assertThat(plan.placements()).noneMatch(position -> position.stepId().equals("empty"));
    }

    @Test void eachStepRetainsItsOwnMemoryAndLogicalDocumentBudgetAndReturnsFreshStableConfigurations() {
        var plan = plan(); var configs = plan.configurations("quorum");
        assertThat(configs).hasSameSizeAs(plan.placements());
        for (var placement : plan.placements()) {
            var config = configs.stream().filter(c -> c.getName().equals(OperatorMapBinding.configurationPattern(placement))).findFirst().orElseThrow();
            assertThat(config.getName()).endsWith(".run.*");
            assertThat(config.getSplitBrainProtectionName()).isEqualTo("quorum");
            assertThat(config.getMapStoreConfig().getWriteDelaySeconds()).isZero();
            assertThat(config.getMapStoreConfig().getProperties()).containsEntry(OperatorMapBindings.SCOPED_PROPERTY,"true");
            assertThat(config.getEvictionConfig().getSize()).isEqualTo(placement.stepId().equals("doc") ? 512 : placement.stepId().equals("other") ? 1024 : 4000);
            if (placement.resource() == Resource.NEST_STATE)
                assertThat(config.getMapStoreConfig().getProperties()).containsEntry("tapstate.nest.state-database",placement.database());
        }
        var doc = plan.nests().get("doc"); var other = plan.nests().get("other");
        assertThat(doc.settings().elementsAllowedIn(doc.topology().assembler().mapName())).isEqualTo(21);
        assertThat(other.settings().elementsAllowedIn(other.topology().assembler().mapName())).isEqualTo(34);
        configs.getFirst().setName("mutated"); configs.getFirst().getMapStoreConfig().getProperties().clear();
        assertThat(plan.configurations(null)).noneMatch(c -> c.getName().equals("mutated"));
        assertThat(plan.configurations(null).getFirst().getMapStoreConfig().getProperties()).containsEntry(OperatorMapBindings.SCOPED_PROPERTY,"true");
    }

    @Test void successiveOriginalExecutionsChangeOnlyHotNamesAndNeverBorrowAnotherPhysicalScope() {
        var plan = plan(); var first = plan.bind(fence(SCOPE,1)); var second = plan.bind(fence(SCOPE,2));
        assertThat(first.bindings()).extracting(OperatorMapBinding::placement).containsExactlyElementsOf(plan.placements());
        assertThat(first.bindings()).extracting(OperatorMapBinding::configurationPattern)
                .containsExactlyElementsOf(second.bindings().stream().map(OperatorMapBinding::configurationPattern).toList());
        assertThat(first.bindings()).extracting(OperatorMapBinding::mapName)
                .doesNotContainAnyElementsOf(second.bindings().stream().map(OperatorMapBinding::mapName).toList());
        var foreign = new PipelineIntentScope("cluster",1,"cache-b","p",7);
        assertFenced(() -> plan.bind(fence(foreign,1))); assertFenced(() -> plan.bind(null));
        assertFenced(() -> plan.bind(new ExecutionFence("p",1,1)));
        assertThatThrownBy(() -> plan.placements().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> plan.nests().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void candidateCollectionsAreCapturedAndIncompleteOrDuplicateJoinCoordinatesAreRefused() {
        var aliases = new ArrayList<>(List.of("dimension")); var joins = new LinkedHashMap<String,Collection<String>>(); joins.put("joined",aliases);
        var pipeline = pipeline(); var plan = OperatorCoordinatePlan.compile(SCOPE,pipeline,"cache",TABLES::get,joins,NestSettings.defaults());
        aliases.add("late"); joins.clear();
        assertThat(plan.placements()).noneMatch(p -> p.namespace().equals("late"));
        assertThatThrownBy(() -> OperatorCoordinatePlan.compile(SCOPE,pipeline,"cache",TABLES::get,Map.of(),NestSettings.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OperatorCoordinatePlan.compile(SCOPE,pipeline,"cache",TABLES::get,Map.of("joined",List.of("duplicate","duplicate")),NestSettings.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void equivalentAliasIterationOrdersProduceTheSameOriginalManifest() {
        var first = OperatorCoordinatePlan.compile(SCOPE,pipeline(),"cache",TABLES::get,Map.of("joined",List.of("a.b","a")),NestSettings.defaults());
        var reversed = OperatorCoordinatePlan.compile(SCOPE,pipeline(),"cache",TABLES::get,Map.of("joined",List.of("a","a.b")),NestSettings.defaults());
        assertThat(first.bind(fence(SCOPE,1))).isEqualTo(reversed.bind(fence(SCOPE,1)));
    }

    private static OperatorCoordinatePlan plan() {
        return OperatorCoordinatePlan.compile(SCOPE,pipeline(),"cache",TABLES::get,Map.of("joined",List.of("a.b","a")),NestSettings.defaults());
    }
    private static PipelineResource pipeline() {
        var child = new Embed("claim",Map.of("policy_id","policy_id"),EmbedAs.ARRAY,"child",List.of("claim_id"),null,null,null);
        var parent = new Embed("policy",Map.of("customer_id","customer_id"),EmbedAs.ARRAY,"$root",List.of("policy_no"),null,null,List.of(child));
        var lookup = new Embed("country",Map.of("country_id","country_id"),EmbedAs.OBJECT,"country",null,null,null,null);
        var doc = new TransformBody.Nest(null,null,512,21,new NestStateStorage("nested"),new NestRoot("customer",List.of("customer_id"),null,null,List.of(parent,lookup)));
        var other = new TransformBody.Nest(null,null,1024,34,null,new NestRoot("customer",List.of("customer_id"),null,null,List.of(
                new Embed("policy",Map.of("customer_id","customer_id"),EmbedAs.ARRAY,"policies",List.of("policy_id"),null,null,null))));
        var empty = new TransformBody.Nest(null,null,null,null,new NestStateStorage("empty-cache"),new NestRoot("customer",List.of("customer_id"),null,null,List.of()));
        return new PipelineResource("p",null,List.of(SourceRef.bare("src")),List.of(
                step("doc",doc),step("other",other),step("empty",empty),step("joined",new TransformBody.Join(JoinEngine.BUILTIN,"SELECT * FROM a"))),null,null,null,null);
    }
    private static Step step(String id,TransformBody body) { return Step.inline(id,FromClause.aliases(Map.of("customer",FromRef.literal("src"))),body,null); }
    private static OperatorPlacement nest(String step,String db,List<String> path,NestMap kind,String alias) { return OperatorMapCoordinates.nest(SCOPE,db,step,path,kind,alias); }
    private static OperatorPlacement join(Resource kind,String alias) { return OperatorMapCoordinates.join(SCOPE,"cache","joined",kind,alias); }
    private static ExecutionFence fence(PipelineIntentScope scope,long execution) {
        return new ExecutionFence(scope,new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId()),new WorkloadOwner("node","boot"),3,execution,1));
    }
    private static void assertFenced(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
    }
}
