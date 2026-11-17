package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

class StoredObservationFactsTest {
    private static final Instant AT = Instant.parse("2026-11-17T04:00:00Z");

    @Test void foldsOnlyTheStoredFactsAndKeepsEveryOtherField() {
        var projection = new StoredObservationFacts();
        var original = observation("flow", names(1002));
        var result = projection.project(original, scope("flow",1));
        assertThat(original.facts().getFirst().points()).hasSize(1002);
        assertThat(result.facts().getFirst().points()).hasSize(1001);
        assertThat(result.facts().getFirst().points().stream().mapToLong(MetricPoint::value).sum()).isEqualTo(1002);
        assertThat(result.facts().getFirst().points().stream()
                .filter(p -> p.attributes().containsKey(MetricAttributes.OVERFLOW))).hasSize(1);
        assertThat(result.metrics()).isEqualTo(original.metrics());
        assertThat(result.snapshot()).isEqualTo(original.snapshot());
        assertThat(result.positions()).isEqualTo(original.positions());
        assertThat(result.failure()).isEqualTo(original.failure());
        assertThat(result.initialLoad()).isEqualTo(original.initialLoad());
        assertThat(result.observedAt()).isEqualTo(original.observedAt());
        assertThat(result.state()).isEqualTo(original.state());
    }

    @Test void keepsNamedSeriesStableAndGivesOtherPipelinesTheirOwnBudget() {
        var projection = new StoredObservationFacts();
        projection.project(observation("flow", names(1000)), scope("flow",1));
        var later = projection.project(observation("flow", List.of("new_table","t0000")), scope("flow",1));
        assertThat(later.facts().getFirst().points()).hasSize(2);
        assertThat(later.facts().getFirst().points()).anySatisfy(point ->
                assertThat(point.attributes()).containsEntry(MetricAttributes.OVERFLOW, "true"));
        var independent = observation("other",List.of("new_table"));
        assertThat(projection.project(independent,scope("other",1))).isSameAs(independent);
    }

    @Test void aSuccessorAndDeletionResetOnlyTheRetiredPipelinesBudget() {
        var projection = new StoredObservationFacts();
        projection.project(observation("flow",names(1000)),scope("flow",1));
        var successor = observation("flow",List.of("successor_table"));
        assertThat(projection.project(successor,scope("flow",3))).isSameAs(successor);
        assertThatThrownBy(() -> projection.project(observation("flow",List.of("late")),scope("flow",1)))
                .isInstanceOfSatisfying(TapstateException.class, e ->
                        assertThat(e.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        projection.retain(List.of());
        var recreated = observation("flow",List.of("fresh_table"));
        assertThat(projection.project(recreated,scope("flow",4))).isSameAs(recreated);
    }

    private static PipelineIntentScope scope(String id,long generation) {
        return new PipelineIntentScope("cluster",1,"cache",id,generation);
    }

    private static List<String> names(int count) {
        return IntStream.range(0,count).mapToObj(i -> String.format(Locale.ROOT,"t%04d",i)).toList();
    }

    private static Observation observation(String id,List<String> tables) {
        var points = tables.stream().map(table -> MetricPoint.accumulated(
                Map.of(MetricAttributes.PIPELINE_ID,id,MetricAttributes.TABLE_ID,table,"direction","out","op","insert"),
                AT.minusSeconds(1),AT,1L)).toList();
        var facts = List.of(new MetricFact("tapstate.pipeline.records",MetricType.COUNTER,"{record}",points));
        var failure = new ObservationFailure(IoError.STORE_UNAVAILABLE.code(),Map.of("detail","fixture"));
        var load = new InitialLoad("fixture-run",AT,"snapshot_only",
                Map.of("retained",new InitialLoad.Table(InitialLoad.Stage.LANDED,1L,1L)));
        return new Observation(id,PipelineState.FAILED,Map.of("records.out",(long)tables.size()),
                Map.of("retained",new TableSnapshot(1,1L,100,true)),
                Map.of("retained","opaque-position"),failure,AT,facts,load);
    }
}
