package io.tapstate.core.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.NodeParallelism.Origin;
import io.tapstate.core.lifecycle.NodeParallelism.Scope;
import io.tapstate.core.lifecycle.ParallelismPlanner.Planned;
import io.tapstate.core.lifecycle.ParallelismPlanner.Refused;
import io.tapstate.core.lifecycle.ParallelismRequest.Kind;
import io.tapstate.core.lifecycle.ParallelismRequest.Singleton;
import org.junit.jupiter.api.Test;

/**
 * A node that says nothing runs at its kind's default - one processor for a source or a step, four writers for
 * a view or a serve.sync - and says that it did: the origin reads {@code node-default}, never
 * {@code explicit}. A default that read as written would tell whoever is asking why a node runs this wide that
 * somebody asked for it.
 */
class OperatorAndSinkDefaultsAreDifferentTest {

    private static NodeParallelism plan(Kind kind, Integer written, Singleton singleton, int members) {
        ParallelismRequest request = new ParallelismRequest("n", kind, written, singleton, false, 1024, 0);
        return ((Planned) ParallelismPlanner.plan(request, members, ParallelismBudget.DEFAULTS)).parallelism();
    }

    @Test
    void anOrdinaryStepThatSaysNothingIsOneProcessorForTheCluster() {
        NodeParallelism step = plan(Kind.TRANSFORM, null, null, 3);

        assertThat(step.requested()).isEqualTo(1);
        assertThat(step.origin()).isEqualTo(Origin.NODE_DEFAULT);
        assertThat(step.scope()).isEqualTo(Scope.TOTAL_ONE);
    }

    @Test
    void aSinkThatSaysNothingIsFourWriters() {
        NodeParallelism sink = plan(Kind.SINK, null, null, 1);

        assertThat(sink.requested()).isEqualTo(4);
        assertThat(sink.origin()).isEqualTo(Origin.NODE_DEFAULT);
        assertThat(sink.scope()).isEqualTo(Scope.NATIVE);
        assertThat(sink.effective()).isEqualTo(4);
    }

    @Test
    void aWrittenValueOverridesTheDefaultAndSaysItWasWritten() {
        NodeParallelism sink = plan(Kind.SINK, 1, null, 3);
        NodeParallelism step = plan(Kind.TRANSFORM, 4, null, 2);

        assertThat(sink.requested()).isEqualTo(1);
        assertThat(sink.origin()).isEqualTo(Origin.EXPLICIT);
        assertThat(sink.scope()).isEqualTo(Scope.TOTAL_ONE);
        assertThat(step.origin()).isEqualTo(Origin.EXPLICIT);
        assertThat(step.effective()).isEqualTo(4);
        // Writing the default out is still writing it.
        assertThat(plan(Kind.TRANSFORM, 1, null, 1).origin()).isEqualTo(Origin.EXPLICIT);
    }

    @Test
    void aSourceAskedForMoreIsReadByOneAndSaysWhy() {
        NodeParallelism source = plan(Kind.SOURCE, 4, Singleton.SOURCE_READS_NOT_SPLIT, 3);

        assertThat(source.requested()).isEqualTo(4);
        assertThat(source.origin()).isEqualTo(Origin.EXPLICIT);
        assertThat(source.scope()).isEqualTo(Scope.TOTAL_ONE);
        assertThat(source.effective()).isEqualTo(1);
        assertThat(source.reasons()).containsExactly("source-reads-not-split");
    }

    @Test
    void aKeylessSingleTargetSinkRunsAsOneByDefaultAndRefusesAWrittenWiderTarget() {
        NodeParallelism byDefault = plan(Kind.SINK, null, Singleton.SINGLE_TARGET_KEYLESS, 3);

        assertThat(byDefault.scope()).isEqualTo(Scope.TOTAL_ONE);
        assertThat(byDefault.reasons()).containsExactly("single-target-keyless");

        ParallelismRequest written = new ParallelismRequest(
                "serve.out", Kind.SINK, 8, Singleton.SINGLE_TARGET_KEYLESS, false, 1024, 0);
        assertThat(ParallelismPlanner.plan(written, 3, ParallelismBudget.DEFAULTS))
                .isInstanceOfSatisfying(Refused.class, refused -> {
                    assertThat(refused.singleton()).isEqualTo(Singleton.SINGLE_TARGET_KEYLESS);
                    assertThat(refused.requested()).isEqualTo(8);
                });
    }
}
