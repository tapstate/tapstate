package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkbenchReducerTest {

    @Test
    void currentPublishedSnapshotReplacesStateAndRequestsRender() {
        WorkbenchSnapshot snapshot = new WorkbenchSnapshot(4, 9);
        WorkbenchReducer<WorkbenchState> reducer = WorkbenchReducer.workbench();
        WorkbenchState expected = reducer.reduce(
                        WorkbenchState.initial(), new WorkbenchEvent.SnapshotExpected(snapshot))
                .state();

        WorkbenchReducer.Reduction<WorkbenchState> reduction = reducer.reduce(
                expected,
                new WorkbenchEvent.SnapshotPublished(snapshot));

        assertThat(reduction.state().snapshot()).contains(snapshot);
        assertThat(reduction.effects()).containsExactly(WorkbenchEffect.Render.INSTANCE);
    }

    @Test
    void stalePublishedSnapshotLeavesStateAndEffectsUnchanged() {
        WorkbenchReducer<WorkbenchState> reducer = WorkbenchReducer.workbench();
        WorkbenchSnapshot expectedSnapshot = new WorkbenchSnapshot(5, 12);
        WorkbenchState expected = reducer.reduce(
                        WorkbenchState.initial(), new WorkbenchEvent.SnapshotExpected(expectedSnapshot))
                .state();

        WorkbenchReducer.Reduction<WorkbenchState> reduction = reducer.reduce(
                expected,
                new WorkbenchEvent.SnapshotPublished(new WorkbenchSnapshot(4, 11)));

        assertThat(reduction.state()).isSameAs(expected);
        assertThat(reduction.effects()).isEmpty();
    }

    @Test
    void newContextExpectationClearsTheVisibleSnapshotAndRequestsRender() {
        WorkbenchReducer<WorkbenchState> reducer = WorkbenchReducer.workbench();
        WorkbenchSnapshot accepted = new WorkbenchSnapshot(5, 12);
        WorkbenchState visible = WorkbenchState.initial()
                .expectSnapshot(accepted)
                .acceptSnapshot(accepted);
        WorkbenchSnapshot nextContext = new WorkbenchSnapshot(6, 13);

        WorkbenchReducer.Reduction<WorkbenchState> reduction = reducer.reduce(
                visible,
                new WorkbenchEvent.SnapshotExpected(nextContext));

        assertThat(reduction.state().expectedSnapshot()).contains(nextContext);
        assertThat(reduction.state().snapshot()).isEmpty();
        assertThat(reduction.effects()).containsExactly(WorkbenchEffect.Render.INSTANCE);
    }

    @Test
    void sameContextExpectationRetainsTheVisibleSnapshotWithoutRequestingRender() {
        WorkbenchReducer<WorkbenchState> reducer = WorkbenchReducer.workbench();
        WorkbenchSnapshot accepted = new WorkbenchSnapshot(5, 12);
        WorkbenchState visible = WorkbenchState.initial()
                .expectSnapshot(accepted)
                .acceptSnapshot(accepted);
        WorkbenchSnapshot nextRequest = new WorkbenchSnapshot(5, 13);

        WorkbenchReducer.Reduction<WorkbenchState> reduction = reducer.reduce(
                visible,
                new WorkbenchEvent.SnapshotExpected(nextRequest));

        assertThat(reduction.state().expectedSnapshot()).contains(nextRequest);
        assertThat(reduction.state().snapshot()).contains(accepted);
        assertThat(reduction.effects()).isEmpty();
    }

    @Test
    void reductionDefensivelyCopiesItsEffectList() {
        List<WorkbenchEffect> effects = new ArrayList<>(List.of(WorkbenchEffect.Render.INSTANCE));

        WorkbenchReducer.Reduction<WorkbenchState> reduction =
                new WorkbenchReducer.Reduction<>(WorkbenchState.initial(), effects);
        effects.clear();

        assertThat(reduction.effects()).containsExactly(WorkbenchEffect.Render.INSTANCE);
        assertThatThrownBy(() -> reduction.effects().add(WorkbenchEffect.Render.INSTANCE))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
