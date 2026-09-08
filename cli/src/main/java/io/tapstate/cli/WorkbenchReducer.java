package io.tapstate.cli;

import java.util.List;
import java.util.Objects;

/** Pure typed reduction from an immutable state and event to a replacement state and effects. */
@FunctionalInterface
interface WorkbenchReducer<S> {

    Reduction<S> reduce(S state, WorkbenchEvent event);

    static WorkbenchReducer<WorkbenchState> workbench() {
        return new SnapshotReducer();
    }

    /** Immutable output interpreted only by the render-thread runtime. */
    record Reduction<S>(S state, List<WorkbenchEffect> effects) {
        public Reduction {
            Objects.requireNonNull(state, "state");
            effects = List.copyOf(effects);
        }
    }

    /** The workbench's concrete snapshot acceptance reducer. */
    final class SnapshotReducer implements WorkbenchReducer<WorkbenchState> {

        @Override
        public Reduction<WorkbenchState> reduce(WorkbenchState state, WorkbenchEvent event) {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(event, "event");
            return switch (event) {
                case WorkbenchEvent.SnapshotExpected expected -> expect(state, expected.snapshot());
                case WorkbenchEvent.SnapshotPublished published -> publish(state, published.snapshot());
            };
        }

        private Reduction<WorkbenchState> expect(WorkbenchState state, WorkbenchSnapshot expected) {
            WorkbenchState next = state.expectSnapshot(expected);
            if (next.snapshot().equals(state.snapshot())) {
                return new Reduction<>(next, List.of());
            }
            return new Reduction<>(next, List.of(WorkbenchEffect.Render.INSTANCE));
        }

        private Reduction<WorkbenchState> publish(WorkbenchState state, WorkbenchSnapshot published) {
            WorkbenchState next = state.acceptSnapshot(published);
            if (next == state) {
                return new Reduction<>(state, List.of());
            }
            return new Reduction<>(next, List.of(WorkbenchEffect.Render.INSTANCE));
        }
    }
}
