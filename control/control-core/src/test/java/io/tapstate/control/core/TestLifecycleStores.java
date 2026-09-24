package io.tapstate.control.core;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StateStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory lifecycle documents for tests that need to say what a pipeline is doing.
 *
 * <p>Shared rather than copied into each test: the guards under test read both halves of the
 * lifecycle, and a per-test fake that answered one of them differently would make the tests disagree
 * about the states that matter while every one of them stayed green.
 */
final class TestLifecycleStores {

    private TestLifecycleStores() {
    }

    /** A pipeline that is doing {@code actual} and has been asked to be {@code intent}. */
    static LivePipelines livePipelines(Desired desired, State state) {
        return new LivePipelines(desired, state);
    }

    static final class Desired implements DesiredStore {
        private final Map<String, DesiredState> docs = new LinkedHashMap<>();

        void put(String pipelineId, PipelineState target) {
            docs.put(pipelineId, new DesiredState(pipelineId, target, "0".repeat(64)));
        }

        @Override
        public void save(DesiredState desired) {
            docs.put(desired.pipelineId(), desired);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return new ArrayList<>(docs.keySet());
        }

        @Override
        public void delete(String pipelineId) {
            docs.remove(pipelineId);
        }
    }

    static final class State implements StateStore {
        private final Map<String, CheckpointDoc> docs = new LinkedHashMap<>();

        void put(String pipelineId, PipelineState actual) {
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, StateJson.of(actual), Instant.EPOCH));
        }

        @Override
        public Optional<CheckpointDoc> read(String pipelineId) {
            return Optional.ofNullable(docs.get(pipelineId));
        }

        @Override
        public void create(String pipelineId, String stateJson, Instant touchTime) {
            docs.put(pipelineId, CheckpointDoc.initial(pipelineId, stateJson, touchTime));
        }

        @Override
        public CasOutcome compareAndSwap(
                String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            throw new UnsupportedOperationException("these guards never write state");
        }

        @Override
        public void delete(String pipelineId) {
            docs.remove(pipelineId);
        }
    }
}
