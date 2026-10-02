package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeasuredLifecycleActuatorTest {

    @Test
    void actualVerbsAreTimedOnceIncludingFailedCallsAndPreparedStartCleanup() {
        LifecycleWorkDispatcher dispatcher = LifecycleWorkDispatcher.inline();
        LifecycleActuator actuator = new MeasuredLifecycleActuator(new StubActuator(), dispatcher);

        try (LifecycleActuator.PreparedStart prepared = actuator.prepareStart("flow-a")) {
            prepared.submit();
        }
        actuator.start("flow-b");
        assertThatThrownBy(() -> actuator.pause("flow-a"))
                .isInstanceOf(IllegalStateException.class).hasMessage("pause failed");
        actuator.resume("flow-a");
        actuator.stop("flow-a", false);
        actuator.stopForRebuildingResume("flow-b", false);

        List<MetricFact> facts = LifecycleProcessFacts.snapshot(
                dispatcher.health(), 0, dispatcher.startedAt(), Instant.now());
        MetricFact duration = facts.stream().filter(fact -> fact.name().equals(
                "tapstate.process.lifecycle.work.duration")).findFirst().orElseThrow();
        assertThat(duration.points()).hasSize(4);
        assertThat(count(duration, "start")).isEqualTo(2);
        assertThat(count(duration, "pause")).isEqualTo(1);
        assertThat(count(duration, "resume")).isEqualTo(1);
        assertThat(count(duration, "stop")).isEqualTo(2);
        assertThat(duration.points()).allSatisfy(point ->
                assertThat(point.attributes()).containsOnlyKeys(MetricAttributes.LIFECYCLE_VERB));
    }

    private static long count(MetricFact fact, String verb) {
        return fact.points().stream().filter(point -> verb.equals(
                point.attributes().get(MetricAttributes.LIFECYCLE_VERB))).findFirst().orElseThrow()
                .histogram().count();
    }

    private static final class StubActuator implements LifecycleActuator {
        @Override
        public void start(String pipelineId) {
        }

        @Override
        public void pause(String pipelineId) {
            throw new IllegalStateException("pause failed");
        }

        @Override
        public void resume(String pipelineId) {
        }

        @Override
        public void stop(String pipelineId, boolean purgeState) {
        }

        @Override
        public Optional<Throwable> failure(String pipelineId) {
            return Optional.empty();
        }

        @Override
        public Optional<Throwable> lost(String pipelineId) {
            return Optional.empty();
        }

        @Override
        public boolean isCarryingAJob(String pipelineId) {
            return false;
        }
    }
}
