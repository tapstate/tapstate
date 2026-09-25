package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import io.tapstate.runtime.engine.MemberOutOfMemory;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;

/**
 * Once the engine's member has been shut down for want of memory, the process's liveness is broken, whether or
 * not anything announcing the loss got through.
 *
 * <p>The member is shut down on the thread the error reached, by handling that swallows whatever is thrown from
 * it, on a heap that has just run out. An announcement made from there can run out of memory too, and one that
 * did was lost without a trace: the record the engine fails its pipelines from was already written, so the
 * pipelines read failed, while the health check, which reads the liveness, went on passing until somebody
 * restarted the server by hand. So the assembly here is booted with a listener that runs out of memory itself
 * when a broken liveness is announced to it, ahead of everything else that listens, and the liveness is held to
 * agree with that record all the same.
 *
 * <p>The healthy half is asserted first, so that a liveness that was never correct cannot pass for one that
 * broke.
 */
class AnEngineLostToOutOfMemoryBreaksLivenessTest {

    /**
     * The error the collector raises when it gives up. The member's handling weighs a single refused allocation
     * against how full the heap is before it acts, and this heap is not full; a collector that gave up is reason
     * enough on its own.
     */
    private static final String THE_COLLECTOR_GAVE_UP = "GC overhead limit exceeded";

    /** How long the member thread gets to hand its error over and end. The handling runs on it, synchronously. */
    private static final Duration HANDED_OVER_WITHIN = Duration.ofSeconds(60);

    @Test
    void theLivenessIsBrokenEvenWhenAnnouncingTheLossRunsOutOfMemoryToo() throws InterruptedException {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .web(WebApplicationType.NONE)
                .properties("tapstate.store.mongo.enabled=false")
                .listeners(new RunsOutOfMemoryWhenABrokenLivenessIsAnnounced())
                .run()) {
            HazelcastInstance member = context.getBean(HazelcastInstance.class);
            ApplicationAvailability availability = context.getBean(ApplicationAvailability.class);
            assertThat(availability.getLivenessState())
                    .as("while its engine is up, the process is alive")
                    .isEqualTo(LivenessState.CORRECT);

            runOutOfMemoryOnAMemberThread();

            assertThat(member.getLifecycleService().isRunning())
                    .as("the member's own out-of-memory handling took it down")
                    .isFalse();
            assertThat(MemberOutOfMemory.of(member))
                    .as("the record the engine fails the member's pipelines from is written")
                    .isPresent();
            assertThat(availability.getLivenessState())
                    .as("and the liveness the health check reads agrees with it")
                    .isEqualTo(LivenessState.BROKEN);
        }
    }

    /**
     * Lets an out-of-memory error escape one of the member's own threads, which is how the member hears of it,
     * and returns once the member's out-of-memory handling is done with it.
     */
    private static void runOutOfMemoryOnAMemberThread() throws InterruptedException {
        Thread memberThread = new HazelcastManagedThread(() -> {
            throw new OutOfMemoryError(THE_COLLECTOR_GAVE_UP);
        }, "test-member-out-of-memory");
        memberThread.start();
        assertThat(memberThread.join(HANDED_OVER_WITHIN))
                .as("the member thread hands its out-of-memory error over and ends")
                .isTrue();
    }

    /**
     * Hears every change of availability ahead of everything else that listens, and runs out of memory itself
     * when the change is to a broken liveness, so that nothing listening after it hears of that change.
     */
    private static final class RunsOutOfMemoryWhenABrokenLivenessIsAnnounced
            implements ApplicationListener<AvailabilityChangeEvent<?>>, Ordered {

        @Override
        public void onApplicationEvent(AvailabilityChangeEvent<?> event) {
            if (event.getState() == LivenessState.BROKEN) {
                throw new OutOfMemoryError("Java heap space");
            }
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE;
        }
    }
}
