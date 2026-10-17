package io.tapstate.archtests;

import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WatermarkPolicy;
import com.hazelcast.jet.impl.execution.WatermarkCoalescer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No production code may let an input queue stop constraining the bound the engine combines across
 * queues. The durable frontier of a job whose sink acks positions rests entirely on that combination: it
 * is the lowest promise across every queue, and it is safe precisely because a queue that has said
 * nothing still holds it down. A queue marked idle is dropped from the combination, so a silent instance
 * stops being waited for and the bound jumps to the highest any other queue reached — past changes that
 * are still in flight, with the job reporting nothing at all.
 *
 * <p>This is the one rule whose breach is both silent and immediately wrong, which is why it is a gate
 * rather than a review note: nothing throws, no counter moves, the pipeline stays healthy, and the
 * durable position simply stops being true. Marking a queue idle also disregards which axis carried the
 * marker, so one axis is enough to take the whole queue out.
 *
 * <p>Every mechanism below is checked against a fixture that really uses it before production is scanned.
 * A ban that matches nothing passes for free, and would keep passing after the API it names is renamed;
 * the positive control per mechanism is what stops this gate from quietly disarming itself.
 */
class IdleQueueGatesTest {

    /** Each way the engine can be told to let a queue go idle, by the name this gate reports it under. */
    private static final Map<String, Predicate<JavaAccess<?>>> IDLE_MECHANISMS = idleMechanisms();

    private static JavaClasses productionClasses;
    private static JavaClasses fixtureClasses;

    @BeforeAll
    static void importClasses() {
        productionClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
        fixtureClasses = new ClassFileImporter().importClasses(QueueThatGoesIdle.class);
    }

    @Test
    @DisplayName("positive control: every banned mechanism is still detected on code that uses it")
    void eachIdleMechanismIsDetectedOnAFixtureThatUsesIt() {
        IDLE_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(fixtureClasses, detects))
                .as("%s is no longer detected — the API it names has moved or been renamed, and this "
                        + "gate would pass over production code that uses it", mechanism)
                .isNotEmpty());
    }

    @Test
    @DisplayName("no production code marks an input queue idle")
    void noProductionCodeLetsAQueueGoIdle() {
        IDLE_MECHANISMS.forEach((mechanism, detects) -> assertThat(accesses(productionClasses, detects))
                .as("%s: a queue dropped from the combined bound stops holding the frontier down, so "
                        + "the durable position runs past changes still in flight and nothing reports it",
                        mechanism)
                .isEmpty());
    }

    private static Map<String, Predicate<JavaAccess<?>>> idleMechanisms() {
        Map<String, Predicate<JavaAccess<?>>> mechanisms = new LinkedHashMap<>();
        mechanisms.put("an event-time policy (every one of them carries an idle timeout)",
                access -> targets(access, EventTimePolicy.class) && access.getName().equals("eventTimePolicy"));
        mechanisms.put("the event-time mapper (it turns a quiet partition into the idle marker)",
                access -> targets(access, EventTimeMapper.class));
        mechanisms.put("a source's partition idle timeout",
                access -> access.getName().equals("setPartitionIdleTimeout"));
        mechanisms.put("the idle marker itself",
                access -> targets(access, WatermarkCoalescer.class));
        return Map.copyOf(mechanisms);
    }

    private static boolean targets(JavaAccess<?> access, Class<?> owner) {
        return access.getTargetOwner().getName().equals(owner.getName());
    }

    /** Every access these classes make that the mechanism matches, named by where it was made. */
    private static List<String> accesses(JavaClasses classes, Predicate<JavaAccess<?>> detects) {
        return classes.stream()
                .flatMap(type -> type.getAccessesFromSelf().stream())
                .filter(detects)
                .map(access -> access.getOriginOwner().getName() + " -> " + access.getTarget().getFullName())
                .toList();
    }

    /**
     * Uses every banned mechanism, so the detectors above are checked against real bytecode rather than
     * against a name that may no longer exist. Never called: it exists to be read by the importer.
     */
    @SuppressWarnings("unused")
    private static final class QueueThatGoesIdle {

        private static EventTimePolicy<Object> policyWithAnIdleTimeout() {
            return EventTimePolicy.eventTimePolicy(
                    event -> 0L, WatermarkPolicy.limitingLag(0), 0, 0, 1000);
        }

        private static Object mappedIdle() {
            return new EventTimeMapper<>(policyWithAnIdleTimeout()).flatMapIdle();
        }

        private static StreamSource<Object> sourceGoingIdle(StreamSource<Object> source) {
            return source.setPartitionIdleTimeout(1000);
        }

        private static Watermark idleMarker() {
            return WatermarkCoalescer.IDLE_MESSAGE;
        }
    }
}
