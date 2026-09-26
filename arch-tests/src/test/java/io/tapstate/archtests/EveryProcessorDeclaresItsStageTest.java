package io.tapstate.archtests;

import com.hazelcast.jet.core.Processor;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.runtime.engine.StageTimer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every processor family the engine draws a vertex with says which stage of the graph it runs, and every
 * stage in the closed set is run by some family. Both directions are held: a family without a stage is
 * time that will be spent and reported under no name, and a stage no family runs is a value the attribute
 * promises and never carries — which a reader would go looking for.
 *
 * <p>The stage vocabulary is read off the graph rather than chosen for it, so this is where "read off the
 * graph" is checked: a processor added without a stage reddens the first rule, and a stage added without a
 * processor reddens the second.
 */
class EveryProcessorDeclaresItsStageTest {

    /**
     * The vertices that run no processing of their own. A union, and the merge that gives a nest one edge
     * per stream, are topology: the vertex exists so ordinals downstream stay unique. The router in front of
     * a sink that runs several writers is topology too: all it does is pick which of two edges into those
     * writers a row takes. Nothing is spent in either that a reader would want to see on its own, and a
     * router timed under the sink's stage would fill it with units of next to nothing - the shape that hides
     * a slow writer. The processor that takes a step's input in the batches its author asked for only decides
     * when the processor it wraps is handed its rows; that one times its own stage, and timing the wrapper too
     * would count the same work twice.
     */
    private static final Set<String> TOPOLOGY = Set.of(
            "io.tapstate.runtime.engine.PassthroughProcessor",
            "io.tapstate.runtime.engine.SinkRouter",
            "io.tapstate.runtime.engine.InputBatches");

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    /** Every concrete processor the product wires into a graph. */
    private static List<JavaClass> processors() {
        return tapstateClasses.stream()
                .filter(candidate -> candidate.isAssignableTo(Processor.class))
                .filter(candidate -> !candidate.isInterface())
                .filter(candidate -> !candidate.getModifiers().contains(JavaModifier.ABSTRACT))
                .collect(Collectors.toList());
    }

    @Test
    @DisplayName("every processor family the engine wires declares its stage, except the ones that are topology")
    void everyProcessorDeclaresItsStage() {
        List<JavaClass> processors = processors();

        assertThat(processors).extracting(JavaClass::getName).containsAll(TOPOLOGY);
        assertThat(processors)
                .filteredOn(processor -> !TOPOLOGY.contains(processor.getName()))
                .allSatisfy(processor -> assertThat(processor.isAssignableTo(Staged.class))
                        .as("%s declares the stage its time is measured under", processor.getName())
                        .isTrue());
        for (String topology : TOPOLOGY) {
            assertThat(tapstateClasses.get(topology).isAssignableTo(Staged.class))
                    .as("%s spends no time a reader would want to see on its own", topology)
                    .isFalse();
        }
    }

    @Test
    @DisplayName("every stage in the closed set is the stage of some processor family")
    void everyStageIsRunBySomeFamily() {
        Set<String> declared = processors().stream()
                .filter(processor -> processor.isAssignableTo(Staged.class))
                .map(EveryProcessorDeclaresItsStageTest::stageDeclaredBy)
                .collect(Collectors.toSet());

        assertThat(declared).containsExactlyInAnyOrderElementsOf(
                Arrays.stream(Stage.values()).map(Enum::name).collect(Collectors.toList()));
    }

    @Test
    @DisplayName("every processor family that declares a stage obtains a timer and ends units of work with it")
    void everyStagedProcessorTimesItsStage() {
        // Declaring a stage says where a processor's time would be reported; timing it is what puts a number
        // there. A family that declares and does not time reports its stage as a name with an empty
        // distribution behind it, which reads as a stage that costs nothing.
        //
        // Which is why obtaining the timer is not enough to ask for: a processor that calls `of` in init,
        // keeps the timer and never ends a unit passes that question while being exactly the failure above
        // -- and a stage reported as fast is harder to notice than a stage reported as missing. The calls
        // are collected across the class, its lambdas included, because a family that ends its unit inside
        // one handed to a flat-mapper is doing it correctly.
        assertThat(processors())
                .filteredOn(processor -> processor.isAssignableTo(Staged.class))
                .allSatisfy(processor -> assertThat(stageTimerCallsFrom(processor))
                        .as("%s obtains a stage timer and ends timed units of work with it",
                                processor.getName())
                        .contains("of", "end"));
    }

    /** The stage-timer methods {@code processor} calls, anywhere in the class, its lambdas included. */
    private static Set<String> stageTimerCallsFrom(JavaClass processor) {
        return processor.getMethodCallsFromSelf().stream()
                .filter(call -> call.getTargetOwner().isEquivalentTo(StageTimer.class))
                .map(call -> call.getTarget().getName())
                .collect(Collectors.toSet());
    }

    /**
     * The constant {@code stage()} answers with, read off the method's own field access rather than by
     * running it: a processor is built for a job, not for a test.
     */
    private static String stageDeclaredBy(JavaClass processor) {
        List<String> constants = processor.getMethod("stage").getFieldAccesses().stream()
                .filter(access -> access.getTargetOwner().isEquivalentTo(Stage.class))
                .map(access -> access.getTarget().getName())
                .distinct()
                .collect(Collectors.toList());
        assertThat(constants).as("%s names exactly one stage", processor.getName()).hasSize(1);
        return constants.get(0);
    }
}
