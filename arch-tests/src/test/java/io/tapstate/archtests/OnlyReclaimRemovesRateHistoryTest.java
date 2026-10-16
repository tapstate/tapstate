package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.spi.store.RateHistoryStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Stopping a pipeline does not remove its history; deleting it does. The history is an observation
 * kept beside the run, not the run's state: the gap either side of a stop is the stretch somebody most
 * wants to compare, and a stop that erased the chart would erase exactly that. Stop already clears the
 * pipeline's state, and clearing "everything about it" would be the natural next line to write — so this
 * holds that the one place samples are removed is the reclaim of the pipeline itself, and that a stop,
 * a purge, or anything else reaching for that call reddens here.
 */
class OnlyReclaimRemovesRateHistoryTest {

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    @Test
    @DisplayName("nothing but the artifact reclaim removes a pipeline's samples")
    void nothingButReclaimRemovesSamples() {
        noClasses()
                .that().doNotHaveFullyQualifiedName("io.tapstate.control.core.ArtifactMutationService")
                .and().doNotImplement(RateHistoryStore.class)
                .should().callMethod(RateHistoryStore.class, "deleteAll", String.class)
                .because("samples leave by age or with the pipeline they belong to; a stop keeps them, since the"
                        + " stretch either side of a stop is what a history is read for")
                .check(tapstateClasses);
    }
}
