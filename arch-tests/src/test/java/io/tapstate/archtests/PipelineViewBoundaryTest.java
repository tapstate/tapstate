package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/** Guards the Pipeline typed projection from bypassing the artifact query boundary. */
class PipelineViewBoundaryTest {

    private static final List<String> PIPELINE_VIEW_TYPES = List.of(
            "io.tapstate.control.core.PipelineView",
            "io.tapstate.control.core.PipelineRepresentation",
            "io.tapstate.control.core.PipelineViewService");

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importProductionClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.control.core");
    }

    @Test
    void pipelineTypedProjectionDoesNotDependOnArtifactStore() {
        assertThat(PIPELINE_VIEW_TYPES)
                .allSatisfy(type -> assertThat(tapstateClasses.contain(type)).isTrue());
        assertAll(PIPELINE_VIEW_TYPES.stream()
                .map(type -> () -> noClasses().that().haveFullyQualifiedName(type)
                        .should().dependOnClassesThat().areAssignableTo(ArtifactStore.class)
                        .check(tapstateClasses)));
    }
}
