package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** Guards the Source typed projection from reaching around the generic artifact services. */
class SourceProjectionBoundaryTest {

    private static final List<String> SOURCE_BOUNDARY_TYPES = List.of(
            "io.tapstate.control.restapi.SourceController",
            "io.tapstate.control.core.SourceProjectionService",
            "io.tapstate.control.core.SourceRepresentation");

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importProductionClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.control.core", "io.tapstate.control.restapi");
    }

    @Test
    void sourceTypedProjectionDoesNotDependOnArtifactStore() {
        assertThat(SOURCE_BOUNDARY_TYPES)
                .allSatisfy(type -> assertThat(tapstateClasses.contain(type)).isTrue());
        assertAll(SOURCE_BOUNDARY_TYPES.stream()
                .map(type -> () -> noClasses().that().haveFullyQualifiedName(type)
                        .should().dependOnClassesThat().areAssignableTo(ArtifactStore.class)
                        .check(tapstateClasses)));
    }
}
