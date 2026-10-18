package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.control.core.OperationRegistry;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A face surface is every operation staged at or below a ceiling, and this gate pins where that ceiling
 * comes from: the one stage the build ships at, never an argument the calling face chose.
 *
 * <p>The difference is not stylistic. A face that names its own ceiling can open operations the build
 * does not claim to offer, and the call site reads as ordinary code — {@code exposedOn(MCP, BETA)} looks
 * like a lookup, not like a release decision, which is how an alpha build came to project its BETA-staged
 * operations onto a live tool catalog. Reading the stage from one place cannot be got wrong per face.
 *
 * <p>Method visibility already keeps the ceiling-taking form out of other packages; what it cannot keep
 * out is a production class in the registry's own package. That is the hole this gate closes, and it is
 * checked here rather than there because only a whole-scan sees every call site at once.
 */
class MaturityCeilingGatesTest {

    private static JavaClasses production;

    @BeforeAll
    static void importProductionClasses() {
        production = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    /** Every production call to {@code OperationRegistry.exposedOn} taking {@code parameterCount} arguments. */
    private static List<JavaMethodCall> surfaceDerivations(int parameterCount) {
        return production.stream()
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(call -> call.getTargetOwner().isAssignableTo(OperationRegistry.class)
                        && call.getName().equals("exposedOn")
                        && call.getTarget().getRawParameterTypes().size() == parameterCount)
                .toList();
    }

    @Test
    @DisplayName("a face derives its surface from the shipped stage, never from a ceiling it names itself")
    void productionNeverNamesItsOwnMaturityCeiling() {
        assertThat(surfaceDerivations(1))
                .as("positive control: some face must derive a surface at all, or the rule below rules on nothing")
                .isNotEmpty();
        assertThat(surfaceDerivations(2))
                .as("a face that names its own ceiling can open a wider surface than the build ships, and the "
                        + "call site reads as a lookup rather than as the release decision it actually is")
                .isEmpty();
    }

    @Test
    @DisplayName("the ceiling-taking form still exists, so the ban is not written against an absent method")
    void theCeilingTakingFormStillExists() {
        List<JavaMethod> ceilingTaking = production.get(OperationRegistry.class).getMethods().stream()
                .filter(method -> method.getName().equals("exposedOn"))
                .filter(method -> method.getRawParameterTypes().size() == 2)
                .toList();

        assertThat(ceilingTaking)
                .as("renaming or dropping this overload would leave the ban above matching nothing and "
                        + "passing green while production went back to choosing its own ceiling")
                .hasSize(1);
    }
}
