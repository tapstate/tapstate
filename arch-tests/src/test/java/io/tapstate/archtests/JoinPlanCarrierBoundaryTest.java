package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaParameterizedType;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The join plan is what an execution carrier is handed, and the carrier can only be swapped while
 * the plan says nothing about the library that produced it. A signature is where that leaks: a
 * consumer able to see a SQL-library type in one compiles against it, and by then the swap has
 * quietly stopped being available. Stated in a document, this holds right up until somebody writes
 * a second carrier and finds out it does not, which is the day it is most expensive to learn.
 *
 * <p>The front end itself is exempt. It is the one class whose job is to talk to that library, and
 * it does so inside method bodies, which no consumer can see or depend on.
 */
class JoinPlanCarrierBoundaryTest {

    private static final String FRONT_END = "io.tapstate.core.sql.SqlFrontEnd";
    private static final String SQL_LIBRARY = "org.apache.calcite.";

    private static JavaClasses planClasses;

    @BeforeAll
    static void importPlanClasses() {
        planClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.core.sql");
    }

    @Test
    @DisplayName("no type the plan is made of names the SQL library in a signature")
    void planSignaturesNameNoSqlLibraryType() {
        assertThat(planClasses.stream().map(JavaClass::getSimpleName).toList())
                .as("positive control: the plan must be in the scan at all, or the ban below rules "
                        + "on an empty set and passes for that reason")
                .contains("JoinPlan", "JoinTree", "JoinKey");

        List<String> leaks = planClasses.stream()
                .filter(type -> !isFrontEnd(type))
                .flatMap(JoinPlanCarrierBoundaryTest::signatureTypes)
                .filter(name -> name.startsWith(SQL_LIBRARY))
                .distinct()
                .toList();

        assertThat(leaks)
                .as("a carrier consuming the plan would compile against these, and the boundary "
                        + "would be gone before anyone noticed it had been crossed")
                .isEmpty();
    }

    @Test
    @DisplayName("the scan does read signatures, so the emptiness above is a finding not a miss")
    void theScanFindsSqlLibraryTypesWhereTheyAreExpected() {
        List<String> insideTheFrontEnd = planClasses.stream()
                .filter(JoinPlanCarrierBoundaryTest::isFrontEnd)
                .flatMap(JoinPlanCarrierBoundaryTest::signatureTypes)
                .filter(name -> name.startsWith(SQL_LIBRARY))
                .distinct()
                .toList();

        assertThat(insideTheFrontEnd)
                .as("positive control: the front end's own members do take and return them, so a "
                        + "scan that reads no signature at all reddens here instead of passing above")
                .isNotEmpty();
    }

    /** The front end and anything nested in it, including the tables it builds anonymously. */
    private static boolean isFrontEnd(JavaClass type) {
        String name = type.getFullName();
        return name.equals(FRONT_END) || name.startsWith(FRONT_END + "$");
    }

    /** Every type named by this class's fields, return types and parameters. */
    private static Stream<String> signatureTypes(JavaClass type) {
        Stream<JavaType> fields = type.getFields().stream().map(JavaField::getType);
        Stream<JavaType> returned = type.getMethods().stream().map(JavaMethod::getReturnType);
        Stream<JavaType> parameters = Stream.concat(
                        type.getMethods().stream(), type.getConstructors().stream())
                .map(JavaCodeUnit.class::cast)
                .flatMap(unit -> unit.getParameterTypes().stream());
        return Stream.of(fields, returned, parameters)
                .flatMap(stream -> stream)
                .flatMap(JoinPlanCarrierBoundaryTest::names);
    }

    /**
     * A type's own name plus every name inside it. Erasure alone would miss the interesting case:
     * a list of a SQL-library type erases to a list, and the type that matters disappears.
     */
    private static Stream<String> names(JavaType type) {
        Stream<String> self = Stream.of(type.toErasure().getFullName());
        if (type instanceof JavaParameterizedType parameterized) {
            return Stream.concat(self, parameterized.getActualTypeArguments().stream()
                    .flatMap(JoinPlanCarrierBoundaryTest::names));
        }
        return self;
    }
}
