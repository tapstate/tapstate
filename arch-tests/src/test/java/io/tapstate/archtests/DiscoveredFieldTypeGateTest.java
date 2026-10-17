package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import io.tapstate.spi.store.SourceField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An adapter that discovers a stream hands out columns carrying the tapstate type each one resolved to.
 * It is the only place that type can be worked out: the database's own spelling means nothing away from
 * the connector that declared it, so a column that leaves discovery without one leaves it without one
 * forever.
 *
 * <p><b>What this guards against is not a wrong mapping - it is no mapping at all.</b> A discoverer that
 * hands a column's name and the source's spelling and stops compiles, stores, serves and reports; every
 * column it produced simply carries the unresolved type, which reads exactly like a column whose type
 * genuinely could not be worked out. Nothing downstream refuses such a column, so the first report is a
 * target table built without a type somebody notices weeks later.
 *
 * <p>The rule is written now rather than after the fact because a second discovery path is going to be
 * built against this same boundary, and the cheapest way to write one is the form this forbids.
 */
class DiscoveredFieldTypeGateTest {

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    /**
     * The form whose whole meaning is "nothing resolved a type for this column". It exists for callers
     * holding only a source's spelling; an adapter standing at the connector is never one of them, and a
     * store reading a record back has three different reasons to give and must give the one it has.
     */
    private static ArchRule theTypelessFormIsNotForAdapters() {
        return noClasses().that().resideInAPackage("io.tapstate.adapters..")
                .should().callConstructor(SourceField.class, String.class, String.class)
                .because("an adapter either resolves a column's type or says which unknown it is; the "
                        + "form that does neither leaves every column it built indistinguishable from "
                        + "one whose type genuinely could not be worked out");
    }

    @Test
    @DisplayName("no adapter hands out a column with no type and no reason for having none")
    void noAdapterUsesTheTypelessForm() {
        // The scope is asserted before the rule, because a rule with nothing in scope passes for the
        // same reason a clean tree does - a renamed package, an importer that read no adapter, and a
        // tree that obeys the rule are one output otherwise.
        assertThat(tapstateClasses.stream()
                .anyMatch(imported -> imported.getPackageName().startsWith("io.tapstate.adapters")))
                .as("the rule has adapters in scope")
                .isTrue();

        theTypelessFormIsNotForAdapters().allowEmptyShould(true).check(tapstateClasses);
    }

    @Test
    @DisplayName("positive control: the rule sees the form it forbids")
    void theRuleSeesTheFormItForbids() {
        // Without this, a rule that had stopped matching anything at all - a renamed constructor, a
        // package that no longer holds the adapters - would pass for the same reason a clean tree does.
        JavaClasses control = new ClassFileImporter().importClasses(TypelessDiscoverer.class);

        assertThat(noClasses()
                .should().callConstructor(SourceField.class, String.class, String.class)
                .evaluate(control).hasViolation())
                .as("the check must fail over a class that does the thing")
                .isTrue();
    }

    /** The discoverer somebody writes next, in the shape this gate exists to turn away. */
    private static final class TypelessDiscoverer {

        static SourceField field(String name, String dataType) {
            return new SourceField(name, dataType);
        }
    }
}
