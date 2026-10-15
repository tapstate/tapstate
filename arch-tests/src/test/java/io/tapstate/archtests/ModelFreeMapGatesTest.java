package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaParameterizedType;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A free map on a model record says "any key, any value, nobody checks". Two of them are meant to say
 * that and the rest are drift, so this gate names the two and refuses every other.
 *
 * <p>{@code config} carries a connector's connection settings. Its keys belong to whichever connector
 * the resource names, they differ per connector, and the product has no standing to enumerate them —
 * it stays free on purpose, validated against the connector's own declared spec rather than against a
 * type here.
 *
 * <p>{@code experimental} is the declared escape hatch from the compatibility freeze: whatever sits in
 * it is exempt precisely because it is marked as not yet a contract.
 *
 * <p>Everything else the product owns, so it has a type. Options in particular are the engine's own
 * configuration, and an engine option is a typed component plus a key in the parser's vocabulary — not
 * an entry in a map nothing validates.
 *
 * <p>Typed maps are untouched: a {@code Map<String, FieldRule>} states what it holds and is not a free
 * map. Only a value type of {@code Object} makes it free, so that is what this reads, not the raw type.
 *
 * <p>Scope is the DSL model and nothing else. The free maps elsewhere are free for reasons no type can
 * fix: the SPI's connector settings are {@code config} under another name, and the data-plane records
 * carry rows and discovered schemas, whose shape is the user's rather than ours. Widening this gate to
 * them would turn it into a list of names to keep adding to, which is the thing it exists instead of.
 */
class ModelFreeMapGatesTest {

    /** The two component names allowed to hold a free map, and why, is in this class's javadoc. */
    private static final List<String> ALLOWED = List.of("config", "experimental");

    private static JavaClasses modelClasses;

    @BeforeAll
    static void importClasses() {
        modelClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate.core.model");
    }

    /** A field of type {@code Map<?, Object>} — the shape that promises nothing about its values. */
    private static boolean isFreeMap(JavaField field) {
        JavaType type = field.getType();
        if (!(type instanceof JavaParameterizedType parameterized)) {
            return false;
        }
        if (!type.toErasure().isAssignableTo(Map.class)) {
            return false;
        }
        List<JavaType> args = parameterized.getActualTypeArguments();
        return args.size() == 2 && args.get(1).getName().equals(Object.class.getName());
    }

    @Test
    @DisplayName("the gate sees free maps at all (positive control)")
    void findsFreeMapsSomewhere() {
        // Without this, deleting every free map -- or breaking isFreeMap so it matches nothing --
        // would leave the rule below passing over an empty set, which reads exactly like compliance.
        List<String> allFreeMaps = modelClasses.stream()
                .flatMap(c -> c.getFields().stream())
                .filter(ModelFreeMapGatesTest::isFreeMap)
                .map(f -> f.getOwner().getSimpleName() + "." + f.getName())
                .collect(Collectors.toList());

        assertThat(allFreeMaps)
                .as("the exempt free maps must still be found, or this gate is measuring nothing")
                .isNotEmpty();
    }

    @Test
    @DisplayName("only config and experimental may be a free map")
    void noFreeMapOutsideTheTwoExemptions() {
        List<String> offenders = modelClasses.stream()
                .flatMap(c -> c.getFields().stream())
                .filter(ModelFreeMapGatesTest::isFreeMap)
                .filter(f -> !ALLOWED.contains(f.getName()))
                .map(f -> f.getOwner().getSimpleName() + "." + f.getName())
                .sorted()
                .collect(Collectors.toList());

        assertThat(offenders)
                .as("a free map outside 'config' (connector-owned) and 'experimental' (not yet a "
                        + "contract): give it a type, or state why the product cannot own its keys")
                .isEmpty();
    }
}
