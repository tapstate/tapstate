package io.tapstate.archtests;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.ReservedField;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The reserved row field is spelled by its registry and by nothing else the product ships.
 *
 * <p>Everything the product adds to a customer's row sits under one top-level name, and whatever
 * serves a row to a target removes what is not meant to leave by asking the registry who owns what.
 * That only holds while every writer goes through the registry. A class that wrote the name as a
 * literal of its own could put data under a sub-key the registry never heard of, or read it back by a
 * path the gate does not know, and nothing at runtime would say so: the row would simply carry it.
 *
 * <p>So the literal is refused in every shipped source file but the registry's. Matched on the exact
 * name, alone or followed by a path step, never on a prefix: several unrelated internal constants
 * begin with the same characters and are not this field.
 */
class ReservedFieldIsSpelledInOnePlaceTest {

    private static final Path REPOSITORY = Path.of("..");

    /** The registry, the one file allowed to spell the name. */
    private static final String REGISTRY = "core/core-common/src/main/java/io/tapstate/core/common/ReservedField.java";

    /** The name as a whole string literal, or as the first step of a dotted path inside one. */
    private static final Pattern THE_NAME_AS_A_LITERAL =
            Pattern.compile("\"" + Pattern.quote(ReservedField.NAME) + "(?:\"|\\.)");

    @Test
    @DisplayName("no shipped class but the registry spells the reserved field")
    void noShippedClassButTheRegistrySpellsTheReservedField() {
        List<String> offences = new ArrayList<>();
        boolean registrySeen = false;
        for (Path file : shippedSources()) {
            String relative = REPOSITORY.relativize(file).toString().replace('\\', '/');
            Matcher matcher = THE_NAME_AS_A_LITERAL.matcher(read(file));
            if (relative.equals(REGISTRY)) {
                registrySeen = matcher.find();
                continue;
            }
            while (matcher.find()) {
                offences.add(relative + ": " + matcher.group());
            }
        }

        assertThat(registrySeen)
                .as("positive control: the registry itself spells the name and this scan finds it "
                        + "there; if it does not, the scan below is reading nothing")
                .isTrue();
        assertThat(offences)
                .as("the reserved field written as a literal outside its registry; read and write "
                        + "it through the registry so the serving gate knows the owner")
                .isEmpty();
    }

    /**
     * The pattern has to see the shapes it refuses and pass the ones it must not, or an empty answer
     * over the real tree means nothing.
     */
    @Test
    @DisplayName("the pattern matches the name and a path under it, and nothing that merely starts like it")
    void thePatternMatchesTheNameAndNothingThatMerelyStartsLikeIt() {
        assertThat(THE_NAME_AS_A_LITERAL.matcher("row.put(\"__tap\", owners);").find()).isTrue();
        assertThat(THE_NAME_AS_A_LITERAL.matcher("Filters.gt(\"__tap.srs.q\", seq)").find()).isTrue();

        assertThat(THE_NAME_AS_A_LITERAL.matcher("String CARRIED = \"__tapstate_carried\";").find()).isFalse();
        assertThat(THE_NAME_AS_A_LITERAL.matcher("String renamed = \"__tap_src\";").find()).isFalse();
    }

    private static List<Path> shippedSources() {
        try (Stream<Path> walk = Files.walk(REPOSITORY)) {
            List<Path> sources = walk
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        String path = p.toString().replace('\\', '/');
                        return path.contains("/src/main/java/") && !path.contains("/target/");
                    })
                    .sorted()
                    .toList();
            assertThat(sources)
                    .as("shipped sources under " + REPOSITORY.toAbsolutePath().normalize()
                            + " — an empty scan and a scan of nothing read the same")
                    .hasSizeGreaterThan(100);
            return sources;
        } catch (IOException e) {
            throw new UncheckedIOException("walking " + REPOSITORY, e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }
}
