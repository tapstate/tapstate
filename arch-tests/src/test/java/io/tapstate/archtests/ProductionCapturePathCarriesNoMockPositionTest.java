package io.tapstate.archtests;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Nothing the product ships invents a resume position.
 *
 * <p>A position says where a read carries on from, and the only honest sources of one are the store it
 * was written to and the connector that produced it. A constant standing in for either is the failure
 * this exists to stop, and it is a quiet one: the pipeline starts, reports healthy, and reads from a
 * place nothing ever observed — every change between the real position and the invented one is simply
 * never seen, and no assertion anywhere is about that gap.
 *
 * <p>The wiring module is where such a constant actually lived. While the capture path was being built
 * out it carried {@code MOCK_CDC_START}, a hardcoded token handed to the runtime in place of a read
 * one; the pipeline ran the whole time. It is gone, and today this module names no position type at
 * all — which is what makes the rule cheap to state and worth pinning: a value invented here has to be
 * written before it can be used, so both halves are refused.
 *
 * <p><b>What is deliberately not refused:</b> a constant that is not a position. The same module holds
 * {@code MOCK_SCHEMA_VER}, the schema version stamped on ring items while schema evolution is still a
 * later increment. It reads like a sibling of the constant above and is not one: a schema version is
 * not a place in a change stream, and nothing resumes from it. Keying this gate on the {@code MOCK_}
 * spelling rather than on what the value is would refuse it on the day it was written, which is how a
 * gate gets an exemption list and stops meaning anything.
 */
class ProductionCapturePathCarriesNoMockPositionTest {

    private static final Path REPOSITORY = Path.of("..");

    /**
     * The module this is about: the one that wires the capture path together. It is named rather than
     * derived, because the rule is about wiring inventing a value, not about the layers whose whole job
     * is to carry positions — {@code spi-capture} declares the type and {@code runtime/srs} reads and
     * writes real ones, so a scan over every module would have to exempt exactly those two.
     */
    private static final Path WIRING = Path.of("..", "app", "src", "main", "java");

    /** A position built out of a literal: the value came from the source file, not from a source. */
    private static final Pattern POSITION_FROM_A_LITERAL =
            Pattern.compile("new\\s+SourcePosition\\s*\\(\\s*[\"']");

    /**
     * A constant this module defines that names itself a position. Matched on what the name says the
     * value IS, never on how it is spelled: {@code MOCK_SCHEMA_VER} is not caught by this and must not
     * be, while {@code MOCK_CDC_START}, {@code FIXED_OFFSET} and {@code DEFAULT_WATERMARK} all are.
     */
    private static final Pattern A_POSITION_SHAPED_CONSTANT = Pattern.compile(
            "static\\s+final\\s+[\\w.<>\\[\\]]+\\s+\\w*"
                    + "(CDC_START|POSITION|WATERMARK|OFFSET|RESUME_FROM|START_TOKEN)"
                    + "\\w*\\s*=\\s*[\"'\\d]");

    @Test
    @DisplayName("the wiring module builds no position out of a literal")
    void theWiringModuleBuildsNoPositionOutOfALiteral() {
        assertThat(offences(POSITION_FROM_A_LITERAL))
                .as("a position built from a literal in the wiring module — it did not come from the "
                        + "source or the store, so the run silently skips everything before it")
                .isEmpty();
    }

    @Test
    @DisplayName("the wiring module defines no constant standing in for a position")
    void theWiringModuleDefinesNoConstantStandingInForAPosition() {
        assertThat(offences(A_POSITION_SHAPED_CONSTANT))
                .as("a constant in the wiring module whose name says it is a position and whose value "
                        + "is a literal — this is the shape MOCK_CDC_START had")
                .isEmpty();
    }

    /**
     * The gate has to be able to see the thing it refuses, or an empty answer means nothing. Both
     * patterns are run against the shape they exist to catch, so a rewrite that stops matching fails
     * here rather than passing quietly over the real tree.
     */
    @Test
    @DisplayName("both patterns match the constant that actually lived here")
    void bothPatternsMatchTheConstantThatActuallyLivedHere() {
        String asItWas = """
                    private static final String MOCK_CDC_START = "mock-cdc-start-0";
                    CaptureStart start = CaptureStart.resume(new SourcePosition("mock-cdc-start-0"));
                """;
        assertThat(A_POSITION_SHAPED_CONSTANT.matcher(asItWas).find()).isTrue();
        assertThat(POSITION_FROM_A_LITERAL.matcher(asItWas).find()).isTrue();

        // And the sibling that is not a position stays out of it, in both directions.
        String schemaVersion = "    private static final long MOCK_SCHEMA_VER = 0L;\n";
        assertThat(A_POSITION_SHAPED_CONSTANT.matcher(schemaVersion).find()).isFalse();
        assertThat(POSITION_FROM_A_LITERAL.matcher(schemaVersion).find()).isFalse();
    }

    private static List<String> offences(Pattern pattern) {
        List<String> found = new ArrayList<>();
        for (Path file : shippedSources()) {
            String body = read(file);
            Matcher matcher = pattern.matcher(body);
            while (matcher.find()) {
                found.add(REPOSITORY.relativize(file).toString().replace('\\', '/')
                        + ": " + matcher.group().trim());
            }
        }
        return found;
    }

    private static List<Path> shippedSources() {
        assertThat(Files.isDirectory(WIRING))
                .as("the wiring module's sources at " + WIRING + " — an empty scan and a scan of "
                        + "nothing read the same, so the root is checked rather than assumed")
                .isTrue();
        try (Stream<Path> walk = Files.walk(WIRING)) {
            return walk.filter(p -> p.toString().endsWith(".java")).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException("walking " + WIRING, e);
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
