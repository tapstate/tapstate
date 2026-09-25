package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The heap the capacity defaults are sized against and the heap the documentation requires are the same
 * number.
 *
 * <p>They are two statements of one fact, kept in two places because two different people read them: an
 * operator sizing a machine reads the page, and the limits read the constant. Nothing connects them, so
 * either can be changed alone - and both ways of drifting are silent. Raise the constant and the page
 * goes on asking for a machine that is now too small, so the limits are never reached on a member built
 * to the documentation. Raise the page and every limit stays sized for a member nobody is running any
 * more, which is the same failure from the other end.
 *
 * <p>Every figure the page states is checked rather than only the first one found, so a second mention
 * that disagrees with the first is caught too - which is the shape this actually drifts into, since the
 * number is stated once as a requirement and again wherever the reason for it is explained.
 */
class TheHeapTheLimitsAssumeIsTheOneTheDocsRequireTest {

    /**
     * The pages an operator reads before sizing a machine: where the requirement is stated, and where a
     * cluster reader meets it again. Both, because one page agreeing with the code while the other
     * quietly asks for something else is the drift this is here for - and the second page exists
     * precisely because the requirement is not only a cluster's.
     */
    private static final List<Path> REQUIREMENT = List.of(
            Path.of("../../docs/running-on-your-own-databases.md"),
            Path.of("../../docs/cluster/README.md"));

    private static final Pattern HEAP_FIGURE = Pattern.compile("(\\d+) GiB of JVM heap");

    @Test
    void thePageAsksForTheHeapTheDefaultsWereSizedAgainst() {
        List<Integer> stated = statedInTheDocumentation();

        assertThat(stated)
                .describedAs("the page states the requirement at all - without a figure on it this case "
                        + "passes over an empty list and the documentation could say nothing")
                .isNotEmpty();
        long expected = NestSettings.REFERENCE_MEMBER_HEAP_BYTES / (1024L * 1024L * 1024L);
        assertThat(stated)
                .describedAs("every figure %s states is the one the limits are divided out of", REQUIREMENT)
                .allSatisfy(gib -> assertThat((long) gib).isEqualTo(expected));
        assertThat(stated)
                .describedAs("and each page says it, rather than one of them carrying every mention while "
                        + "another names a machine size nowhere at all")
                .hasSizeGreaterThanOrEqualTo(REQUIREMENT.size());
    }

    private static List<Integer> statedInTheDocumentation() {
        List<Integer> figures = new ArrayList<>();
        for (Path page : REQUIREMENT) {
            String text;
            try {
                text = Files.readString(page);
            } catch (IOException cause) {
                throw new UncheckedIOException("cannot read " + page.toAbsolutePath(), cause);
            }
            Matcher found = HEAP_FIGURE.matcher(text);
            int onThisPage = 0;
            while (found.find()) {
                figures.add(Integer.parseInt(found.group(1)));
                onThisPage++;
            }
            if (onThisPage == 0) {
                throw new AssertionError(page + " states no heap figure at all, so a reader who arrives "
                        + "there is told nothing and this case would pass over what is missing");
            }
        }
        return figures;
    }
}
