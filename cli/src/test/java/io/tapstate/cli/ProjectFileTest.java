package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The project file {@code new} writes reads back as the id it was given, whatever that id looks like to
 * YAML: a word YAML takes for a boolean or a null, a number, or one carrying a quote, is quoted so the
 * parser sees the same string the user typed.
 */
class ProjectFileTest {

    @ParameterizedTest
    @ValueSource(strings = {"orders", "bank-c360", "true", "No", "null", "2026", "o\"brien", "back\\\\slash", "with space"})
    void theWrittenIdReadsBackUnchanged(String id) {
        assertThat(new DslParser().parseProject(ProjectFile.content(id)).id()).isEqualTo(id);
    }
}
