package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrictYamlTest {

    @Test
    void parsesTheSupportedScalarSubsetAndComments() {
        Map<String, Object> document = StrictYaml.parse("""
                # a leading comment
                name: "line\\nvalue" # an inline comment
                plain: hello#not-a-comment
                quoted: 'it''s fine'
                number: -42
                enabled: true
                disabled: false
                empty: null
                omitted: ~
                emptyMap: {}
                emptyList: []
                seeds:
                  - https://one.example.com
                  - "https://two.example.com/path:443"
                """);

        assertThat(document)
                .containsEntry("name", "line\nvalue")
                .containsEntry("plain", "hello#not-a-comment")
                .containsEntry("quoted", "it's fine")
                .containsEntry("number", -42)
                .containsEntry("enabled", true)
                .containsEntry("disabled", false)
                .containsEntry("empty", null)
                .containsEntry("omitted", null)
                .containsEntry("emptyMap", Map.of())
                .containsEntry("emptyList", List.of())
                .containsEntry("seeds", List.of("https://one.example.com", "https://two.example.com/path:443"));
    }

    @Test
    void acceptsCrLfAndRejectsAnEmptyDocument() {
        assertThat(StrictYaml.parse("name: value\r\n")).containsEntry("name", "value");
        assertThatThrownBy(() -> StrictYaml.parse(""))
                .isInstanceOf(StrictYaml.ParseFailure.class)
                .hasMessageContaining("empty");
    }

    @Test
    void rejectsUnsupportedStructureAndMalformedIndentation() {
        assertParseFailure("- root sequence");
        assertParseFailure("name value");
        assertParseFailure("name:");
        assertParseFailure("name:\n    nested: value");
        assertParseFailure("name: value\n  extra: value");
        assertParseFailure("items:\n  - one\n    - two");
        assertParseFailure("name: [one, two]");
        assertParseFailure("name: &anchor value");
        assertParseFailure("name: !tag value");
        assertParseFailure("---\nname: value");
        assertParseFailure("%YAML 1.2\nname: value");
    }

    @Test
    void rejectsDuplicateKeysInvalidKeysAndOutOfRangeIntegers() {
        assertParseFailure("name: one\nname: two");
        assertParseFailure(": missing key");
        assertParseFailure("number: 999999999999999999999");
        assertParseFailure("bad\tkey: value");
        assertParseFailure(" bad: value");
    }

    @Test
    void rejectsBrokenQuotesAndEscapes() {
        assertParseFailure("name: \"unterminated");
        assertParseFailure("name: 'unterminated");
        assertParseFailure("name: \"bad\\q\"");
        assertParseFailure("name: 'bad\nvalue'");
    }

    private static void assertParseFailure(String source) {
        assertThatThrownBy(() -> StrictYaml.parse(source))
                .isInstanceOf(StrictYaml.ParseFailure.class);
    }
}
