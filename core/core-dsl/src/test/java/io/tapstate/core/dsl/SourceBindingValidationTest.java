package io.tapstate.core.dsl;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceBindingValidationTest {

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_a
            connector: mysql
            mode: cdc
            tables: [orders]
            """;

    @Test
    void invalidSourceAliasReportsItsYamlPath() {
        assertThatThrownBy(() -> validate("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [{id: src_a, as: invalid.alias}]
                view: {from: orders, primary_key: id}
                """)).isInstanceOfSatisfying(DslException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(DslError.ILLEGAL_VALUE);
                    assertThat(failure.path()).isEqualTo("source[0].as");
                });
    }

    @Test
    void conflictingNodeOverridesOnOneBindingAreRejected() {
        assertThatThrownBy(() -> validate("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [{id: src_a, nodeConfig: {preImage: true}},
                         {id: src_a, nodeConfig: {preImage: false}}]
                view: {from: orders, primary_key: id}
                """)).isInstanceOfSatisfying(DslException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(DslError.DUPLICATE_ID));
    }

    private static void validate(String pipeline) {
        var parser = new DslParser();
        Workspace.of(List.of(parser.parse(SOURCE), parser.parse(pipeline)));
    }
}
