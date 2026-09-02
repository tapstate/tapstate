package io.tapstate.core.dsl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * B3-3 hardening: malformed structure (a YAML node of the wrong kind, a non-string map
 * value, a non-boolean/non-numeric scalar, an out-of-range integer) must surface as a
 * positioned {@link DslException} — never a raw ClassCastException. This upholds the B3
 * error-model contract (human-readable file/line/field-path) for the kinds of mistakes a
 * user actually makes; the well-formed corpus never exercises these paths.
 */
class DslParserTypeErrorTest {

    private final DslParser parser = new DslParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedStructures")
    void malformedStructureThrowsDslExceptionNotClassCast(String name, String yaml) {
        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).as(name).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.line()).as("%s carries a source line", name).isPositive();
    }

    static Stream<Arguments> malformedStructures() {
        return Stream.of(
                Arguments.of("scalar field given a mapping", """
                        version: tapstate/v1
                        kind: source
                        id: { a: b }
                        connector: mysql
                        """),
                Arguments.of("serve given a sequence", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        serve: [a, b]
                        """),
                Arguments.of("tables given a mapping", """
                        version: tapstate/v1
                        kind: source
                        id: s
                        connector: mysql
                        tables: { a: b }
                        """),
                Arguments.of("source list element is a mapping", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: [s1, { x: y }]
                        """),
                Arguments.of("pk list element is a mapping", """
                        version: tapstate/v1
                        kind: source
                        id: s
                        connector: mysql
                        tables:
                          - name: ORDERS
                            pk: [id, { x: y }]
                        """),
                Arguments.of("streaming from list element is a mapping", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        transforms:
                          - id: m
                            type: map
                            from: [a, { x: y }]
                            fields: { c: $c }
                        """),
                Arguments.of("streaming from is a mapping", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        transforms:
                          - id: m
                            type: map
                            from: { a: b }
                            fields: { c: $c }
                        """),
                Arguments.of("nest alias value is a mapping", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        transforms:
                          - id: n
                            type: nest
                            from: { c: { nested: v }, o: orders }
                            root: { from: c, key: [id] }
                        """),
                Arguments.of("embed on value is non-string", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        transforms:
                          - id: n
                            type: nest
                            from: { parent: p, child: c }
                            root:
                              from: parent
                              key: [id]
                              embed:
                                - from: child
                                  on: { parent_id: 123 }
                                  as: array
                                  path: children
                        """),
                Arguments.of("srs queryable is non-boolean", """
                        version: tapstate/v1
                        kind: source
                        id: s
                        connector: mysql
                        mode: cdc
                        tables: [t]
                        srs: { queryable: "yes" }
                        """),
                Arguments.of("settings batch_size is non-numeric", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        settings: { batch_size: "lots" }
                        """),
                Arguments.of("settings batch_size overflows int", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        settings: { batch_size: 2147483648 }
                        """),
                Arguments.of("sync element is a scalar", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        serve:
                          from: /.*/
                          sync: [ not_a_mapping ]
                        """),
                Arguments.of("push format is a sequence", """
                        version: tapstate/v1
                        kind: pipeline
                        id: p
                        source: src
                        serve:
                          from: /.*/
                          push:
                            - source: tgt
                              topic: t
                              format: [a, b]
                        """));
    }

    @Test
    void aFreeFormConfigHoldingAListIsARefusalNamingConfigRatherThanACastCrash() {
        // config is read as a free map. An unchecked cast used to let a list through here and fail
        // later as a ClassCastException: an authoring mistake reported as a fault in the reader, with
        // no field, no position and no document anywhere in it.
        String yaml = """
                version: tapstate/v1
                kind: source
                id: src_ora
                connector: oracle
                config: [host, port]
                """;

        Throwable t = catchThrowable(() -> parser.parse(yaml));

        assertThat(t).isInstanceOf(DslException.class);
        DslException ex = (DslException) t;
        assertThat(ex.code()).isEqualTo(DslError.ILLEGAL_VALUE);
        assertThat(ex.path()).isEqualTo("config");
        assertThat(ex.args()).containsEntry("expected", "a mapping");
    }
}
