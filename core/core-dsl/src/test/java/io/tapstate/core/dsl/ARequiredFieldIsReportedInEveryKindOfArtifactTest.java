package io.tapstate.core.dsl;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A required field is left out the same way everywhere, so it has to come back the same way
 * everywhere: a coded refusal carrying the field path and the position the field was looked for at.
 * Transform bodies are only where an author notices it first -- the identity of a resource, the
 * table a source selects, the layer a view materializes on and each element of a serve surface are
 * read through the same null-answering accessors into the same null-checking records.
 *
 * <p>Every row below is a different reader, deliberately: a guard installed on the readers one
 * report happened to name leaves the rest crashing, and nothing about a bare NullPointerException
 * naming an internal component tells the author which of their fields it came from. Each row is
 * complete apart from the one required field its key names, and every row still undiagnosed is
 * listed in the failure rather than only the first.
 */
class ARequiredFieldIsReportedInEveryKindOfArtifactTest {

    private final DslParser parser = new DslParser();

    private static Map<String, String> bodiesMissingARequiredField() {
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("source without an id", """
                version: tapstate/v1
                kind: source
                connector: mysql
                """);
        bodies.put("source without a connector", """
                version: tapstate/v1
                kind: source
                id: orders_db
                """);
        bodies.put("table selection without a name", """
                version: tapstate/v1
                kind: source
                id: orders_db
                connector: mysql
                tables:
                  - filter: "row.total > 0"
                """);
        bodies.put("source reference without an id", """
                version: tapstate/v1
                kind: pipeline
                id: orders
                source:
                  - srs: true
                """);
        bodies.put("map without fields", """
                version: tapstate/v1
                kind: transform
                id: trim
                type: map
                """);
        bodies.put("nest without a root", """
                version: tapstate/v1
                kind: transform
                id: orders_nested
                type: nest
                """);
        bodies.put("nest root without a from", """
                version: tapstate/v1
                kind: transform
                id: orders_nested
                type: nest
                root:
                  key: [id]
                """);
        bodies.put("embed without an on", """
                version: tapstate/v1
                kind: transform
                id: orders_nested
                type: nest
                root:
                  from: customers
                  embed:
                    - from: orders
                      as: array
                      path: orders
                """);
        bodies.put("hot storage without a ttl", """
                version: tapstate/v1
                kind: view
                id: customer_360
                storage:
                  hot: {}
                """);
        bodies.put("warm storage without a collection", """
                version: tapstate/v1
                kind: view
                id: customer_360
                storage:
                  warm: {}
                """);
        bodies.put("sync without a source", """
                version: tapstate/v1
                kind: serve
                id: customer_api
                sync:
                  - write_mode: upsert
                """);
        bodies.put("query without a type", """
                version: tapstate/v1
                kind: serve
                id: customer_api
                query:
                  - backend: mongodb
                """);
        bodies.put("push without a source", """
                version: tapstate/v1
                kind: serve
                id: customer_api
                push:
                  - topic: customers
                """);
        return bodies;
    }

    @Test
    void aBodyMissingARequiredFieldIsRefusedWithAPathAndAPositionRatherThanCrashing() {
        List<String> undiagnosed = new ArrayList<>();

        bodiesMissingARequiredField().forEach((name, yaml) -> {
            Throwable thrown = catchThrowable(() -> parser.parse(yaml));
            if (!(thrown instanceof DslException refusal)) {
                undiagnosed.add(name + " -> " + thrown);
                return;
            }
            if (refusal.path() == null || refusal.path().isBlank() || refusal.line() <= 0) {
                undiagnosed.add(name + " -> " + refusal.code().code()
                        + " at path '" + refusal.path() + "' line " + refusal.line());
            }
        });

        assertThat(undiagnosed)
                .as("bodies whose absent required field was not reported as a located dsl error")
                .isEmpty();
    }

    /**
     * The refusal names the field that was left out, not merely the artifact it was left out of.
     * A path alone puts an author on the right line; the field name is what tells them which of the
     * fields on it the parser went looking for and did not find.
     */
    @Test
    void theRefusalNamesTheFieldThatWasLeftOut() {
        Throwable thrown = catchThrowable(() -> parser.parse("""
                version: tapstate/v1
                kind: view
                id: customer_360
                primary_key: customer_id
                storage:
                  warm: {}
                """));

        assertThat(thrown).isInstanceOf(DslException.class);
        DslException refusal = (DslException) thrown;
        assertThat(refusal.path()).isEqualTo("storage.warm.collection");
        assertThat(refusal.args()).containsEntry("detail", "required field 'collection' is missing");
    }
}
