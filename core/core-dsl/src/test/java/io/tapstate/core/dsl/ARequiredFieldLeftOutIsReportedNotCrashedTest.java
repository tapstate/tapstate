package io.tapstate.core.dsl;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Leaving a required payload field out of a transform body is the most ordinary mistake an author
 * writing one can make, so it has to come back the way every other malformed artifact does: a coded
 * refusal carrying the field path and the position the field was looked for at. A bare
 * NullPointerException naming an internal class is reserved for programmer errors, and this is not
 * one -- it is a property of the document the author handed in.
 *
 * <p>All four bodies below are read the same way: the parser takes the field with a plain scalar
 * accessor that answers null when the key is absent, and hands that null straight to a record which
 * null-checks it. A case naming only one of them would be satisfied by a guard on that single
 * branch while its siblings kept crashing, so the four shapes travel together as one case, and
 * every row that is still undiagnosed is named in the failure rather than only the first.
 */
class ARequiredFieldLeftOutIsReportedNotCrashedTest {

    private final DslParser parser = new DslParser();

    /** Each artifact is well formed and complete apart from the one required field named in the key. */
    private static Map<String, String> bodiesMissingARequiredField() {
        Map<String, String> bodies = new LinkedHashMap<>();
        bodies.put("join without sql", """
                version: tapstate/v1
                kind: transform
                id: no_sql
                type: join
                engine: builtin
                """);
        bodies.put("join without engine", """
                version: tapstate/v1
                kind: transform
                id: no_engine
                type: join
                sql: |
                  SELECT c.id AS customer_id FROM c JOIN o ON o.customer_id = c.id
                """);
        bodies.put("filter without expr", """
                version: tapstate/v1
                kind: transform
                id: no_expr
                type: filter
                """);
        bodies.put("js without script", """
                version: tapstate/v1
                kind: transform
                id: no_script
                type: js
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
            // A refusal an author can act on names where to look, in the document and on the page.
            if (refusal.path() == null || refusal.path().isBlank() || refusal.line() <= 0) {
                undiagnosed.add(name + " -> " + refusal.code().code()
                        + " at path '" + refusal.path() + "' line " + refusal.line());
            }
        });

        assertThat(undiagnosed)
                .as("bodies whose absent required field was not reported as a located dsl error")
                .isEmpty();
    }
}
