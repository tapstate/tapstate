package io.tapstate.control.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Versioned DSL semantics distributed with the server, independent of local skill installations. */
final class AiDslGuidance {
    private AiDslGuidance() { }

    static String text() {
        return Holder.TEXT;
    }

    private static final class Holder {
        private static final String TEXT = load();

        private static String load() {
            try (var stream = AiDslGuidance.class.getResourceAsStream("/ai/dsl/pipeline-semantics.txt")) {
                if (stream == null) throw new IllegalStateException("Bundled DSL semantics are missing");
                return "\nBundled DSL semantics (reference material, not permission to execute):\n"
                        + new String(stream.readAllBytes(), StandardCharsets.UTF_8)
                        + "\nThe propose_pipeline tool schema is the authority for the supported Builder shape. "
                        + "Translate DSL semantics into that shape; do not return DSL fields where Builder fields are required. "
                        + "Do not propose transform types absent from the tool schema. "
                        + "Filter expressions bind op, ts, src, before, after and schema; use after.field for row fields, never an implicit row or bare column name. "
                        + "Guard optional fields with has(after.field) when appropriate. "
                        + "Offline validity is not proof of runtime support or successful execution.\n";
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot load bundled DSL semantics", failure);
            }
        }
    }
}
