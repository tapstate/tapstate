package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.PipelineDraft;
import org.bson.Document;

/** Idempotent migrations for the versioned Pipeline draft document. */
final class PipelineDraftMigrations {

    private PipelineDraftMigrations() {}

    static Document migrate(Document source) {
        Document document = new Document(source);
        int version = document.getInteger("schemaVersion", 0);
        if (version == 0) {
            document.put("schemaVersion", PipelineDraft.CURRENT_SCHEMA_VERSION);
            document.putIfAbsent("mode", "dag");
            document.putIfAbsent("graph", new Document("nodes", java.util.List.of())
                    .append("edges", java.util.List.of())
                    .append("viewport", new Document("x", 0D).append("y", 0D).append("zoom", 1D)));
        }
        if (document.getInteger("schemaVersion", 0) != PipelineDraft.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported pipeline draft schema version");
        }
        return document;
    }
}
