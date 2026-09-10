package io.tapstate.adapters.mongostore.schemamigration;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Validates old schema records without normalizing or discarding their stored observations. */
public final class LegacySchemaDocuments {

    private LegacySchemaDocuments() {
    }

    public static String owner(Document document) {
        String id = string(document.get("_id"), String.valueOf(document.get("_id")), "_id");
        if (id.isBlank() || id.contains(".")) {
            throw unreadable(id, "_id");
        }
        return id;
    }

    public static List<Document> tables(Document envelope, String id) {
        string(envelope.get("connectorId"), id, "connectorId");
        integer(envelope.get("discoveredAt"), id, "discoveredAt");
        List<Document> tables = documents(envelope.get("tables"), id, "tables");
        for (Document table : tables) {
            string(table.get("name"), id, "tables.name");
            for (Document field : optionalDocuments(table.get("fields"), id, "tables.fields")) {
                string(field.get("name"), id, "tables.fields.name");
                optionalString(field.get("type"), id, "tables.fields.type");
                optionalString(field.get("tapstateType"), id, "tables.fields.tapstateType");
                optionalString(field.get("unknownBecause"), id, "tables.fields.unknownBecause");
            }
            strings(table.get("primaryKey"), id, "tables.primaryKey");
            for (Document index : optionalDocuments(table.get("indexes"), id, "tables.indexes")) {
                string(index.get("name"), id, "tables.indexes.name");
                strings(index.get("fields"), id, "tables.indexes.fields");
                if (index.get("unique") != null && !(index.get("unique") instanceof Boolean)) {
                    throw unreadable(id, "tables.indexes.unique");
                }
            }
        }
        return tables;
    }

    public static List<Document> steps(Document legacy, String id) {
        List<Document> steps = documents(legacy.get("steps"), id, "steps");
        Set<String> names = new HashSet<>();
        for (Document step : steps) {
            String name = string(step.get("step"), id, "steps.step");
            if (!names.add(name)) {
                throw unreadable(id, "steps.step");
            }
            versions(step.get("versions"), id);
        }
        return steps;
    }

    public static List<Document> versions(Object raw, String id) {
        List<Document> versions = documents(raw, id, "versions");
        long previous = -1;
        for (Document version : versions) {
            long number = integer(version.get("version"), id, "versions.version");
            if (number <= previous) {
                throw unreadable(id, "versions.version");
            }
            previous = number;
            Set<String> columns = new HashSet<>();
            for (Document column : documents(version.get("columns"), id, "versions.columns")) {
                if (!columns.add(string(column.get("name"), id, "versions.columns.name"))) {
                    throw unreadable(id, "versions.columns.name");
                }
                string(column.get("type"), id, "versions.columns.type");
            }
            nonBlank(version.get("statement"), id, "versions.statement");
            nonBlank(version.get("derivedFrom"), id, "versions.derivedFrom");
            nonBlank(version.get("derivedBy"), id, "versions.derivedBy");
        }
        return versions;
    }

    /** Existing split records may have advanced after an earlier partial move; never roll them back. */
    public static void requirePreservedHistory(List<Document> legacy, Document split, String id) {
        List<Document> existing = versions(split.get("versions"), id);
        if (existing.size() < legacy.size()) {
            throw unreadable(id, "versions");
        }
        for (int i = 0; i < legacy.size(); i++) {
            Document old = legacy.get(i);
            Document current = existing.get(i);
            if (integer(old.get("version"), id, "versions.version")
                    != integer(current.get("version"), id, "versions.version")
                    || !old.get("columns").equals(current.get("columns"))) {
                throw unreadable(id, "versions");
            }
            // Only the latest version's provenance can have been refreshed by record(). Older
            // versions are immutable, so a disagreement there is not a resumable partial migration.
            if (i < legacy.size() - 1 && (!old.get("statement").equals(current.get("statement"))
                    || !old.get("derivedFrom").equals(current.get("derivedFrom"))
                    || !old.get("derivedBy").equals(current.get("derivedBy")))) {
                throw unreadable(id, "versions");
            }
        }
    }

    public static String string(Object value, String id, String field) {
        if (!(value instanceof String text)) {
            throw unreadable(id, field);
        }
        return text;
    }

    private static void nonBlank(Object value, String id, String field) {
        if (string(value, id, field).isBlank()) {
            throw unreadable(id, field);
        }
    }

    private static void optionalString(Object value, String id, String field) {
        if (value != null && !(value instanceof String)) {
            throw unreadable(id, field);
        }
    }

    private static long integer(Object value, String id, String field) {
        if (!(value instanceof Long) && !(value instanceof Integer)) {
            throw unreadable(id, field);
        }
        return ((Number) value).longValue();
    }

    private static List<Document> optionalDocuments(Object value, String id, String field) {
        return value == null ? List.of() : documents(value, id, field);
    }

    private static List<Document> documents(Object value, String id, String field) {
        if (!(value instanceof List<?> list)) {
            throw unreadable(id, field);
        }
        List<Document> documents = new ArrayList<>();
        for (Object entry : list) {
            if (!(entry instanceof Document document)) {
                throw unreadable(id, field);
            }
            documents.add(document);
        }
        return documents;
    }

    private static void strings(Object value, String id, String field) {
        if (value == null) {
            return;
        }
        if (!(value instanceof List<?> list)) {
            throw unreadable(id, field);
        }
        for (Object entry : list) {
            string(entry, id, field);
        }
    }

    public static TapstateException unreadable(String id, String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null);
    }
}
