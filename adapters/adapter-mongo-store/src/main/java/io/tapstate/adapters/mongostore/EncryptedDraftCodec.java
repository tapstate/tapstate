package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftSummary;
import org.bson.BSONException;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Separate encrypted summary and body retain metadata-only reads without exposing private authoring values. */
final class EncryptedDraftCodec {
    private static final DocumentCodec BSON = new DocumentCodec();
    private static final Set<String> METADATA = Set.of("schemaVersion", "mode", "name", "description", "baseArtifactHash",
            "publishedDraftRevision", "publishedArtifactHash", "createdAt", "updatedAt", "updatedBy");
    private final SourceConfigCipherProvider ciphers;

    EncryptedDraftCodec(SourceConfigCipherProvider ciphers) { this.ciphers = Objects.requireNonNull(ciphers, "ciphers"); }

    PipelineDraft capture(PipelineDraft draft) {
        byte[] bytes = null;
        try {
            bytes = bytes(MongoPipelineDraftStore.toDocument(draft));
            Document document = document(bytes);
            PipelineDraftSummary summary = metadata(document, draft.pipelineId(), draft.revision(), document.getDate("updatedAt"), true);
            validateBody(document, summary.mode());
            return MongoPipelineDraftStore.fromDocument(document);
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ArithmeticException
                 | ClassCastException | NullPointerException invalid) {
            throw diagnostic(draft.pipelineId(), IoError.DOCUMENT_INVALID);
        } finally {
            if (bytes != null) Arrays.fill(bytes, (byte) 0);
        }
    }

    Document encode(PipelineDraft draft, ClientSession session) {
        if (draft.revision() == Long.MAX_VALUE) throw diagnostic(draft.pipelineId(), IoError.DOCUMENT_INVALID);
        SourceConfigCipher cipher = cipherForWrite(session);
        Document all = MongoPipelineDraftStore.toDocument(draft);
        String bodyField = draft.mode() == PipelineDraft.Mode.DAG ? "graph" : "wizard";
        Document body = new Document(bodyField, all.remove(bodyField));
        all.remove("_id"); all.remove("revision");
        byte[] summaryBytes = bytes(all);
        byte[] bodyBytes = bytes(body);
        try {
            return new Document("_id", draft.pipelineId()).append("formatVersion", 1).append("revision", draft.revision())
                    .append("deleted", false).append("updatedAt", Date.from(draft.updatedAt()))
                    .append("summary", cipher.encryptBytes(context(draft.pipelineId(), draft.revision(), "summary"), "pipeline-draft", summaryBytes))
                    .append("body", cipher.encryptBytes(context(draft.pipelineId(), draft.revision(), "body"), "pipeline-draft", bodyBytes));
        } finally {
            Arrays.fill(summaryBytes, (byte) 0); Arrays.fill(bodyBytes, (byte) 0);
        }
    }

    PipelineDraft decode(Document row) {
        String id = id(row);
        try {
            long revision = header(row, true);
            Document metadata = decrypt(row, "summary", revision);
            PipelineDraftSummary summary = metadata(metadata, id, revision, row.getDate("updatedAt"), false);
            Document body = decrypt(row, "body", revision);
            keys(body, summary.mode() == PipelineDraft.Mode.DAG ? "graph" : "wizard");
            validateBody(body, summary.mode());
            metadata.putAll(body);
            metadata.put("_id", id); metadata.put("revision", revision);
            return MongoPipelineDraftStore.fromDocument(metadata);
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ArithmeticException
                 | ClassCastException | NullPointerException | IndexOutOfBoundsException | BufferUnderflowException invalid) {
            throw diagnostic(id, IoError.DOCUMENT_UNREADABLE);
        }
    }

    PipelineDraftSummary summary(Document row) { return summary(row, false); }

    PipelineDraftSummary summaryProjection(Document row) { return summary(row, true); }

    private PipelineDraftSummary summary(Document row, boolean projection) {
        String id = id(row);
        try {
            long revision = header(row, !projection);
            return metadata(decrypt(row, "summary", revision), id, revision, row.getDate("updatedAt"), false);
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ArithmeticException
                 | ClassCastException | NullPointerException | IndexOutOfBoundsException | BufferUnderflowException invalid) {
            throw diagnostic(id, IoError.DOCUMENT_UNREADABLE);
        }
    }

    /** Authenticate both parts at open; a partially authored graph is still checked when requested by id. */
    void verify(Document row) {
        if (Boolean.TRUE.equals(row.get("deleted"))) { tombstoneRevision(row); return; }
        summary(row);
        String id = id(row);
        try {
            long revision = header(row, true);
            Document body = decrypt(row, "body", revision);
            if (body.size() != 1 || !(body.get("graph") instanceof Document || body.get("wizard") instanceof Document)) invalid();
        } catch (BSONException | CodecConfigurationException | IllegalArgumentException | ClassCastException
                 | IndexOutOfBoundsException | BufferUnderflowException invalid) {
            throw diagnostic(id, IoError.DOCUMENT_UNREADABLE);
        }
    }

    long tombstoneRevision(Document row) {
        if (!row.keySet().equals(Set.of("_id", "formatVersion", "revision", "deleted", "updatedAt"))
                || !(row.get("_id") instanceof String) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !Boolean.TRUE.equals(row.get("deleted")) || !(row.get("revision") instanceof Long revision)
                || revision < 1 || revision == Long.MAX_VALUE || !(row.get("updatedAt") instanceof Date)) {
            throw diagnostic(id(row), IoError.DOCUMENT_UNREADABLE);
        }
        return revision;
    }

    SourceConfigCipher cipherForWrite(ClientSession session) {
        SourceConfigCipher cipher = ciphers.currentForWrite();
        if (ciphers.requiresWriteFence() && !ciphers.fenceWrite(new SourceConfigWriteScope(session), cipher.activeKeyId())) {
            throw new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
        }
        return cipher;
    }

    private Document decrypt(Document row, String part, long revision) {
        String id = id(row);
        byte[] plaintext;
        try {
            plaintext = ciphers.current().decryptBytes(context(id, revision, part), "pipeline-draft", row.getString(part));
        } catch (TapstateException first) {
            if (first.code() != IoError.DOCUMENT_UNREADABLE) throw first;
            plaintext = ciphers.refresh().decryptBytes(context(id, revision, part), "pipeline-draft", row.getString(part));
        }
        try { return document(plaintext); }
        finally { Arrays.fill(plaintext, (byte) 0); }
    }

    private static long header(Document row, boolean withBody) {
        Set<String> fields = withBody ? Set.of("_id", "formatVersion", "revision", "deleted", "updatedAt", "summary", "body")
                : Set.of("_id", "formatVersion", "revision", "deleted", "updatedAt", "summary");
        if (!row.keySet().equals(fields) || !(row.get("_id") instanceof String) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !(row.get("revision") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                || !Boolean.FALSE.equals(row.get("deleted")) || !(row.get("updatedAt") instanceof Date)
                || !(row.get("summary") instanceof String) || (withBody && !(row.get("body") instanceof String))) invalid();
        return row.getLong("revision");
    }

    private static PipelineDraftSummary metadata(Document metadata, String id, long revision, Date updatedAt, boolean full) {
        Set<String> fields = new java.util.HashSet<>(METADATA);
        if (full) { fields.add("_id"); fields.add("revision"); fields.add(metadata.containsKey("graph") ? "graph" : "wizard"); }
        if (revision < 1 || revision == Long.MAX_VALUE || !metadata.keySet().equals(fields) || !Integer.valueOf(PipelineDraft.CURRENT_SCHEMA_VERSION).equals(metadata.get("schemaVersion"))
                || !(metadata.get("createdAt") instanceof Date created) || !(metadata.get("updatedAt") instanceof Date updated)
                || !updated.equals(updatedAt) || !(metadata.get("updatedBy") instanceof String author) || author.isBlank()) invalid();
        for (String field : List.of("name", "description", "baseArtifactHash", "publishedArtifactHash")) {
            if (metadata.get(field) != null && !(metadata.get(field) instanceof String)) invalid();
        }
        Object published = metadata.get("publishedDraftRevision");
        if (published != null && (!(published instanceof Long value) || value < 1 || value > revision)) invalid();
        Object mode = metadata.get("mode");
        if (!("dag".equals(mode) || "wizard".equals(mode))) invalid();
        return new PipelineDraftSummary(id, "dag".equals(mode) ? PipelineDraft.Mode.DAG : PipelineDraft.Mode.WIZARD,
                metadata.getString("name"), metadata.getString("description"), revision, metadata.getString("baseArtifactHash"),
                metadata.getLong("publishedDraftRevision"), metadata.getString("publishedArtifactHash"),
                metadata.getDate("createdAt").toInstant(), metadata.getDate("updatedAt").toInstant(), metadata.getString("updatedBy"));
    }

    private static void validateBody(Document body, PipelineDraft.Mode mode) {
        String field = mode == PipelineDraft.Mode.DAG ? "graph" : "wizard";
        Document value = body.get(field, Document.class);
        if (value == null) invalid();
        if (mode == PipelineDraft.Mode.DAG) {
            keys(value, "nodes", "edges", "viewport");
            for (Document node : documents(value, "nodes")) {
                keys(node, "id", "type", "sourceId", "table", "config", "metadata");
                normalize(node, "config"); normalize(node, "metadata");
            }
            for (Document edge : documents(value, "edges")) keys(edge, "id", "source", "target");
            keys(value.get("viewport", Document.class), "x", "y", "zoom");
        } else {
            keys(value, "root", "related", "transforms", "output");
            if (value.get("root") != null) {
                Document root = value.get("root", Document.class);
                keys(root, "id", "sourceId", "table", "key", "preTransforms"); strings(root, "key"); transforms(root, "preTransforms");
            }
            for (Document related : documents(value, "related")) {
                keys(related, "id", "parentId", "sourceId", "table", "relation", "preTransforms");
                Document relation = related.get("relation", Document.class);
                keys(relation, "on", "shape", "path", "key", "arrayKey"); strings(relation, "key"); strings(relation, "arrayKey");
                for (Document pair : documents(relation, "on")) keys(pair, "childField", "parentField");
                transforms(related, "preTransforms");
            }
            transforms(value, "transforms");
            if (value.get("output") != null) {
                Document output = value.get("output", Document.class); keys(output, "kind", "config"); normalize(output, "config");
            }
        }
    }
    private static void transforms(Document parent, String field) {
        for (Document transform : documents(parent, field)) { keys(transform, "id", "type", "fields"); normalize(transform, "fields"); }
    }
    private static void normalize(Document parent, String field) {
        Document value = parent.get(field, Document.class); if (value == null) invalid();
        parent.put(field, new Document(MongoCatalogStore.normalizeMap(value)));
    }
    private static List<Document> documents(Document parent, String field) {
        Object value = parent.get(field); if (!(value instanceof List<?>)) invalid();
        return ((List<?>) value).stream().map(item -> { if (!(item instanceof Document)) invalid(); return (Document) item; }).toList();
    }
    private static void strings(Document parent, String field) {
        Object value = parent.get(field); if (!(value instanceof List<?>)) invalid();
        for (Object item : (List<?>) value) if (!(item instanceof String)) invalid();
    }
    private static void keys(Document document, String... fields) {
        if (document == null || !document.keySet().equals(Set.of(fields))) invalid();
    }
    private static void invalid() { throw new IllegalArgumentException("Invalid encrypted draft shape"); }
    private static byte[] bytes(Document document) {
        RawBsonDocument raw = new RawBsonDocument(document, BSON);
        return Arrays.copyOfRange(raw.getBackingArray(), raw.getByteOffset(), raw.getByteOffset() + raw.getByteLength());
    }
    private static Document document(byte[] bytes) {
        if (bytes.length < 5 || ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt() != bytes.length || bytes[bytes.length - 1] != 0) invalid();
        return new RawBsonDocument(bytes).decode(BSON);
    }
    private static String context(String id, long revision, String part) { return JsonWriter.write(List.of("pipeline-draft", id, revision, part)); }
    private static String id(Document row) { return row.get("_id") instanceof String text ? text : "<invalid>"; }
    private static TapstateException diagnostic(String id, IoError code) {
        return new TapstateException(code, Map.of("id", id, "field", "draft"), null);
    }
}
