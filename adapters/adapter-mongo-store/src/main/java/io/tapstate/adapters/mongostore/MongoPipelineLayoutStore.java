package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.PipelineLayoutStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** MongoDB storage for editor-only Pipeline node positions and viewport state. */
public final class MongoPipelineLayoutStore implements PipelineLayoutStore {

    private final MongoCollection<Document> collection;

    public MongoPipelineLayoutStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<PipelineLayout> get(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", pipelineId)).first());
        return document == null ? Optional.empty() : Optional.of(toLayout(document));
    }

    @Override
    public void save(PipelineLayout layout) {
        Objects.requireNonNull(layout, "layout");
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", layout.pipelineId()), toDocument(layout), new ReplaceOptions().upsert(true)));
    }

    @Override
    public void delete(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        StoreIo.run(() -> collection.deleteOne(new Document("_id", pipelineId)));
    }

    static Document toDocument(PipelineLayout layout) {
        List<Document> nodes = new ArrayList<>(layout.nodes().size());
        layout.nodes().forEach((id, position) -> nodes.add(new Document("id", id)
                .append("x", position.x())
                .append("y", position.y())));
        Document document = new Document("_id", layout.pipelineId()).append("nodes", nodes);
        if (layout.viewport() != null) {
            document.append("viewport", new Document("x", layout.viewport().x())
                    .append("y", layout.viewport().y())
                    .append("zoom", layout.viewport().zoom()));
        }
        return document;
    }

    static PipelineLayout toLayout(Document document) {
        String pipelineId = requireString(document.get("_id"), String.valueOf(document.get("_id")));
        Object rawNodes = document.get("nodes");
        if (!(rawNodes instanceof List<?> nodeDocuments)) {
            throw corrupt(pipelineId);
        }
        Map<String, PipelineLayout.NodePosition> nodes = new LinkedHashMap<>();
        for (Object rawNode : nodeDocuments) {
            if (!(rawNode instanceof Document node)) {
                throw corrupt(pipelineId);
            }
            String id = requireString(node.get("id"), pipelineId);
            if (nodes.put(id, new PipelineLayout.NodePosition(requireDouble(node.get("x"), pipelineId),
                    requireDouble(node.get("y"), pipelineId))) != null) {
                throw corrupt(pipelineId);
            }
        }
        return new PipelineLayout(pipelineId, nodes, viewport(document, pipelineId));
    }

    private static PipelineLayout.Viewport viewport(Document document, String pipelineId) {
        Object raw = document.get("viewport");
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof Document viewport)) {
            throw corrupt(pipelineId);
        }
        double zoom = requireDouble(viewport.get("zoom"), pipelineId);
        if (zoom <= 0) {
            throw corrupt(pipelineId);
        }
        return new PipelineLayout.Viewport(
                requireDouble(viewport.get("x"), pipelineId),
                requireDouble(viewport.get("y"), pipelineId), zoom);
    }

    private static String requireString(Object value, String pipelineId) {
        if (!(value instanceof String string) || string.isBlank()) {
            throw corrupt(pipelineId);
        }
        return string;
    }

    private static double requireDouble(Object value, String pipelineId) {
        if (!(value instanceof Number number)) {
            throw corrupt(pipelineId);
        }
        double numberValue = number.doubleValue();
        if (!Double.isFinite(numberValue)) {
            throw corrupt(pipelineId);
        }
        return numberValue;
    }

    private static TapstateException corrupt(String pipelineId) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", pipelineId), null);
    }
}
