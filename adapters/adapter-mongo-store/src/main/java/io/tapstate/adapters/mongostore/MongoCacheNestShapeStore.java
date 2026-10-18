package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.NestShapeStore;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import com.mongodb.client.ClientSession;
import io.tapstate.spi.store.OperatorPlacement;

/** The first actual shape wins within one stable physical state generation. */
final class MongoCacheNestShapeStore implements NestShapeStore {
    private final MongoCachePipelineAccess access;

    MongoCacheNestShapeStore(MongoCachePipelineAccess access, String database) {
        this.access = Objects.requireNonNull(access, "access");
        if (!access.database.getName().equals(database)) throw MongoCachePipelineAccess.fenced();
    }

    @Override public Optional<Set<String>> read(String stepId) {
        Document id = id(stepId);
        return access.metadataTransaction(SystemCollections.CACHE_NEST_SHAPES, false, session -> {
            Document inventory = MongoOperatorInventory.require(session, access.database, access.scope);
            boolean known = MongoOperatorInventory.knowsShape(inventory, access.database.getName(), stepId);
            Document row = SystemCollections.CACHE_NEST_SHAPES.on(access.database).find(session, new Document("_id", id)).first();
            if ((row != null) != known) throw access.unreadable("nest-shape");
            return row == null ? Optional.empty() : Optional.of(decode(row, id));
        });
    }

    @Override public Optional<Set<String>> recordIfAbsent(String stepId, Set<String> paths) {
        Document id = id(stepId); Set<String> captured = capture(paths); List<String> encoded = captured.stream().map(OperatorStateText::encode).toList();
        return access.metadataTransaction(SystemCollections.CACHE_NEST_SHAPES, true, session -> {
            var table = SystemCollections.CACHE_NEST_SHAPES.on(access.database);
            Document existing = table.find(session, new Document("_id", id)).first();
            Document inventory = MongoOperatorInventory.require(session, access.database, access.scope);
            boolean known = MongoOperatorInventory.knowsShape(inventory, access.database.getName(), stepId);
            if (existing != null) {
                if (!known) throw access.unreadable("operator-inventory");
                return Optional.of(decode(existing, id));
            }
            if (known) throw access.unreadable("nest-shape");
            new MongoCacheMetadataStructure(access.database, access.rateRetention).verifyFacet(SystemCollections.CACHE_NEST_STATE);
            var retained = new Document("_id.placement.scope", MongoPipelinePreparationFacts.scope(access.scope))
                    .append("_id.placement.stepId", OperatorStateText.encode(stepId));
            if (SystemCollections.CACHE_NEST_STATE.on(access.database).find(session, retained).projection(new Document("_id", 1)).first() != null)
                throw access.unreadable("nest-shape");
            table.insertOne(session, new Document("_id", id).append("formatVersion", 1).append("paths", encoded));
            MongoOperatorInventory.rememberShape(session, access.database, access.scope, inventory, stepId);
            return Optional.empty();
        });
    }

    void requirePath(ClientSession session, OperatorPlacement placement) {
        new MongoCacheMetadataStructure(access.database, access.rateRetention).verifyFacet(SystemCollections.CACHE_NEST_SHAPES);
        Document id = id(placement.stepId());
        Document row = SystemCollections.CACHE_NEST_SHAPES.on(access.database).find(session, new Document("_id", id)).first();
        Document inventory = MongoOperatorInventory.require(session, access.database, access.scope);
        if (!MongoOperatorInventory.knowsShape(inventory, access.database.getName(), placement.stepId())
                || row == null || !decode(row, id).contains(placement.pathId())) throw access.unreadable("nest-shape");
    }

    private Document id(String stepId) {
        if (stepId == null || stepId.isBlank()) throw invalid();
        return new Document("scope", MongoPipelinePreparationFacts.scope(access.scope)).append("stepId", OperatorStateText.encode(stepId));
    }

    private Set<String> capture(Set<String> paths) {
        if (paths == null || paths.stream().anyMatch(path -> path == null || (path.isBlank() && !path.isEmpty()))) throw invalid();
        return Collections.unmodifiableSet(new TreeSet<>(paths));
    }

    private Set<String> decode(Document row, Document id) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "paths")) || !id.equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("paths") instanceof List<?> encoded)) throw access.unreadable("nest-shape");
            Set<String> paths = new LinkedHashSet<>(); String previous = null;
            for (Object entry : encoded) {
                if (!(entry instanceof String text)) throw access.unreadable("nest-shape");
                String path = OperatorStateText.decode(text);
                if ((path.isBlank() && !path.isEmpty()) || (previous != null && previous.compareTo(path) >= 0) || !paths.add(path)) throw access.unreadable("nest-shape");
                previous = path;
            }
            return Collections.unmodifiableSet(paths);
        } catch (IllegalArgumentException | ClassCastException invalid) { throw access.unreadable("nest-shape"); }
    }

    private TapstateException invalid() { return new TapstateException(IoError.DOCUMENT_INVALID, Map.of("id", access.scope.pipelineId(), "field", "nest-shape"), null); }
}
