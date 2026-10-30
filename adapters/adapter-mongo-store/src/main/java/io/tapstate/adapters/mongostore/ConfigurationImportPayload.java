package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.ConfigurationImportStore.Payload;
import io.tapstate.spi.store.ConfigurationImportStore.Receipt;
import io.tapstate.spi.store.ConfigurationImportStore.Ticket;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.User;
import org.bson.Document;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Captures caller-owned nested values before guards, transactions or their retries can mutate them. */
final class ConfigurationImportPayload {
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();
    final Payload payload;
    final String inputHash;
    final String mappingHash;
    final List<ArtifactWrite> writes;
    final Map<String,String> resourceHashes;

    ConfigurationImportPayload(Payload requested) {
        int count = requested.resources().size()+requested.users().size()+requested.layouts().size();
        if (count < 1) throw refused("empty-payload");
        if (count > ConfigurationImportStore.MAX_OBJECTS) throw limit("objects");
        var resources = new ArrayList<Resource>(); var resourceTrees = new ArrayList<Object>();
        var ids = new HashSet<String>(); var pipelines = new HashSet<String>(); var hashes = new TreeMap<String,String>();
        for (var resource : requested.resources().stream().sorted(java.util.Comparator.comparing(Resource::id)).toList()) {
            if (!ids.add(resource.id())) throw refused("duplicate-resource");
            Map<String,Object> tree;
            Resource frozen;
            try {
                String json = JsonWriter.write(WRITER.tree(resource)); objectBudget(json);
                @SuppressWarnings("unchecked") var detached = (Map<String,Object>) JsonReader.parse(json);
                tree = detached; frozen = PARSER.fromTree(tree);
            } catch (DslException | IllegalArgumentException invalid) { throw refused("invalid-resource"); }
            resources.add(frozen); resourceTrees.add(tree); hashes.put(frozen.id(),CanonicalHash.of(frozen));
            if (frozen instanceof PipelineResource) pipelines.add(frozen.id());
        }
        var users = requested.users().stream().sorted(java.util.Comparator.comparing(User::username)).toList();
        var usernames = new HashSet<String>(); var userTrees = new ArrayList<Object>();
        for (var user : users) {
            if (!usernames.add(user.username())) throw refused("duplicate-user");
            var tree = new TreeMap<String,Object>(); tree.put("username",user.username()); tree.put("passwordHash",user.passwordHash()); tree.put("role",user.role());
            objectBudget(JsonWriter.write(tree)); userTrees.add(tree);
        }
        var layouts = requested.layouts().stream().sorted(java.util.Comparator.comparing(PipelineLayout::pipelineId)).toList();
        var layoutIds = new HashSet<String>(); var layoutTrees = new ArrayList<Object>();
        for (var layout : layouts) {
            if (!layoutIds.add(layout.pipelineId()) || !pipelines.contains(layout.pipelineId())) throw refused("invalid-layout-owner");
            var nodes = new TreeMap<String,Object>(); layout.nodes().forEach((id,position) -> nodes.put(id,List.of(position.x(),position.y())));
            var tree = new TreeMap<String,Object>(); tree.put("pipelineId",layout.pipelineId()); tree.put("nodes",nodes);
            tree.put("viewport",layout.viewport() == null ? null : List.of(layout.viewport().x(),layout.viewport().y(),layout.viewport().zoom()));
            objectBudget(JsonWriter.write(tree)); layoutTrees.add(tree);
        }
        var references = new TreeMap<>(requested.referenceHashes());
        if (references.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || !hash(entry.getValue()))) throw refused("invalid-reference");
        var mapping = new TreeMap<>(requested.defaultSourceMapping());
        if (mapping.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || entry.getValue().isBlank())) throw refused("invalid-mapping");
        var complete = new LinkedHashMap<String,Object>(); complete.put("resources",resourceTrees); complete.put("users",userTrees);
        complete.put("layouts",layoutTrees); complete.put("references",references); complete.put("defaultSourceMapping",mapping);
        String json = JsonWriter.write(complete);
        if (bytes(json) > ConfigurationImportStore.MAX_PAYLOAD_BYTES) throw limit("payload-bytes");
        this.payload = new Payload(resources,users,layouts,references,mapping);
        this.inputHash = CanonicalHash.ofText(json); this.mappingHash = CanonicalHash.ofText(JsonWriter.write(mapping));
        this.resourceHashes = java.util.Collections.unmodifiableMap(hashes);
        this.writes = resources.stream().map(resource -> ArtifactWrite.createOnly(resource).guardedBy(references)).toList();
    }
    Receipt receipt(Ticket ticket) {
        var receipt = new Receipt(ticket.id(),inputHash,ticket.destinationRevision()+1,resourceHashes,
                new TreeSet<>(payload.users().stream().map(User::username).toList()),new TreeSet<>(payload.layouts().stream().map(PipelineLayout::pipelineId).toList()));
        if (bytes(JsonWriter.write(receiptTree(receipt))) > ConfigurationImportStore.MAX_RECEIPT_BYTES) throw limit("receipt-bytes");
        return receipt;
    }
    static Document receiptTree(Receipt receipt) {
        return new Document("id",receipt.id()).append("inputHash",receipt.inputHash()).append("configurationRevision",receipt.configurationRevision())
                .append("resourceHashes",new Document(receipt.resourceHashes())).append("usernames",List.copyOf(receipt.usernames())).append("layoutIds",List.copyOf(receipt.layoutIds()));
    }
    private static int bytes(String text) { return text.getBytes(StandardCharsets.UTF_8).length; }
    private static void objectBudget(String text) { if (bytes(text) > ConfigurationImportStore.MAX_OBJECT_BYTES) throw limit("object-bytes"); }
    private static boolean hash(String text) { return text != null && text.matches("[0-9a-f]{64}"); }
    static TapstateException refused(String reason) { return new TapstateException(IoError.CONFIGURATION_IMPORT_REFUSED,Map.of("reason",reason),null); }
    static TapstateException limit(String limit) { return new TapstateException(IoError.CONFIGURATION_IMPORT_LIMIT,Map.of("limit",limit),null); }
}
