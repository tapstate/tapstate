package io.tapstate.app;

import io.tapstate.adapters.mongostore.SourceConfigCipher;
import io.tapstate.control.core.ConfigurationExportService;
import io.tapstate.control.core.ConfigurationImportService;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ConfigurationExportStore;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.ConfigurationImportStore.Payload;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.User;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Versioned transport envelopes reuse the authenticated Source cipher with a caller-held independent key. */
final class EncryptedConfigurationTransfer implements ConfigurationImportService.TransferCodec, ConfigurationExportService.Encoder {
    private static final Set<String> FIELDS = Set.of("version", "kind", "mode", "defaultSourceId", "resources", "users", "layouts", "referenceHashes", "defaultSourceMapping");
    private static final CanonicalWriter WRITER = new CanonicalWriter();
    private static final DslParser PARSER = new DslParser();
    private static final String EXPORT = "export1", PREPARED = "prepared1";

    @Override public String sealExport(ConfigurationExportStore.Snapshot snapshot, String key) {
        if (snapshot.mode() == StorageBinding.Mode.CLOUD && !snapshot.payload().users().isEmpty()) throw refused();
        return encrypt(tree(snapshot.payload(), EXPORT, snapshot.mode().name(), snapshot.defaultSourceId()), EXPORT, key);
    }
    @Override public String seal(Payload payload, String key) {
        return encrypt(tree(payload, PREPARED, payload.users().isEmpty() ? null : "OP", null), PREPARED, key);
    }
    @Override public Payload open(String encoded, String key, Map<String,String> confirmedMapping) {
        try {
            if (encoded == null || encoded.length() > 8 * 1024 * 1024) throw refused();
            int separator = encoded.indexOf(':'); if (separator < 1) throw refused();
            String kind = encoded.substring(0, separator);
            if (!Set.of(EXPORT, PREPARED).contains(kind)) throw refused();
            String clear = cipher(key).decrypt("configuration-transfer", kind, encoded.substring(separator + 1));
            if (bytes(clear) > ConfigurationImportStore.MAX_PAYLOAD_BYTES) throw refused();
            var document = object(JsonReader.parse(clear));
            if (!document.keySet().equals(FIELDS) || !Long.valueOf(1).equals(document.get("version")) || !kind.equals(document.get("kind"))) throw refused();
            String mode = nullableText(document.get("mode"));
            var users = users(document.get("users"));
            if (!users.isEmpty() && !"OP".equals(mode) || mode != null && !Set.of("OP", "CLOUD").contains(mode)) throw refused();
            String originalDefault = nullableText(document.get("defaultSourceId"));
            var references = strings(document.get("referenceHashes"));
            var mapping = strings(document.get("defaultSourceMapping"));
            var resourceTrees = list(document.get("resources"));
            if (kind.equals(EXPORT)) {
                if (!references.isEmpty() || !mapping.isEmpty() || mode == null) throw refused();
                if (!confirmedMapping.keySet().equals(originalDefault == null ? Set.of() : Set.of(originalDefault))) throw refused();
                mapping = Map.copyOf(confirmedMapping);
            } else if (originalDefault != null || !mapping.equals(confirmedMapping)) throw refused();
            var resources = new ArrayList<Resource>();
            for (Object item : resourceTrees) {
                var resource = object(item); objectBudget(resource);
                if (kind.equals(EXPORT) && originalDefault != null) {
                    if (originalDefault.equals(resource.get("id"))) throw refused();
                    remapTargets(resource, originalDefault, text(mapping.get(originalDefault)));
                }
                resources.add(PARSER.fromTree(resource));
            }
            var result = new Payload(resources, users, layouts(document.get("layouts")), references, mapping);
            // Re-encoding checks every object and the complete canonical payload, including users and layouts.
            tree(result, kind, mode, originalDefault);
            return result;
        } catch (TapstateException | IllegalArgumentException invalid) { throw refused(); }
    }

    private static Map<String,Object> tree(Payload payload, String kind, String mode, String defaultId) {
        int count = payload.resources().size() + payload.users().size() + payload.layouts().size();
        if (count < 1 || count > ConfigurationImportStore.MAX_OBJECTS) throw refused();
        var ids = new HashSet<String>(); var pipelines = new HashSet<String>();
        var resources = new ArrayList<Object>();
        for (var resource : payload.resources().stream().sorted(Comparator.comparing(Resource::id)).toList()) {
            if (!ids.add(resource.id())) throw refused();
            if (resource instanceof PipelineResource) pipelines.add(resource.id());
            if (resource instanceof io.tapstate.core.model.SourceResource source && Set.of("mongodb", "mongodb-atlas").contains(source.connector())
                    && source.config().containsKey("tlsCaPem")) {
                Object ca = source.config().get("tlsCaPem");
                if (!(ca instanceof String text) || bytes(text) > 64 * 1024) throw refused();
            }
            var value = WRITER.tree(resource); objectBudget(value); resources.add(value);
        }
        var usernames = new HashSet<String>(); var users = new ArrayList<Object>();
        for (var user : payload.users().stream().sorted(Comparator.comparing(User::username)).toList()) {
            if (!usernames.add(user.username()) || !validUser(user)) throw refused();
            var value = Map.of("username", user.username(), "passwordHash", user.passwordHash(), "role", user.role());
            objectBudget(value); users.add(value);
        }
        var layoutIds = new HashSet<String>(); var layouts = new ArrayList<Object>();
        for (var layout : payload.layouts().stream().sorted(Comparator.comparing(PipelineLayout::pipelineId)).toList()) {
            if (!pipelines.contains(layout.pipelineId()) || !layoutIds.add(layout.pipelineId())) throw refused();
            var nodes = new TreeMap<String,Object>(); layout.nodes().forEach((id, position) -> nodes.put(id, List.of(position.x(), position.y())));
            var value = new LinkedHashMap<String,Object>(); value.put("pipelineId", layout.pipelineId()); value.put("nodes", nodes);
            value.put("viewport", layout.viewport() == null ? null : List.of(layout.viewport().x(), layout.viewport().y(), layout.viewport().zoom()));
            objectBudget(value); layouts.add(value);
        }
        if (payload.referenceHashes().entrySet().stream().anyMatch(e -> e.getKey().isBlank() || !e.getValue().matches("[0-9a-f]{64}"))
                || payload.defaultSourceMapping().size() > 1 || payload.defaultSourceMapping().entrySet().stream().anyMatch(e -> e.getKey().isBlank() || e.getValue().isBlank())) throw refused();
        var result = new LinkedHashMap<String,Object>(); result.put("version", 1); result.put("kind", kind); result.put("mode", mode);
        result.put("defaultSourceId", defaultId); result.put("resources", resources); result.put("users", users); result.put("layouts", layouts);
        result.put("referenceHashes", new TreeMap<>(payload.referenceHashes())); result.put("defaultSourceMapping", new TreeMap<>(payload.defaultSourceMapping()));
        if (bytes(JsonWriter.write(result)) > ConfigurationImportStore.MAX_PAYLOAD_BYTES) throw refused();
        return result;
    }
    private static String encrypt(Map<String,Object> tree, String kind, String key) {
        return kind + ":" + cipher(key).encrypt("configuration-transfer", kind, JsonWriter.write(tree));
    }
    private static SourceConfigCipher cipher(String key) {
        byte[] decoded = null;
        try {
            if (key == null || !key.matches("[A-Za-z0-9_-]{43}")) throw refused();
            decoded = Base64.getUrlDecoder().decode(key);
            if (decoded.length != 32 || !Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(key)) throw refused();
            return new SourceConfigCipher(decoded);
        } catch (IllegalArgumentException invalid) { throw refused(); }
        finally { if (decoded != null) Arrays.fill(decoded, (byte)0); }
    }
    /** Only typed sink Source references are rewritten; arbitrary config/SQL/labels keep their original content. */
    private static void remapTargets(Map<String,Object> resource, String from, String to) {
        if ("pipeline".equals(resource.get("kind"))) {
            Object raw = resource.get("source");
            var sources = raw instanceof List<?> values ? values : raw == null ? List.of() : List.of(raw);
            for (Object source : sources) if (from.equals(source instanceof Map<?,?> map ? map.get("id") : source)) throw refused();
            Object serve = resource.get("serve");
            if (serve instanceof Map<?,?>) remapSinks(object(serve), from, to);
        } else if ("serve".equals(resource.get("kind"))) remapSinks(resource, from, to);
    }
    private static void remapSinks(Map<String,Object> serve, String from, String to) {
        for (String field : List.of("sync", "push")) {
            Object raw = serve.get(field); if (raw == null) continue;
            for (Object item : raw instanceof List<?> values ? values : List.of(raw)) {
                var sink = object(item); if (from.equals(sink.get("source"))) sink.put("source", to);
            }
        }
    }
    private static List<User> users(Object value) {
        var result = new ArrayList<User>();
        for (Object item : list(value)) {
            var row = object(item); if (!row.keySet().equals(Set.of("username", "passwordHash", "role"))) throw refused();
            result.add(new User(text(row.get("username")), text(row.get("passwordHash")), text(row.get("role"))));
        }
        return result;
    }
    private static boolean validUser(User user) {
        return Set.of("admin", "write", "read").contains(user.role())
                && user.passwordHash().matches("\\$2[aby]\\$(0[4-9]|[12][0-9]|3[01])\\$[./A-Za-z0-9]{53}");
    }
    private static List<PipelineLayout> layouts(Object value) {
        var result = new ArrayList<PipelineLayout>();
        for (Object item : list(value)) {
            var row = object(item); if (!row.keySet().equals(Set.of("pipelineId", "nodes", "viewport"))) throw refused();
            var nodes = new TreeMap<String,PipelineLayout.NodePosition>();
            object(row.get("nodes")).forEach((id, position) -> {
                var xy = list(position); if (xy.size() != 2) throw refused();
                nodes.put(id, new PipelineLayout.NodePosition(number(xy.get(0)), number(xy.get(1))));
            });
            PipelineLayout.Viewport viewport = null;
            if (row.get("viewport") != null) {
                var xyz = list(row.get("viewport")); if (xyz.size() != 3) throw refused();
                viewport = new PipelineLayout.Viewport(number(xyz.get(0)), number(xyz.get(1)), number(xyz.get(2)));
            }
            result.add(new PipelineLayout(text(row.get("pipelineId")), nodes, viewport));
        }
        return result;
    }
    @SuppressWarnings("unchecked") private static Map<String,Object> object(Object value) {
        if (!(value instanceof Map<?,?> map) || map.keySet().stream().anyMatch(k -> !(k instanceof String))) throw refused();
        return (Map<String,Object>) map;
    }
    private static List<?> list(Object value) {
        if (!(value instanceof List<?> values)) throw refused();
        return values;
    }
    private static Map<String,String> strings(Object value) {
        var result = new TreeMap<String,String>(); object(value).forEach((k,v) -> result.put(text(k), text(v))); return result;
    }
    private static String text(Object value) { if (!(value instanceof String s) || s.isBlank()) throw refused(); return s; }
    private static String nullableText(Object value) { return value == null ? null : text(value); }
    private static double number(Object value) { if (!(value instanceof Number n)) throw refused(); return n.doubleValue(); }
    private static int bytes(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }
    private static void objectBudget(Object value) { if (bytes(JsonWriter.write(value)) > ConfigurationImportStore.MAX_OBJECT_BYTES) throw refused(); }
    private static TapstateException refused() { return new TapstateException(IoError.CONFIGURATION_IMPORT_REFUSED, Map.of("reason", "invalid-transfer"), null); }
}
