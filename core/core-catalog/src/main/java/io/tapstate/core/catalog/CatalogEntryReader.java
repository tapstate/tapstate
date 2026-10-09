package io.tapstate.core.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.WriteMode;

/**
 * Reads one catalog entry from its bundled JSON form into a {@link ConnectorCatalogEntry}. This is
 * the consumer dual of the build tool's entry writer; the two agree on the product format. Uses the
 * dependency-free {@link CatalogJson} reader so the core ring ships no JSON library.
 *
 * <p>The product format (deterministic key order):
 * {@code {id, name, displayName, icon, group, modes[], discovery, sink{capable,writeSemantics[]},
 * pushOut, config[{name,type,label{},required,default,secret,options[{value,label{}}],visibleWhen}],
 * provenance{specPath,specContentHash,pdkApiVersion,requiredLevel,modeSource{}}}}. The two
 * catalog-wide revisions are not in an entry - they live in the index head, read by TapstateCatalog.
 * Enum-valued strings use each enum's {@code yaml()} code.
 */
public final class CatalogEntryReader {

    private CatalogEntryReader() {
    }

    public static ConnectorCatalogEntry read(String json) {
        return fromTree(asMap(CatalogJson.parse(json)));
    }

    public static ConnectorCatalogEntry fromTree(Map<String, Object> m) {
        String connectorId = requireString(m, "id");
        return new ConnectorCatalogEntry(
                connectorId,
                str(m.get("name")),
                str(m.get("displayName")),
                str(m.get("icon")),
                groupOf(str(m.get("group"))),
                modesOf(m.get("modes")),
                discoveryOf(str(m.get("discovery"))),
                // Stored capabilities cannot restore a role withdrawn by the running release.
                SinkRules.applySupportPolicy(connectorId, sinkOf(asMap(m.get("sink")))),
                bool(m, "pushOut"),
                configOf(m.get("config")),
                provenanceOf(asMap(m.get("provenance"))));
    }

    private static List<SourceMode> modesOf(Object raw) {
        List<SourceMode> modes = new ArrayList<>();
        for (Object item : asList(raw)) {
            modes.add(sourceModeOf(String.valueOf(item)));
        }
        return modes;
    }

    private static SinkCapability sinkOf(Map<String, Object> m) {
        List<WriteMode> semantics = new ArrayList<>();
        for (Object item : asList(m.get("writeSemantics"))) {
            semantics.add(writeModeOf(String.valueOf(item)));
        }
        return new SinkCapability(bool(m, "capable"), semantics);
    }

    private static List<ConfigField> configOf(Object raw) {
        List<ConfigField> fields = new ArrayList<>();
        for (Object item : asList(raw)) {
            Map<String, Object> f = asMap(item);
            fields.add(new ConfigField(
                    str(f.get("name")),
                    configTypeOf(str(f.get("type"))),
                    stringMap(f.get("label")),
                    bool(f, "required"),
                    str(f.get("default")),
                    bool(f, "secret"),
                    optionsOf(f.get("options")),
                    visibleWhenOf(f.get("visibleWhen"))));
        }
        return fields;
    }

    private static List<EnumOption> optionsOf(Object raw) {
        List<EnumOption> options = new ArrayList<>();
        for (Object item : asList(raw)) {
            Map<String, Object> o = asMap(item);
            options.add(new EnumOption(str(o.get("value")), stringMap(o.get("label"))));
        }
        return options;
    }

    private static VisibleWhen visibleWhenOf(Object raw) {
        if (!(raw instanceof Map<?, ?>)) {
            return null;
        }
        Map<String, Object> m = asMap(raw);
        List<String> values = new ArrayList<>();
        for (Object item : asList(m.get("equalsAnyOf"))) {
            values.add(String.valueOf(item));
        }
        return new VisibleWhen(str(m.get("controllingField")), values);
    }

    /**
     * Both catalog-wide revisions come back null: an entry document does not carry them, so reading
     * one on its own - which is exactly what a server-registered row is - has none to report.
     * {@link TapstateCatalog} fills them in from the index head for the bundled path.
     */
    private static Provenance provenanceOf(Map<String, Object> m) {
        Map<SourceMode, ModeSource> modeSource = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : asMap(m.get("modeSource")).entrySet()) {
            modeSource.put(sourceModeOf(entry.getKey()), modeSourceOf(String.valueOf(entry.getValue())));
        }
        return new Provenance(
                null,
                null,
                str(m.get("specPath")),
                str(m.get("specContentHash")),
                str(m.get("pdkApiVersion")),
                str(m.get("requiredLevel")),
                modeSource);
    }

    // ---- enum reverse lookups by yaml() code ----

    private static SourceMode sourceModeOf(String yaml) {
        for (SourceMode mode : SourceMode.values()) {
            if (mode.yaml().equals(yaml)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown source mode in catalog: " + yaml);
    }

    private static WriteMode writeModeOf(String yaml) {
        for (WriteMode mode : WriteMode.values()) {
            if (mode.yaml().equals(yaml)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("unknown write mode in catalog: " + yaml);
    }

    private static ConnectorGroup groupOf(String yaml) {
        for (ConnectorGroup group : ConnectorGroup.values()) {
            if (group.yaml().equals(yaml)) {
                return group;
            }
        }
        throw new IllegalArgumentException("unknown connector group in catalog: " + yaml);
    }

    private static Discovery discoveryOf(String yaml) {
        for (Discovery discovery : Discovery.values()) {
            if (discovery.yaml().equals(yaml)) {
                return discovery;
            }
        }
        throw new IllegalArgumentException("unknown discovery value in catalog: " + yaml);
    }

    private static ConfigType configTypeOf(String yaml) {
        for (ConfigType type : ConfigType.values()) {
            if (type.yaml().equals(yaml)) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown config type in catalog: " + yaml);
    }

    private static ModeSource modeSourceOf(String yaml) {
        for (ModeSource source : ModeSource.values()) {
            if (source.yaml().equals(yaml)) {
                return source;
            }
        }
        throw new IllegalArgumentException("unknown mode source in catalog: " + yaml);
    }

    // ---- tree accessors ----

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object value) {
        return value instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    private static List<?> asList(Object value) {
        return value instanceof List<?> l ? l : List.of();
    }

    private static String str(Object value) {
        return value instanceof String s ? s : null;
    }

    /** A required catalog string; throws if absent/blank/non-string, mirroring the producer's invariant. */
    private static String requireString(Map<String, Object> m, String key) {
        String value = str(m.get(key));
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("catalog entry missing required string '" + key + "'");
        }
        return value;
    }

    /** A required catalog boolean; throws if absent or not a JSON boolean (no silent coercion to false). */
    private static boolean bool(Map<String, Object> m, String key) {
        Object value = m.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalArgumentException(
                "catalog entry field '" + key + "' must be a boolean, got: " + value);
    }

    private static Map<String, String> stringMap(Object value) {
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : asMap(value).entrySet()) {
            result.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return result;
    }
}
