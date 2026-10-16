package io.tapstate.control.core;

import io.tapstate.core.catalog.ConfigField;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.MongoUriUserInfo;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SrsSchemaEvolution;
import io.tapstate.core.model.TableRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Maps between the frontend Source projection and the canonical core Source model. */
public final class SourceRepresentation {

    private final Supplier<TapstateCatalog> catalog;

    public SourceRepresentation(TapstateCatalog catalog) {
        this(() -> Objects.requireNonNull(catalog, "catalog"));
    }

    public SourceRepresentation(Supplier<TapstateCatalog> catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /** Builds a core Source while applying catalog and shared transport secret replacement semantics. */
    public SourceResource toModel(SourceDraft draft, SourceResource existing) {
        return toModel(draft, existing, true);
    }

    /** Builds a persistent Source from structured HTTP input and catalog and shared transport secret rules. */
    public SourceResource toModel(SourceInput input, SourceResource existing) {
        Objects.requireNonNull(input, "input");
        return toModel(input.asDraft(), existing, true);
    }

    /** Builds a draft Source while applying only the draft's explicit secret removals. */
    SourceResource toDraftModel(SourceDraft draft) {
        return toModel(draft, null, false);
    }

    private SourceResource toModel(
            SourceDraft draft, SourceResource existing, boolean enforceSecretRules) {
        Objects.requireNonNull(draft, "draft");
        ConnectorCatalogEntry connector = connector(draft.connector());
        Map<String, SecretField> secrets = secretFields(connector);
        if (enforceSecretRules) {
            validateSuppliedSecrets(draft, secrets);
        }
        List<String> clearSecrets = enforceSecretRules
                ? validateClearSecrets(draft, secrets)
                : draft.clearSecrets();

        Map<String, Object> config = new LinkedHashMap<>(draft.config());
        if (existing != null && existing.connector().equals(draft.connector())) {
            for (SecretField secret : secrets.values()) {
                if (!secret.contains(config) && secret.value(existing.config()) != null) {
                    secret.put(config, SourceDraft.copyJsonValue(secret.value(existing.config())));
                }
            }
        }
        if (isMongoConnector(draft.connector()) && config.get("uri") instanceof String uri
                && MongoUriUserInfo.isRedactedDisplay(uri)) {
            Object saved = existing != null && existing.connector().equals(draft.connector())
                    ? existing.config().get("uri") : null;
            if (!(saved instanceof String savedUri) || !uri.equals(MongoUriUserInfo.redact(savedUri))) {
                throw malformed("a redacted URI cannot replace connection settings; provide a complete URI");
            }
            config.put("uri", savedUri);
        }
        for (String secret : clearSecrets) {
            SecretField field = secrets.get(secret);
            if (field == null) {
                config.remove(secret);
            } else {
                field.remove(config);
            }
        }

        // Options are the engine's own configuration and its vocabulary is empty today, so the model
        // has nowhere to put one. Refusing here rather than dropping it silently: a draft that keeps
        // its options and loses them on the way in would read as accepted and configure nothing.
        require(draft.options() == null || draft.options().isEmpty(),
                "options carry no engine option today; remove the field");

        return new SourceResource(
                draft.id(),
                draft.metadata(),
                draft.connector(),
                SourceDraft.copyJsonMap(config, false),
                sourceMode(draft.mode()),
                tableModels(draft.tables()),
                srsModel(draft.srs()),
                PipelineRepresentation.execution(draft.execution(), "execution"),
                SourceDraft.copyJsonMap(draft.experimental(), true));
    }

    /** Builds a normalized response with every catalog and shared transport secret removed. */
    public SourceView toView(SourceResource source, String contentHash) {
        Objects.requireNonNull(source, "source");
        Map<String, SecretField> secrets = secretFields(connector(source.connector()));
        Map<String, Object> redactedConfig = new LinkedHashMap<>(source.config());
        secrets.values().forEach(secret -> secret.remove(redactedConfig));
        Object uri = redactedConfig.get("uri");
        if (uri instanceof String value) {
            redactedConfig.put("uri", MongoUriUserInfo.redact(value));
        } else if (isMongoConnector(source.connector()) && uri != null) {
            redactedConfig.put("uri", MongoUriUserInfo.REDACTED);
        }
        List<String> configuredSecrets = secrets.keySet().stream()
                .filter(name -> secrets.get(name).value(source.config()) != null)
                .sorted()
                .toList();

        return new SourceView(
                source.id(),
                source.metadata(),
                source.connector(),
                redactedConfig,
                configuredSecrets,
                source.mode() == null ? null : source.mode().yaml(),
                tableViews(source.tables()),
                // Always empty rather than absent: the wire contract keeps the field, and the model
                // has no engine option to put in it. Dropping the key from responses would be an API
                // change this slice has no business making.
                Map.of(),
                srsView(source.srs()),
                PipelineRepresentation.executionValue(source.execution()),
                source.experimental(),
                contentHash);
    }

    private ConnectorCatalogEntry connector(String connector) {
        try {
            return catalog.get().byId(connector);
        } catch (IllegalArgumentException error) {
            throw malformed("unknown connector: " + connector);
        }
    }

    private static boolean isMongoConnector(String connector) {
        return "mongodb".equals(connector) || "mongodb-atlas".equals(connector);
    }

    private static Map<String, SecretField> secretFields(ConnectorCatalogEntry connector) {
        Map<String, SecretField> result = new LinkedHashMap<>();
        connector.config().stream()
                .filter(ConfigField::secret)
                .forEach(field -> result.put(field.name(), new SecretField(null, field.name(), field.required())));
        result.putIfAbsent("sslKey", new SecretField(null, "sslKey", false));
        result.putIfAbsent("sslKeyPassword", new SecretField(null, "sslKeyPassword", false));
        result.putIfAbsent("__TAPDATA.sshPassword", new SecretField("__TAPDATA", "sshPassword", false));
        return result;
    }

    /** Catalog names remain literal; only explicitly registered shared fields have a nested path. */
    private record SecretField(String group, String name, boolean required) {
        private Map<?, ?> container(Map<String, Object> config) {
            if (group == null) {
                return config;
            }
            return config.get(group) instanceof Map<?, ?> nested ? nested : Map.of();
        }

        boolean contains(Map<String, Object> config) {
            return container(config).containsKey(name);
        }

        Object value(Map<String, Object> config) {
            return container(config).get(name);
        }

        void put(Map<String, Object> config, Object value) {
            if (group == null) {
                config.put(name, value);
            } else {
                Map<String, Object> nested = mutableGroup(config);
                nested.put(name, value);
                config.put(group, nested);
            }
        }

        void remove(Map<String, Object> config) {
            if (group == null) {
                config.remove(name);
            } else if (contains(config)) {
                Map<String, Object> nested = mutableGroup(config);
                nested.remove(name);
                config.put(group, nested);
            }
        }

        private Map<String, Object> mutableGroup(Map<String, Object> config) {
            Object existing = config.get(group);
            if (existing != null && !(existing instanceof Map<?, ?>)) {
                throw malformed("config." + group + " must be an object");
            }
            Map<String, Object> nested = new LinkedHashMap<>();
            container(config).forEach((key, value) -> nested.put((String) key, value));
            return nested;
        }
    }

    private static List<String> validateClearSecrets(
            SourceDraft draft, Map<String, SecretField> secrets) {
        Set<String> seen = new LinkedHashSet<>();
        for (String field : draft.clearSecrets()) {
            if (field == null || !secrets.containsKey(field)) {
                throw malformed("clearSecrets contains an unknown secret field");
            }
            if (!seen.add(field)) {
                throw malformed("clearSecrets contains a duplicate field: " + field);
            }
            if (secrets.get(field).contains(draft.config())) {
                throw malformed("a secret cannot be supplied and cleared in the same request: " + field);
            }
            if (secrets.get(field).required()) {
                throw malformed("a required secret cannot be cleared: " + field);
            }
        }
        return List.copyOf(seen);
    }

    private static void validateSuppliedSecrets(
            SourceDraft draft, Map<String, SecretField> secrets) {
        for (String secret : secrets.keySet()) {
            if (secrets.get(secret).contains(draft.config()) && secrets.get(secret).value(draft.config()) == null) {
                throw malformed("a supplied secret cannot be null: " + secret);
            }
        }
    }

    private static SourceMode sourceMode(String value) {
        if (value == null) {
            return null;
        }
        for (SourceMode mode : SourceMode.values()) {
            if (mode.yaml().equals(value)) {
                return mode;
            }
        }
        throw malformed("unknown Source mode: " + value);
    }

    private static List<TableRef> tableModels(List<SourceTableDraft> tables) {
        if (tables == null) {
            return null;
        }
        List<TableRef> result = new ArrayList<>(tables.size());
        for (SourceTableDraft table : tables) {
            if (table == null) {
                throw malformed("tables cannot contain null entries");
            }
            result.add(tableModel(table));
        }
        return List.copyOf(result);
    }

    private static TableRef tableModel(SourceTableDraft table) {
        return switch (Objects.toString(table.type(), "")) {
            case "literal" -> {
                require(table.name() != null
                                && table.pattern() == null
                                && table.filter() == null
                                && table.pk() == null
                                && table.options() == null,
                        "literal tables accept only name");
                yield TableRef.literal(table.name());
            }
            case "regex" -> {
                require(table.pattern() != null
                                && table.name() == null
                                && table.filter() == null
                                && table.pk() == null
                                && table.options() == null,
                        "regex tables accept only pattern");
                yield TableRef.regex(table.pattern());
            }
            case "spec" -> {
                require(table.name() != null && table.pattern() == null,
                        "spec tables require name and cannot carry pattern");
                require(table.pk() == null || table.pk().stream().noneMatch(Objects::isNull),
                        "spec table pk cannot contain null entries");
                require(table.options() == null || table.options().isEmpty(),
                        "options carry no engine option today; remove the field");
                yield TableRef.spec(table.name(), table.filter(), table.pk());
            }
            default -> throw malformed("unknown table type: " + table.type());
        };
    }

    private static List<SourceTableView> tableViews(List<TableRef> tables) {
        if (tables == null) {
            return null;
        }
        List<SourceTableView> result = new ArrayList<>(tables.size());
        for (TableRef table : tables) {
            result.add(switch (table) {
                case TableRef.Literal literal -> new SourceTableView(
                        "literal", literal.name(), null, null, null, null);
                case TableRef.Regex regex -> new SourceTableView(
                        "regex", null, regex.pattern(), null, null, null);
                case TableRef.Spec spec -> new SourceTableView(
                        "spec", spec.name(), null, spec.filter(), spec.pk(), Map.of());
            });
        }
        return List.copyOf(result);
    }

    private static Srs srsModel(SourceDraft.SourceSrs value) {
        if (value == null) {
            return null;
        }
        return new Srs(
                value.key(),
                value.retention(),
                schemaEvolution(value.schemaEvolution()),
                value.queryable(),
                value.enabled());
    }

    private static SourceDraft.SourceSrs srsView(Srs srs) {
        if (srs == null) {
            return null;
        }
        return new SourceDraft.SourceSrs(
                srs.key(),
                srs.retention(),
                srs.schemaEvolution() == null ? null : srs.schemaEvolution().yaml(),
                srs.queryable(),
                srs.enabled());
    }

    private static SrsSchemaEvolution schemaEvolution(Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String spelling)) {
            throw malformed("srs.schemaEvolution must be a string");
        }
        for (SrsSchemaEvolution candidate : SrsSchemaEvolution.values()) {
            if (candidate.yaml().equals(spelling)) {
                return candidate;
            }
        }
        throw malformed("unknown srs.schemaEvolution: " + spelling);
    }

    private static void require(boolean condition, String reason) {
        if (!condition) {
            throw malformed(reason);
        }
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(
                ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }

}
