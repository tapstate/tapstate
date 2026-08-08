package io.tapstate.core.dsl;

import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestOrder;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import io.tapstate.core.model.SrsSchemaEvolution;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.ViewSchema;
import io.tapstate.core.model.WriteMode;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.representer.Representer;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Parses a single {@code .tap.yml} document into its resource model (plan poc1 B3). The
 * inverse of {@code CanonicalWriter}: parse accepts any legal YAML style, rejects fields
 * outside the tapstate/v1 schema (§11.5) at every nesting level, and normalizes sugar
 * (string use-references, omitted natural-order wiring, anonymous id generation, source
 * scalar/list). The resulting model is the post-normalization form (canonical-form.md §1).
 * It composes the snakeyaml node tree (rather than {@code load()}) so every error can
 * carry a source position.
 */
public final class DslParser {

    private static final Set<String> SOURCE_KEYS = Set.of(
            "version", "kind", "id", "metadata", "connector", "config", "mode",
            "tables", "options", "srs", "experimental");
    private static final Set<String> PIPELINE_KEYS = Set.of(
            "version", "kind", "id", "metadata", "source", "transforms", "view", "serve",
            "settings", "experimental");
    private static final Set<String> METADATA_KEYS = Set.of("labels", "description");
    private static final Set<String> SRS_KEYS = Set.of("key", "retention", "schema_evolution", "queryable", "enabled");
    // snapshot_mode / start_from moved from source options to pipeline settings (read_mode / start_from).
    // options is otherwise a free connector map, so these two names are rejected explicitly instead of
    // being silently passed through to the connector as no-ops when a stale artifact still carries them.
    private static final List<String> RELOCATED_SOURCE_OPTIONS = List.of("snapshot_mode", "start_from");
    private static final Set<String> TABLE_SPEC_KEYS = Set.of("name", "filter", "pk", "options");
    private static final Set<String> STEP_BASE_KEYS = Set.of("id", "type", "from", "options", "experimental");
    private static final Set<String> STEP_USE_KEYS = Set.of("id", "use", "from", "options");
    private static final Set<String> NEST_ROOT_KEYS =
            Set.of("from", "key", "mode", "trackKeyChanges", "embed");
    private static final Set<String> EMBED_KEYS = Set.of(
            "from", "on", "as", "path", "arrayKey", "ignoreUpdates", "trackKeyChanges", "embed");
    private static final Set<String> VIEW_INLINE_KEYS = Set.of("id", "from", "primary_key", "storage", "schema");
    private static final Set<String> VIEW_USE_KEYS = Set.of("id", "use", "from");
    private static final Set<String> STORAGE_KEYS = Set.of("hot", "warm", "cold");
    private static final Set<String> HOT_KEYS = Set.of("ttl");
    private static final Set<String> WARM_KEYS = Set.of("collection", "indexes");
    private static final Set<String> COLD_KEYS = Set.of("partition_by");
    private static final Set<String> VIEW_SCHEMA_KEYS = Set.of("enforce", "evolution");
    private static final Set<String> SERVE_USE_KEYS = Set.of("id", "use", "from");
    private static final Set<String> SERVE_INLINE_KEYS = Set.of("id", "from", "sync", "query", "push");
    private static final Set<String> SYNC_KEYS = Set.of("id", "source", "write_mode", "rename", "ddl", "options");
    private static final Set<String> RENAME_KEYS = Set.of("map", "case", "prefix", "suffix");
    private static final Set<String> QUERY_KEYS = Set.of("type", "backend");
    private static final Set<String> PUSH_KEYS = Set.of("id", "source", "topic", "format", "options");
    private static final Set<String> SETTINGS_KEYS = Set.of(
            "error_policy", "batch_size", "parallelism", "schedule", "read_mode", "start_from");
    private static final Set<String> TRANSFORM_DEF_KEYS = Set.of(
            "version", "kind", "id", "metadata", "type", "options", "experimental");
    private static final Set<String> VIEW_DEF_KEYS = Set.of(
            "version", "kind", "id", "metadata", "primary_key", "storage", "schema", "experimental");
    private static final Set<String> SERVE_DEF_KEYS = Set.of(
            "version", "kind", "id", "metadata", "sync", "query", "push", "experimental");

    /** Parses one YAML document into its {@link Resource} model. */
    public Resource parse(String yaml) {
        Node root = compose(yaml);
        if (!(root instanceof MappingNode mapping)) {
            throw YamlMap.error(DslError.ILLEGAL_VALUE, "", root,
                    Map.of("value", YamlMap.nodeTypeName(root), "expected", "a mapping"));
        }
        YamlMap doc = YamlMap.of(mapping, "");
        String kind = doc.string("kind");
        return switch (kind) {
            case "source" -> source(doc);
            case "pipeline" -> pipeline(doc);
            case "transform" -> transformDefinition(doc);
            case "view" -> viewDefinition(doc);
            case "serve" -> serveDefinition(doc);
            case null, default -> throw YamlMap.error(DslError.ILLEGAL_VALUE, "kind", mapping,
                    Map.of("value", kind == null ? "(absent)" : kind,
                            "expected", "one of: source, pipeline, transform, view, serve"));
        };
    }

    // ---- source -------------------------------------------------------------------

    private SourceResource source(YamlMap m) {
        m.requireOnly(SOURCE_KEYS);
        Map<String, Object> options = m.freeMap("options");
        rejectRelocatedReadOptions(m, options);
        return new SourceResource(
                idOf(m),
                metadata(m),
                m.string("connector"),
                m.freeMap("config"),
                mode(m, "mode"),
                tables(m.seq("tables")),
                options,
                srs(m.mapping("srs")),
                m.freeMap("experimental"));
    }

    /** Rejects read options that have moved to pipeline settings (read_mode / start_from). */
    private static void rejectRelocatedReadOptions(YamlMap m, Map<String, Object> options) {
        if (options == null) {
            return;
        }
        Node optionsNode = m.node("options");
        for (String key : RELOCATED_SOURCE_OPTIONS) {
            if (options.containsKey(key)) {
                throw YamlMap.error(DslError.UNKNOWN_FIELD, "options." + key, optionsNode, Map.of("field", key));
            }
        }
    }

    private List<TableRef> tables(List<Node> items) {
        if (items == null) {
            return null;
        }
        List<TableRef> refs = new ArrayList<>();
        for (Node n : items) {
            if (n instanceof ScalarNode sc) {
                String t = sc.getValue();
                refs.add(isRegex(t) ? TableRef.regex(unslash(t)) : TableRef.literal(t));
            } else {
                YamlMap ts = YamlMap.requireMapping(n, "tables");
                ts.requireOnly(TABLE_SPEC_KEYS);
                // tables[].filter is a source-row CEL predicate bound to source columns (§12),
                // not the event envelope. Its type environment is the source schema, unknown
                // offline — so its compile / type-check is deferred to the engine, unlike the
                // envelope-rooted expressions (filter node / map / push) checked at parse time.
                refs.add(TableRef.spec(ts.string("name"), ts.string("filter"),
                        scalarList(ts.seq("pk"), "tables.pk"), ts.freeMap("options")));
            }
        }
        return refs;
    }

    private Srs srs(YamlMap r) {
        if (r == null) {
            return null;
        }
        r.requireOnly(SRS_KEYS);
        return new Srs(r.string("key"), r.string("retention"),
                enumByYaml(SrsSchemaEvolution.values(), SrsSchemaEvolution::yaml, r, "schema_evolution"),
                boolValue(r, "queryable"),
                boolValue(r, "enabled"));
    }

    // ---- pipeline -----------------------------------------------------------------

    private PipelineResource pipeline(YamlMap m) {
        m.requireOnly(PIPELINE_KEYS);
        List<Step> transforms = transforms(m.seq("transforms"));
        String lastTransform = lastId(transforms);
        ViewBlock view = view(m, lastTransform);
        String beforeServe = view != null ? viewId(view) : lastTransform;
        ServeBlock serve = serve(m, beforeServe);
        return new PipelineResource(
                idOf(m), metadata(m), sources(m), transforms, view, serve, settings(m),
                m.freeMap("experimental"));
    }

    private static List<String> sources(YamlMap m) {
        Node n = m.node("source");
        if (n instanceof ScalarNode sc) {
            return List.of(sc.getValue());
        }
        List<String> ids = new ArrayList<>();
        if (n instanceof SequenceNode seq) {
            for (Node item : seq.getValue()) {
                ids.add(YamlMap.requireScalar(item, "source"));
            }
        }
        return ids;
    }

    // ---- transforms ---------------------------------------------------------------

    private List<Step> transforms(List<Node> items) {
        if (items == null) {
            return null;
        }
        List<Step> steps = new ArrayList<>();
        String prevId = null;
        int index = 0;
        for (Node n : items) {
            index++;
            Step step;
            if (n instanceof ScalarNode sc) {
                step = Step.use(null, sc.getValue(), naturalFrom(prevId, n), null);
            } else {
                YamlMap s = YamlMap.requireMapping(n, "transforms[" + (index - 1) + "]");
                if (s.has("use")) {
                    s.requireOnly(STEP_USE_KEYS);
                    step = Step.use(idOf(s), s.string("use"), fromFlow(s, prevId, n), s.freeMap("options"));
                } else {
                    step = inlineStep(s, index, prevId, n);
                }
            }
            steps.add(step);
            prevId = step.id();
        }
        return steps;
    }

    private Step inlineStep(YamlMap s, int index, String prevId, Node at) {
        String type = s.string("type");
        if (type == null) {
            throw YamlMap.error(DslError.COMPOSITION, s.childPath("type"), at,
                    Map.of("detail", "a transforms step needs either a type: (inline) or a use: (reference)"));
        }
        Set<String> allowed = new HashSet<>(STEP_BASE_KEYS);
        allowed.addAll(payloadKeys(type));
        s.requireOnly(allowed);
        String id = idOf(s);
        if (id == null) {
            id = type + "_" + index;
        }
        boolean aliased = type.equals("nest") || type.equals("join");
        FromClause from = aliased ? fromAliases(s, at) : fromFlow(s, prevId, at);
        return Step.inline(id, from, body(type, s), s.freeMap("options"), s.freeMap("experimental"));
    }

    private static Set<String> payloadKeys(String type) {
        return switch (type) {
            case "js" -> Set.of("script");
            case "map" -> Set.of("fields");
            case "filter" -> Set.of("expr");
            case "union" -> Set.of();
            case "nest" -> Set.of(
                    "primary_key", "order", "entries_in_memory", "max_elements_per_document", "root");
            case "join" -> Set.of("engine", "sql");
            default -> Set.of();
        };
    }

    private TransformBody body(String type, YamlMap s) {
        return switch (type) {
            case "js" -> new TransformBody.Js(s.string("script"));
            case "map" -> new TransformBody.MapProjection(fieldRules(s.mapping("fields")));
            case "filter" -> {
                String expr = s.string("expr");
                checkPredicate(s, "expr", expr);
                yield new TransformBody.Filter(expr);
            }
            case "union" -> new TransformBody.Union();
            case "nest" -> new TransformBody.Nest(
                    s.string("primary_key"),
                    enumByYaml(NestOrder.values(), NestOrder::yaml, s, "order"),
                    positiveIntValue(s, "entries_in_memory"),
                    positiveIntValue(s, "max_elements_per_document"),
                    nestRoot(s.mapping("root")));
            case "join" -> new TransformBody.Join(s.string("engine"), s.string("sql"));
            default -> throw YamlMap.error(DslError.ILLEGAL_VALUE, "type", s.node("type"),
                    Map.of("value", type, "expected", "a known transform type (js, map, filter, union, nest, join)"));
        };
    }

    private Map<String, FieldRule> fieldRules(YamlMap fields) {
        Map<String, FieldRule> rules = new LinkedHashMap<>();
        for (String name : fields.keys()) {
            FieldRule rule = fieldRule(YamlMap.nodeValue(fields.node(name)));
            if (rule instanceof FieldRule.Computed computed) {
                checkValue(fields, name, computed.celExpr());
            }
            rules.put(name, rule);
        }
        return rules;
    }

    private static FieldRule fieldRule(Object value) {
        if (Boolean.FALSE.equals(value)) {
            return FieldRule.drop();
        }
        if (value instanceof String s) {
            if (s.startsWith("$")) {
                return FieldRule.rename(s.substring(1));
            }
            if (s.startsWith("=")) {
                return FieldRule.computed(s.substring(1));
            }
        }
        return FieldRule.literal(value);
    }

    private NestRoot nestRoot(YamlMap r) {
        r.requireOnly(NEST_ROOT_KEYS);
        return new NestRoot(
                r.string("from"),
                scalarList(r.seq("key"), "key"),
                r.string("mode"),
                boolValue(r, "trackKeyChanges"),
                embeds(r.seq("embed")));
    }

    private List<Embed> embeds(List<Node> items) {
        if (items == null) {
            return null;
        }
        List<Embed> out = new ArrayList<>();
        for (Node n : items) {
            YamlMap e = YamlMap.requireMapping(n, "embed");
            e.requireOnly(EMBED_KEYS);
            out.add(new Embed(
                    e.string("from"),
                    stringMap(e, "on"),
                    enumByYaml(EmbedAs.values(), EmbedAs::yaml, e, "as"),
                    e.string("path"),
                    scalarList(e.seq("arrayKey"), "arrayKey"),
                    boolValue(e, "ignoreUpdates"),
                    boolValue(e, "trackKeyChanges"),
                    embeds(e.seq("embed"))));
        }
        return out;
    }

    // ---- view / serve -------------------------------------------------------------

    private ViewBlock view(YamlMap m, String prevId) {
        Node n = m.node("view");
        if (n == null) {
            return null;
        }
        if (n instanceof ScalarNode sc) {
            return new ViewBlock.Use(null, sc.getValue(), naturalRef(prevId, n));
        }
        YamlMap v = m.mapping("view");
        if (v.has("use")) {
            v.requireOnly(VIEW_USE_KEYS);
            return new ViewBlock.Use(idOf(v), v.string("use"), blockFrom(v, prevId, n));
        }
        v.requireOnly(VIEW_INLINE_KEYS);
        String id = idOf(v);
        return new ViewBlock.Inline(
                id != null ? id : "view",
                blockFrom(v, prevId, n),
                v.string("primary_key"),
                storage(v.mapping("storage")),
                viewSchema(v.mapping("schema")));
    }

    private Storage storage(YamlMap st) {
        if (st == null) {
            return null;
        }
        st.requireOnly(STORAGE_KEYS);
        Storage.Hot hot = null;
        if (st.has("hot")) {
            YamlMap h = st.mapping("hot");
            h.requireOnly(HOT_KEYS);
            hot = new Storage.Hot(h.string("ttl"));
        }
        Storage.Warm warm = null;
        if (st.has("warm")) {
            YamlMap w = st.mapping("warm");
            w.requireOnly(WARM_KEYS);
            warm = new Storage.Warm(w.string("collection"), scalarList(w.seq("indexes"), "indexes"));
        }
        Storage.Cold cold = null;
        if (st.has("cold")) {
            YamlMap c = st.mapping("cold");
            c.requireOnly(COLD_KEYS);
            cold = new Storage.Cold(scalarList(c.seq("partition_by"), "partition_by"));
        }
        return new Storage(hot, warm, cold);
    }

    private ViewSchema viewSchema(YamlMap sc) {
        if (sc == null) {
            return null;
        }
        sc.requireOnly(VIEW_SCHEMA_KEYS);
        return new ViewSchema(boolValue(sc, "enforce"), sc.string("evolution"));
    }

    private ServeBlock serve(YamlMap m, String prevId) {
        Node n = m.node("serve");
        if (n == null) {
            return null;
        }
        if (n instanceof ScalarNode sc) {
            return new ServeBlock.Use(null, sc.getValue(), naturalRef(prevId, n));
        }
        YamlMap s = m.mapping("serve");
        if (s.has("use")) {
            s.requireOnly(SERVE_USE_KEYS);
            return new ServeBlock.Use(idOf(s), s.string("use"), blockFrom(s, prevId, n));
        }
        s.requireOnly(SERVE_INLINE_KEYS);
        String id = idOf(s);
        return new ServeBlock.Inline(
                id != null ? id : "serve",     // anonymous serve block -> 'serve' (2026-06-15)
                blockFrom(s, prevId, n),
                syncList(s.seq("sync"), "serve."),
                queryList(s.seq("query"), "serve."),
                pushList(s.seq("push"), "serve."));
    }

    private List<SyncElement> syncList(List<Node> items, String pathPrefix) {
        if (items == null) {
            return null;
        }
        List<SyncElement> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            YamlMap s = YamlMap.requireMapping(items.get(i), pathPrefix + "sync[" + i + "]");
            s.requireOnly(SYNC_KEYS);
            String id = s.string("id");
            out.add(new SyncElement(
                    id != null ? id : "sync_" + (i + 1),     // anonymous sync element -> sync_<N> (2026-06-15)
                    s.string("source"),
                    enumByYaml(WriteMode.values(), WriteMode::yaml, s, "write_mode"),
                    rename(s.mapping("rename")),
                    enumByYaml(DdlPolicy.values(), DdlPolicy::yaml, s, "ddl"),
                    s.freeMap("options")));
        }
        return out;
    }

    private RenameSpec rename(YamlMap r) {
        if (r == null) {
            return null;
        }
        r.requireOnly(RENAME_KEYS);
        return new RenameSpec(
                stringMap(r, "map"),
                enumByYaml(RenameCase.values(), RenameCase::yaml, r, "case"),
                r.string("prefix"),
                r.string("suffix"));
    }

    private List<QueryElement> queryList(List<Node> items, String pathPrefix) {
        if (items == null) {
            return null;
        }
        List<QueryElement> out = new ArrayList<>();
        int i = 0;
        for (Node n : items) {
            YamlMap q = YamlMap.requireMapping(n, pathPrefix + "query[" + i++ + "]");
            q.requireOnly(QUERY_KEYS);
            out.add(new QueryElement(enumByYaml(QueryType.values(), QueryType::yaml, q, "type"), q.string("backend")));
        }
        return out;
    }

    private List<PushElement> pushList(List<Node> items, String pathPrefix) {
        if (items == null) {
            return null;
        }
        List<PushElement> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            YamlMap p = YamlMap.requireMapping(items.get(i), pathPrefix + "push[" + i + "]");
            p.requireOnly(PUSH_KEYS);
            String id = p.string("id");
            out.add(new PushElement(
                    id != null ? id : "push_" + (i + 1),     // anonymous push element -> push_<N> (2026-06-15)
                    p.string("source"), p.string("topic"),
                    pushFormat(p), p.freeMap("options")));
        }
        return out;
    }

    private PushFormat pushFormat(YamlMap owner) {
        Node n = owner.node("format");
        if (n == null) {
            return null;
        }
        if (n instanceof ScalarNode sc) {
            String v = sc.getValue();
            String expr = v.startsWith("=") ? v.substring(1) : v;   // optional '=' marker (X11)
            checkValue(owner, "format", expr);
            return PushFormat.cel(expr);
        }
        return PushFormat.fields(fieldRules(YamlMap.requireMapping(n, owner.childPath("format"))));
    }

    private Settings settings(YamlMap m) {
        YamlMap s = m.mapping("settings");
        if (s == null) {
            return null;
        }
        s.requireOnly(SETTINGS_KEYS);
        return new Settings(
                enumByYaml(ErrorPolicy.values(), ErrorPolicy::yaml, s, "error_policy"),
                intValue(s, "batch_size"),
                intValue(s, "parallelism"),
                s.string("schedule"),
                enumByYaml(ReadMode.values(), ReadMode::yaml, s, "read_mode"),
                s.string("start_from"));
    }

    private Metadata metadata(YamlMap m) {
        YamlMap md = m.mapping("metadata");
        if (md == null) {
            return null;
        }
        md.requireOnly(METADATA_KEYS);
        return new Metadata(stringMap(md, "labels"), md.string("description"));
    }

    // ---- definition bodies (kind: transform / view / serve) -----------------------

    /** A reusable transform body (ADR-0016 §5, X19): same payload grammar as an inline step, no wiring. */
    private TransformResource transformDefinition(YamlMap m) {
        forbidFrom(m);
        String type = m.string("type");
        if (type == null) {
            throw m.errorAt("type", DslError.COMPOSITION,
                    Map.of("detail", "a kind: transform definition needs a type:"));
        }
        Set<String> allowed = new HashSet<>(TRANSFORM_DEF_KEYS);
        allowed.addAll(payloadKeys(type));
        m.requireOnly(allowed);
        return new TransformResource(
                idOf(m), metadata(m), body(type, m), m.freeMap("options"), m.freeMap("experimental"));
    }

    /** A reusable MDM sink definition (ADR-0016 §7, X19): where/how to materialize, no wiring. */
    private ViewResource viewDefinition(YamlMap m) {
        forbidFrom(m);
        m.requireOnly(VIEW_DEF_KEYS);
        return new ViewResource(
                idOf(m), metadata(m), m.string("primary_key"),
                storage(m.mapping("storage")), viewSchema(m.mapping("schema")), m.freeMap("experimental"));
    }

    /** A reusable publish-surface definition (ADR-0016 §8, X19): sync / query / push, no wiring. */
    private ServeResource serveDefinition(YamlMap m) {
        forbidFrom(m);
        m.requireOnly(SERVE_DEF_KEYS);
        return new ServeResource(
                idOf(m), metadata(m),
                syncList(m.seq("sync"), ""), queryList(m.seq("query"), ""), pushList(m.seq("push"), ""),
                m.freeMap("experimental"));
    }

    /** X19: a definition body is pure logic; {@code from:} wiring belongs to the referencing step. */
    private static void forbidFrom(YamlMap m) {
        if (m.has("from")) {
            throw m.errorAt("from", DslError.FORBIDDEN_FIELD, Map.of("field", "from"));
        }
    }

    // ---- from: wiring -------------------------------------------------------------

    /** Streaming {@code from:} (js/map/filter/union/use): scalar or list, always a Flow. */
    private FromClause fromFlow(YamlMap owner, String prevId, Node at) {
        Node fn = owner.node("from");
        if (fn == null) {
            return naturalFrom(prevId, at);
        }
        if (fn instanceof ScalarNode sc) {
            return FromClause.list(fromRefToken(sc.getValue()));
        }
        if (!(fn instanceof SequenceNode seq)) {
            throw owner.errorAt("from", DslError.ILLEGAL_VALUE,
                    Map.of("value", YamlMap.nodeTypeName(fn), "expected", "a scalar or list for a streaming step"));
        }
        List<FromRef> refs = new ArrayList<>();
        for (Node item : seq.getValue()) {
            refs.add(fromRefToken(YamlMap.requireScalar(item, owner.childPath("from"))));
        }
        return new FromClause.Flow(refs);
    }

    /** Alias-map {@code from:} (nest/join); must be authored explicitly. */
    private FromClause fromAliases(YamlMap owner, Node at) {
        YamlMap am = owner.mapping("from");
        if (am == null) {
            throw YamlMap.error(DslError.COMPOSITION, owner.childPath("from"), at,
                    Map.of("detail", "nest/join require an explicit alias-map from:"));
        }
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        for (String alias : am.keys()) {
            aliases.put(alias, fromRef(am.node(alias)));
        }
        return FromClause.aliases(aliases);
    }

    /** A view/serve {@code from:} — a single ref, explicit or natural-order. */
    private FromRef blockFrom(YamlMap owner, String prevId, Node at) {
        Node fn = owner.node("from");
        return fn != null ? fromRef(fn) : naturalRef(prevId, at);
    }

    private static FromClause naturalFrom(String prevId, Node at) {
        if (prevId == null) {
            throw YamlMap.error(DslError.COMPOSITION, "transforms", at,
                    Map.of("detail", "the first transforms step must declare from: (no predecessor to wire from)"));
        }
        return FromClause.list(FromRef.literal(prevId));
    }

    private static FromRef naturalRef(String prevId, Node at) {
        if (prevId == null) {
            throw YamlMap.error(DslError.COMPOSITION, "from", at,
                    Map.of("detail", "from: omitted but there is no upstream output to wire from"));
        }
        return FromRef.literal(prevId);
    }

    private static FromRef fromRef(Node node) {
        return fromRefToken(YamlMap.requireScalar(node, "from"));
    }

    private static FromRef fromRefToken(String token) {
        return isRegex(token) ? FromRef.regex(unslash(token)) : FromRef.literal(token);
    }

    // ---- shared helpers -----------------------------------------------------------

    private static boolean isRegex(String token) {
        return token.length() >= 2 && token.startsWith("/") && token.endsWith("/");
    }

    private static String unslash(String token) {
        return token.substring(1, token.length() - 1);
    }

    private static String lastId(List<Step> steps) {
        return steps == null || steps.isEmpty() ? null : steps.get(steps.size() - 1).id();
    }

    private static String viewId(ViewBlock view) {
        return switch (view) {
            case ViewBlock.Inline i -> i.id();
            case ViewBlock.Use u -> u.id();
        };
    }

    private static SourceMode mode(YamlMap m, String key) {
        return enumByYaml(SourceMode.values(), SourceMode::yaml, m, key);
    }

    private static List<String> scalarList(List<Node> items, String path) {
        if (items == null) {
            return null;
        }
        List<String> out = new ArrayList<>();
        for (Node n : items) {
            out.add(YamlMap.requireScalar(n, path));
        }
        return out;
    }

    private static Map<String, String> stringMap(YamlMap owner, String key) {
        Map<String, Object> raw = owner.freeMap(key);
        if (raw == null) {
            return null;
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            if (!(e.getValue() instanceof String s)) {
                throw owner.errorAt(key, DslError.ILLEGAL_VALUE,
                        Map.of("value", String.valueOf(e.getValue()), "expected", "a string"));
            }
            out.put(e.getKey(), s);
        }
        return out;
    }

    private static Integer intValue(YamlMap m, String key) {
        Object v = m.value(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof Number num)) {
            throw m.errorAt(key, DslError.ILLEGAL_VALUE, Map.of("value", v, "expected", "an integer"));
        }
        if (v instanceof Long l && (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)) {
            throw m.errorAt(key, DslError.ILLEGAL_VALUE, Map.of("value", l, "expected", "a 32-bit integer"));
        }
        return num.intValue();
    }

    /**
     * As {@link #intValue}, and refused at zero and below. A capacity of none is not a small capacity:
     * it admits nothing at all, so it fails every run rather than some, and the author is told where the
     * number was written rather than where it is later enforced.
     */
    private static Integer positiveIntValue(YamlMap m, String key) {
        Integer value = intValue(m, key);
        if (value != null && value <= 0) {
            throw m.errorAt(key, DslError.ILLEGAL_VALUE,
                    Map.of("value", value, "expected", "a count above zero"));
        }
        return value;
    }

    private static Boolean boolValue(YamlMap m, String key) {
        Object v = m.value(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof Boolean b)) {
            throw m.errorAt(key, DslError.ILLEGAL_VALUE, Map.of("value", v, "expected", "true or false"));
        }
        return b;
    }

    /**
     * Reads a resource id and enforces the §2 charset rule: it must not contain the reserved
     * addressing separator {@code '.'} (used for {@code <id>.<table>} stream addressing and the
     * runtime {@code <pipeline_id>.<id>} qualified form). Applies to every id — top-level and inline.
     */
    private static String idOf(YamlMap m) {
        String id = m.string("id");
        if (id != null && id.indexOf('.') >= 0) {
            throw m.errorAt("id", DslError.ILLEGAL_VALUE,
                    Map.of("value", id, "expected", "an id without '.', the reserved addressing separator (§2)"));
        }
        return id;
    }

    /** Coerces a scalar field to an enum by its {@code yaml()} value; illegal-value on miss. */
    private static <E> E enumByYaml(E[] values, Function<E, String> yaml, YamlMap owner, String key) {
        String raw = owner.string(key);
        if (raw == null) {
            return null;
        }
        for (E e : values) {
            if (yaml.apply(e).equals(raw)) {
                return e;
            }
        }
        throw owner.errorAt(key, DslError.ILLEGAL_VALUE,
                Map.of("value", raw, "expected", allowedValues(values, yaml)));
    }

    /** Renders an enum's accepted {@code yaml()} tokens as {@code "one of: a, b, c"} for error reporting. */
    private static <E> String allowedValues(E[] values, Function<E, String> yaml) {
        StringBuilder sb = new StringBuilder("one of: ");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(yaml.apply(values[i]));
        }
        return sb.toString();
    }

    // ---- CEL expression checking (§12) --------------------------------------------

    /** A {@code filter} predicate (§12): must compile to {@code bool} against the §6 envelope (B4). */
    private static void checkPredicate(YamlMap owner, String key, String expr) {
        if (expr != null) {
            failExpression(owner, key, expr, RowExpressions.predicateError(expr));
        }
    }

    /** A computed value — {@code map} field or push {@code format} (§12): must compile against the envelope (B4). */
    private static void checkValue(YamlMap owner, String key, String expr) {
        if (expr != null) {
            failExpression(owner, key, expr, RowExpressions.valueError(expr));
        }
    }

    private static void failExpression(YamlMap owner, String key, String expr, String detail) {
        if (detail != null) {
            throw owner.errorAt(key, DslError.ILLEGAL_EXPRESSION,
                    Map.of("expr", expr, "detail", detail));
        }
    }

    // ---- yaml ---------------------------------------------------------------------

    private static Node compose(String yaml) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        Yaml parser = new Yaml(new SafeConstructor(options),
                new Representer(new DumperOptions()), new DumperOptions(), options,
                new TapstateDialectResolver());
        try {
            return parser.compose(new StringReader(yaml));
        } catch (MarkedYAMLException e) {
            // a located syntax error: surface it coded, with its 1-based line / column (snakeyaml is 0-based)
            int line = e.getProblemMark() != null ? e.getProblemMark().getLine() + 1 : 0;
            int column = e.getProblemMark() != null ? e.getProblemMark().getColumn() + 1 : 0;
            throw new DslException(DslError.MALFORMED_YAML, "", line, column, null,
                    Map.of("detail", detailOf(e)));
        } catch (YAMLException e) {
            // an unlocated YAML fault — still coded, never a raw stack trace at the user boundary
            throw new DslException(DslError.MALFORMED_YAML, "", 0, 0, null,
                    Map.of("detail", detailOf(e)));
        }
    }

    /** A single-line parser diagnostic for {@code detail}: the short problem if any, else the first message line. */
    private static String detailOf(YAMLException e) {
        if (e instanceof MarkedYAMLException marked && marked.getProblem() != null) {
            return marked.getProblem();
        }
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            return "could not parse YAML";
        }
        return message.lines().findFirst().orElse(message).trim();
    }
}
