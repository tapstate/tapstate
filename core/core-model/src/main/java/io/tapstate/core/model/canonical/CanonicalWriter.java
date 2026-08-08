package io.tapstate.core.model.canonical;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
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
import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.ErrorPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Serializes a resource model to its canonical {@code .tap.yml} text — the single
 * deterministic form defined by {@code docs/reference/canonical-form.md} (locked
 * 2026-06-12). Key order, default omission, sugar normalization, quoting and layout are
 * all fixed there; changing any of it means changing that document plus golden review.
 */
public final class CanonicalWriter {

    public String write(Resource resource) {
        Node.MapN tree = switch (resource) {
            case SourceResource s -> source(s);
            case PipelineResource p -> pipeline(p);
            case TransformResource t -> transformDefinition(t);
            case ViewResource v -> viewDefinition(v);
            case ServeResource s -> serveDefinition(s);
        };
        return new YamlEmitter().emit(tree);
    }

    // ---- top-level resources ------------------------------------------------------

    private Node.MapN source(SourceResource s) {
        B b = new B();
        header(b, s);
        b.scalar("connector", s.connector());
        b.freeMap("config", s.config());
        if (s.mode() != null) {
            b.scalar("mode", s.mode().yaml());
        }
        if (s.tables() != null) {
            b.put("tables", tables(s.tables()));
        }
        b.freeMap("options", s.options());
        if (s.srs() != null) {
            b.put("srs", srs(s.srs()));
        }
        b.freeMap("experimental", s.experimental());
        return b.build();
    }

    private Node.MapN pipeline(PipelineResource p) {
        B b = new B();
        header(b, p);
        if (p.sources().size() == 1) {
            b.scalar("source", p.sources().get(0));
        } else {
            b.scalarSeq("source", p.sources());
        }
        if (p.transforms() != null) {
            List<Node> steps = new ArrayList<>();
            for (Step st : p.transforms()) {
                steps.add(step(st));
            }
            b.put("transforms", new Node.SeqN(steps));
        }
        if (p.view() != null) {
            b.put("view", view(p.view()));
        }
        if (p.serve() != null) {
            b.put("serve", serve(p.serve()));
        }
        if (p.settings() != null) {
            b.put("settings", settings(p.settings()));
        }
        b.freeMap("experimental", p.experimental());
        return b.build();
    }

    private Node.MapN transformDefinition(TransformResource t) {
        B b = new B();
        header(b, t);
        b.scalar("type", t.body().type());
        body(b, t.body());
        b.freeMap("options", t.options());
        b.freeMap("experimental", t.experimental());
        return b.build();
    }

    private Node.MapN viewDefinition(ViewResource v) {
        B b = new B();
        header(b, v);
        b.scalar("primary_key", v.primaryKey());
        if (v.storage() != null) {
            b.put("storage", storage(v.storage()));
        }
        if (v.schema() != null) {
            b.put("schema", viewSchema(v.schema()));
        }
        b.freeMap("experimental", v.experimental());
        return b.build();
    }

    private Node.MapN serveDefinition(ServeResource s) {
        B b = new B();
        header(b, s);
        serveElements(b, s.sync(), s.query(), s.push());
        b.freeMap("experimental", s.experimental());
        return b.build();
    }

    private void header(B b, Resource r) {
        b.scalar("version", Resource.VERSION);
        b.scalar("kind", r.kind());
        b.scalar("id", r.id());
        Metadata md = r.metadata();
        if (md != null && !md.isEmpty()) {
            B m = new B();
            if (!md.labels().isEmpty()) {
                m.freeMap("labels", new TreeMap<>(md.labels()));
            }
            m.scalar("description", md.description());
            b.put("metadata", m.build());
        }
    }

    // ---- source pieces ------------------------------------------------------------

    private Node tables(List<TableRef> tables) {
        List<Node> items = new ArrayList<>();
        for (TableRef t : tables) {
            switch (t) {
                case TableRef.Literal l -> items.add(scalar(l.name()));
                case TableRef.Regex r -> items.add(scalar("/" + r.pattern() + "/"));
                case TableRef.Spec sp -> {
                    B e = new B();
                    e.scalar("name", sp.name());
                    e.expression("filter", sp.filter());
                    e.scalarSeq("pk", sp.pk());
                    e.freeMap("options", sp.options());
                    items.add(e.build());
                }
            }
        }
        return new Node.SeqN(items);
    }

    private Node srs(Srs srs) {
        B b = new B();
        b.scalar("key", srs.key());
        b.scalar("retention", srs.retention());
        if (srs.schemaEvolution() != null) {
            b.scalar("schema_evolution", srs.schemaEvolution().yaml());
        }
        b.scalar("queryable", srs.queryable());
        if (Boolean.FALSE.equals(srs.enabled())) {
            b.scalar("enabled", false);     // default true -> omit; only the off switch is written
        }
        return b.build();
    }

    // ---- pipeline pieces ----------------------------------------------------------

    private Node.MapN step(Step st) {
        B b = new B();
        switch (st) {
            case Step.Inline s -> {
                b.scalar("id", s.id());
                b.scalar("type", s.body().type());
                b.put("from", fromClause(s.from()));
                body(b, s.body());
                b.freeMap("options", s.options());
                b.freeMap("experimental", s.experimental());
            }
            case Step.Use u -> {
                if (!u.id().equals(u.use())) {
                    b.scalar("id", u.id());
                }
                b.scalar("use", u.use());
                b.put("from", fromClause(u.from()));
                b.freeMap("options", u.options());
            }
        }
        return b.build();
    }

    private void body(B b, TransformBody body) {
        switch (body) {
            case TransformBody.Js js -> b.literal("script", js.script());
            case TransformBody.MapProjection mp -> b.put("fields", fieldRules(mp.fields()));
            case TransformBody.Filter f -> b.expression("expr", f.expr());
            case TransformBody.Union ignored -> {
            }
            case TransformBody.Nest n -> {
                b.scalar("primary_key", n.primaryKey());
                if (n.order() != null) {
                    b.scalar("order", n.order().yaml());
                }
                b.scalar("entries_in_memory", n.entriesInMemory());
                b.scalar("max_elements_per_document", n.maxElementsPerDocument());
                b.put("root", nestRoot(n.root()));
            }
            case TransformBody.Join j -> {
                b.scalar("engine", j.engine());
                b.literal("sql", j.sql());
            }
        }
    }

    private Node fieldRules(Map<String, FieldRule> fields) {
        B b = new B();
        for (Map.Entry<String, FieldRule> e : fields.entrySet()) {
            b.put(e.getKey(), switch (e.getValue()) {
                case FieldRule.Rename r -> scalar("$" + r.sourceField());
                case FieldRule.Drop ignored -> scalar(Boolean.FALSE);
                case FieldRule.Literal l -> scalar(l.value());
                case FieldRule.Computed c -> new Node.ScalarN("=" + c.celExpr(), Node.Style.EXPRESSION);
            });
        }
        return b.build();
    }

    private Node nestRoot(NestRoot root) {
        B b = new B();
        b.scalar("from", root.from());
        b.scalarSeq("key", root.key());
        b.scalar("mode", root.mode());
        b.scalar("trackKeyChanges", root.trackKeyChanges());
        b.put("embed", embeds(root.embed()));
        return b.build();
    }

    private Node embeds(List<Embed> embeds) {
        if (embeds == null) {
            return new Node.SeqN(List.of());
        }
        List<Node> items = new ArrayList<>();
        for (Embed e : embeds) {
            B b = new B();
            b.scalar("from", e.from());
            b.freeMap("on", new TreeMap<>(e.on()));
            b.scalar("as", e.as().yaml());
            b.scalar("path", e.path());
            b.scalarSeq("arrayKey", e.arrayKey());
            b.scalar("ignoreUpdates", e.ignoreUpdates());
            b.scalar("trackKeyChanges", e.trackKeyChanges());
            if (e.embed() != null) {
                b.put("embed", embeds(e.embed()));
            }
            items.add(b.build());
        }
        return new Node.SeqN(items);
    }

    private Node fromClause(FromClause from) {
        return switch (from) {
            case FromClause.Flow f -> {
                List<Node> items = new ArrayList<>();
                for (FromRef r : f.refs()) {
                    items.add(scalar(fromRef(r)));
                }
                yield new Node.SeqN(items);
            }
            case FromClause.Aliases a -> {
                B b = new B();
                for (Map.Entry<String, FromRef> e : new TreeMap<>(a.aliases()).entrySet()) {
                    b.scalar(e.getKey(), fromRef(e.getValue()));
                }
                yield b.build();
            }
        };
    }

    private String fromRef(FromRef ref) {
        return switch (ref) {
            case FromRef.Literal l -> l.ref();
            case FromRef.Regex r -> "/" + r.pattern() + "/";
        };
    }

    private Node view(ViewBlock view) {
        B b = new B();
        switch (view) {
            case ViewBlock.Inline v -> {
                b.scalar("id", v.id());
                b.scalar("from", fromRef(v.from()));
                b.scalar("primary_key", v.primaryKey());
                if (v.storage() != null) {
                    b.put("storage", storage(v.storage()));
                }
                if (v.schema() != null) {
                    b.put("schema", viewSchema(v.schema()));
                }
            }
            case ViewBlock.Use u -> {
                if (!u.id().equals(u.use())) {
                    b.scalar("id", u.id());
                }
                b.scalar("use", u.use());
                b.scalar("from", fromRef(u.from()));
            }
        }
        return b.build();
    }

    private Node serve(ServeBlock serve) {
        B b = new B();
        switch (serve) {
            case ServeBlock.Inline s -> {
                b.scalar("id", s.id());
                b.scalar("from", fromRef(s.from()));
                serveElements(b, s.sync(), s.query(), s.push());
            }
            case ServeBlock.Use u -> {
                if (!u.id().equals(u.use())) {
                    b.scalar("id", u.id());
                }
                b.scalar("use", u.use());
                b.scalar("from", fromRef(u.from()));
            }
        }
        return b.build();
    }

    private void serveElements(B b, List<SyncElement> sync, List<QueryElement> query,
                               List<PushElement> push) {
        if (sync != null) {
            List<Node> items = new ArrayList<>();
            for (SyncElement e : sync) {
                items.add(syncElement(e));
            }
            b.put("sync", new Node.SeqN(items));
        }
        if (query != null) {
            List<Node> items = new ArrayList<>();
            for (QueryElement e : query) {
                B q = new B();
                q.scalar("type", e.type().yaml());
                q.scalar("backend", e.backend());
                items.add(q.build());
            }
            b.put("query", new Node.SeqN(items));
        }
        if (push != null) {
            List<Node> items = new ArrayList<>();
            for (PushElement e : push) {
                items.add(pushElement(e));
            }
            b.put("push", new Node.SeqN(items));
        }
    }

    private Node syncElement(SyncElement e) {
        B b = new B();
        b.scalar("id", e.id());
        b.scalar("source", e.source());
        if (e.writeMode() != null && e.writeMode() != WriteMode.UPSERT) {
            b.scalar("write_mode", e.writeMode().yaml());
        }
        if (e.rename() != null) {
            b.put("rename", rename(e.rename()));
        }
        if (e.ddl() != null && e.ddl() != DdlPolicy.FAIL) {
            b.scalar("ddl", e.ddl().yaml());
        }
        b.freeMap("options", dropDefault(e.options(), "auto_create_table", Boolean.TRUE));
        return b.build();
    }

    private Node rename(RenameSpec r) {
        B b = new B();
        if (r.map() != null && !r.map().isEmpty()) {
            b.freeMap("map", new TreeMap<>(r.map()));
        }
        if (r.caseMode() != null) {
            b.scalar("case", r.caseMode().yaml());
        }
        b.scalar("prefix", r.prefix());
        b.scalar("suffix", r.suffix());
        return b.build();
    }

    private Node pushElement(PushElement e) {
        B b = new B();
        b.scalar("id", e.id());
        b.scalar("source", e.source());
        b.scalar("topic", e.topic());
        switch (e.format()) {
            case PushFormat.Cel c -> b.put("format",
                    new Node.ScalarN("=" + c.expr(), Node.Style.EXPRESSION));
            case PushFormat.Fields f -> b.put("format", fieldRules(f.fields()));
            case null -> {
            }
        }
        b.freeMap("options", e.options());
        return b.build();
    }

    private Node settings(Settings s) {
        B b = new B();
        if (s.errorPolicy() != null && s.errorPolicy() != ErrorPolicy.FAIL) {
            b.scalar("error_policy", s.errorPolicy().yaml());
        }
        if (s.batchSize() != null && s.batchSize() != 1000) {
            b.scalar("batch_size", s.batchSize());
        }
        if (s.parallelism() != null && s.parallelism() != 1) {
            b.scalar("parallelism", s.parallelism());
        }
        b.scalar("schedule", s.schedule());
        if (s.readMode() != null && s.readMode() != ReadMode.SNAPSHOT_AND_CDC) {
            b.scalar("read_mode", s.readMode().yaml());
        }
        if (s.startFrom() != null && !"latest".equals(s.startFrom())) {
            b.scalar("start_from", s.startFrom());
        }
        return b.build();
    }

    private Node storage(Storage st) {
        B b = new B();
        if (st.hot() != null) {
            B t = new B();
            t.scalar("ttl", st.hot().ttl());
            b.put("hot", t.build());
        }
        if (st.warm() != null) {
            B t = new B();
            t.scalar("collection", st.warm().collection());
            t.scalarSeq("indexes", st.warm().indexes());
            b.put("warm", t.build());
        }
        if (st.cold() != null) {
            B t = new B();
            t.scalarSeq("partition_by", st.cold().partitionBy());
            b.put("cold", t.build());
        }
        return b.build();
    }

    private Node viewSchema(ViewSchema schema) {
        B b = new B();
        b.scalar("enforce", schema.enforce());
        b.scalar("evolution", schema.evolution());
        return b.build();
    }

    // ---- shared helpers -----------------------------------------------------------

    /** Drops an entry whose value equals its documented constant default (§4). */
    private static Map<String, Object> dropDefault(Map<String, Object> map, String key, Object def) {
        if (map == null || !def.equals(map.get(key))) {
            return map;
        }
        Map<String, Object> copy = new TreeMap<>(map);
        copy.remove(key);
        return copy;
    }

    /** Free-form value (config / options / labels / experimental): maps sort by key (§6). */
    private static Node fromValue(Object v) {
        if (v instanceof Map<?, ?> m) {
            B b = new B();
            for (Map.Entry<String, Object> e : new TreeMap<>(asStringMap(m)).entrySet()) {
                b.put(e.getKey(), fromValue(e.getValue()));
            }
            return b.build();
        }
        if (v instanceof List<?> l) {
            List<Node> items = new ArrayList<>();
            for (Object o : l) {
                items.add(fromValue(o));
            }
            return new Node.SeqN(items);
        }
        return scalar(v);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asStringMap(Map<?, ?> m) {
        return (Map<String, Object>) m;
    }

    private static Node.ScalarN scalar(Object v) {
        return new Node.ScalarN(v, Node.Style.AUTO);
    }

    /** Ordered map builder; null / empty values are silently skipped (§4 empty-container rule). */
    private static final class B {
        private final List<Node.Entry> entries = new ArrayList<>();

        void put(String key, Node value) {
            if (value instanceof Node.MapN m && m.entries().isEmpty()) {
                return;
            }
            if (value instanceof Node.SeqN q && q.items().isEmpty()) {
                return;
            }
            entries.add(new Node.Entry(key, value));
        }

        void scalar(String key, Object value) {
            if (value != null) {
                put(key, CanonicalWriter.scalar(value));
            }
        }

        void expression(String key, String cel) {
            if (cel != null) {
                put(key, new Node.ScalarN(cel, Node.Style.EXPRESSION));
            }
        }

        void literal(String key, String text) {
            if (text != null) {
                put(key, new Node.ScalarN(text, Node.Style.LITERAL));
            }
        }

        void scalarSeq(String key, List<String> values) {
            if (values != null && !values.isEmpty()) {
                List<Node> items = new ArrayList<>();
                for (String v : values) {
                    items.add(CanonicalWriter.scalar(v));
                }
                put(key, new Node.SeqN(items));
            }
        }

        void freeMap(String key, Map<String, Object> map) {
            if (map != null && !map.isEmpty()) {
                put(key, fromValue(map));
            }
        }

        Node.MapN build() {
            return new Node.MapN(List.copyOf(entries));
        }
    }
}
