package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Type-specific payload of a transform — shared between inline pipeline steps and
 * {@code kind: transform} definition bodies (ADR-0016 §5, X19: a definition body is pure
 * logic; wiring lives on the step).
 */
@Doc("Type-specific payload of a transform, selected by the type discriminator.")
public sealed interface TransformBody {

    /** The {@code type:} discriminator as it appears in YAML. */
    String type();

    /** {@code type: js} — GraalVM escape hatch; sees all events. */
    @YamlType("js")
    @Doc("JavaScript transform running on GraalVM; sees every event.")
    record Js(
            @Doc(value = "The JavaScript source executed for each event.", required = true)
            String script) implements TransformBody {
        public Js {
            Objects.requireNonNull(script, "script");
        }

        @Override
        public String type() {
            return "js";
        }
    }

    /** {@code type: map} — field projection; declared order is semantic (output order). */
    @YamlType("map")
    @Doc("Field projection transform; the declared field order is the output order.")
    record MapProjection(
            @Doc(value = "Output fields keyed by name, each mapped by a field rule; declared order is the output order.", required = true)
            Map<String, FieldRule> fields) implements TransformBody {
        public MapProjection {
            Objects.requireNonNull(fields, "fields");
            fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        }

        @Override
        public String type() {
            return "map";
        }
    }

    /** {@code type: filter} — CEL row predicate (§12); non-row events bypass (X15). */
    @YamlType("filter")
    @Doc("Row predicate transform; rows failing the expression are dropped while non-row events pass through.")
    record Filter(
            @Doc(value = "The CEL boolean expression evaluated against each row.", required = true)
            String expr) implements TransformBody {
        public Filter {
            Objects.requireNonNull(expr, "expr");
        }

        @Override
        public String type() {
            return "filter";
        }
    }

    /** {@code type: union} — explicit multi-stream merge. */
    @YamlType("union")
    @Doc("Explicit merge of multiple input streams into one.")
    record Union() implements TransformBody {
        @Override
        public String type() {
            return "union";
        }
    }

    /** {@code type: nest} — stateful materializing node producing nested documents (§5.1). */
    @YamlType("nest")
    @Doc("Stateful transform that materializes nested documents from related streams.")
    record Nest(
            @Doc("Primary key used to group child records under their parent document.")
            String primaryKey,
            @Doc("Ordering applied to nested child records.")
            NestOrder order,
            @Doc(value = "How many entries each of this nest's levels keeps in memory; what is beyond it "
                    + "is kept on the layer behind them and read back as it is asked for. A count of "
                    + "entries, not of bytes. Absent leaves the number the deployment was started with.",
                    key = "entries_in_memory")
            Integer entriesInMemory,
            @Doc(value = "How many embedded elements one document of this nest may hold before the run "
                    + "is failed. Per document rather than per nest: a document is assembled whole, so "
                    + "no layer behind the memory can absorb one that outgrows it. Absent leaves the "
                    + "number the deployment was started with.",
                    key = "max_elements_per_document")
            Integer maxElementsPerDocument,
            @Doc(value = "The root stream whose documents receive the nested children.", required = true)
            NestRoot root) implements TransformBody {
        public Nest {
            Objects.requireNonNull(root, "root");
        }

        /** A nest that writes down no capacity of its own, and so runs on whatever the deployment set. */
        public Nest(String primaryKey, NestOrder order, NestRoot root) {
            this(primaryKey, order, null, null, root);
        }

        @Override
        public String type() {
            return "nest";
        }
    }

    /** {@code type: join} — duckdb flat wide-table materialization (§5.2). */
    @YamlType("join")
    @Doc("Materializes a flat wide table by joining streams with SQL.")
    record Join(
            @Doc(value = "The query engine that runs the join, such as duckdb.", required = true)
            String engine,
            @Doc(value = "The SQL query that produces the joined wide table.", required = true)
            String sql) implements TransformBody {
        public Join {
            Objects.requireNonNull(engine, "engine");
            Objects.requireNonNull(sql, "sql");
        }

        @Override
        public String type() {
            return "join";
        }
    }
}
