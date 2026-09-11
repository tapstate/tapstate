package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Type-specific payload of a transform — shared between inline pipeline steps and
 * {@code kind: transform} definition bodies (§5, X19: a definition body is pure
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

    /**
     * {@code type: unwind} — one output row per element of an array field; stateless and row-level,
     * and the only one of that family that changes how many rows travel on.
     *
     * <p><b>Three of the five keys are not this project's invention.</b> A document store's own
     * unwind stage carries a path, an ordinal-column name and a keep-the-empty-ones flag, and those
     * three arrive here under the same names, spelled the way every key here is spelled. An author
     * who already knows that stage does not have to learn a second vocabulary for the same three
     * things, and the camelCase spellings they may arrive with are refused by name rather than
     * ignored - an ignored option expands without the column that was asked for, and the rows look
     * right until somebody counts them.
     *
     * <p><b>The other two exist because that stage never writes anywhere.</b> It hands rows to the
     * next stage of a query; this hands them to a table that has a key and column types. So one key
     * says which field inside an element identifies the row it becomes, and one says what the
     * expanded column is declared as. Both are optional, and both carry the {@code element_} prefix
     * so that which half of the vocabulary a key belongs to is visible without looking it up.
     */
    @YamlType("unwind")
    @Doc("Expands one row into one row per element of an array field.")
    record Unwind(
            @Doc(value = "The array field to expand; one output row is produced per element of it.",
                    required = true)
            String path,
            @Doc("Name of a column carrying each element's ordinal within the array; absent adds none.")
            String includeArrayIndex,
            @Doc(value = "Whether a row whose array is null, missing or empty still produces one "
                    + "output row, that field left empty. Absent drops such rows.", def = "false")
            Boolean preserveNullAndEmptyArrays,
            @Doc("Field inside each element that identifies its row; also copied into a top-level "
                    + "column of the same name, which joins the parent key at the target.")
            String elementKey,
            @Doc("Declared type of the expanded column; absent leaves the connector to infer one.")
            String elementType) implements TransformBody {
        public Unwind {
            Objects.requireNonNull(path, "path");
        }

        @Override
        public String type() {
            return "unwind";
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

    /** {@code type: join} — flat wide-table materialization over joined streams (§5.2). */
    @YamlType("join")
    @Doc("Materializes a flat wide table by joining streams with SQL.")
    record Join(
            @Doc(value = "The engine that runs the join.", required = true)
            JoinEngine engine,
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
