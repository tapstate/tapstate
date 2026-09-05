package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.OutputField;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.DerivedSchemaStore;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Holds a join step's output columns to the ones it was last recorded producing, and refuses the start
 * when they have moved for a reason the author did not ask for.
 *
 * <p>A join's output columns are worked out rather than written down: they follow from the SELECT and
 * from what the source tables say their columns are. Both of those move on their own, so the same
 * pipeline, untouched, can start one day producing a differently shaped row than it produced the day
 * before - a wider column, a different decimal precision, a column that is suddenly nullable. Nothing
 * downstream can see that: the writes succeed, the target holds rows of the wrong shape, and the first
 * report is a truncation or a rounding somebody notices weeks later.
 *
 * <p><b>Three outcomes, not two, and the third one is the common one.</b>
 *
 * <ul>
 *   <li><b>The author edited the query.</b> The new shape is what they asked for. Recorded and allowed
 *       through - refusing here would mean every ordinary edit needed a ceremony to get past.</li>
 *   <li><b>The sources evolved.</b> Ordinary in a change-data product and the operator's call, so the
 *       refusal says so and points at the source side. Reported as
 *       {@code actuation.join-output-schema-source-changed}.</li>
 *   <li><b>The derivation changed.</b> Our compatibility break, not theirs. Reported as
 *       {@code actuation.join-output-schema-engine-changed}, which reads differently on purpose: the
 *       operator has nothing to fix, and the message says to report it.</li>
 * </ul>
 *
 * <p>Telling them apart is what the recorded provenance is for. All three produce a byte-identical
 * difference report - these columns appeared, these went, these changed type - so nothing in the
 * difference itself can attribute it. Only what the earlier answer was computed from can, and it has to
 * keep the author's input and the world's input apart or an ordinary edit reads as the sources moving.
 *
 * <p><b>Why two codes and not one with a reason field.</b> The two want opposite handling and would
 * otherwise have to share one message. A single code has to be written either for the ordinary case,
 * in which case the compatibility break reads as routine and nobody reports it, or for the rare one, in
 * which case the ordinary case reads as an emergency and operators learn to work around the check.
 * Neither is a check anybody keeps.
 */
final class JoinSchemaDrift {

    /**
     * What worked the columns out. Informational only - it is reported, never compared, because the
     * question "did the derivation change" is answered by the columns moving while its inputs did not,
     * which is true whether or not anybody remembered to bump a version string.
     */
    private static final String DERIVED_BY = io.tapstate.core.sql.SqlFrontEnd.DERIVATION_VERSION;

    private final DerivedSchemaStore records;

    JoinSchemaDrift(DerivedSchemaStore records) {
        this.records = Objects.requireNonNull(records, "records");
    }

    /**
     * Holds one join step to what it was last recorded producing, then records today's answer. Throws
     * when the columns moved for a reason the author did not ask for; returns having recorded otherwise.
     *
     * <p>A refused start records nothing. The record is what the next comparison is made against, so
     * absorbing the new shape here would make the difference undetectable by the time anyone looked -
     * the start would refuse once and then quietly run on the new shape forever after.
     *
     * <p><b>Column order is not part of what is compared</b>, although the record preserves it. Two
     * schemas holding the same names at the same types are the same shape here even if they arrive in
     * a different order - which is reachable, through a {@code SELECT *} over a source table that was
     * rebuilt with its columns in another order. Refusing that would be a false alarm: nothing about a
     * reordering can truncate or round a value, and the writes are matched by name. The cost, stated
     * because it is not obvious: a reordering leaves the recorded order stale, so the report renders
     * the order last recorded rather than today's.
     */
    void checkAndRecord(String pipelineId, String stepId, String sql, JoinPlan plan,
            List<SourceTable> tables) {
        Map<String, String> columns = columnsOf(plan);
        String statement = fingerprintOf(sql);
        String derivedFrom = fingerprintOf(tables);
        Optional<DerivedSchema> recorded = records.latest(pipelineId, stepId);
        if (recorded.isPresent()
                && recorded.get().statement().equals(statement)
                && !recorded.get().schema().equals(columns)) {
            throw drift(pipelineId, stepId, recorded.get(), columns, derivedFrom);
        }
        records.record(pipelineId, stepId, columns, statement, derivedFrom, DERIVED_BY);
    }

    /**
     * Records what a join step produces now as the shape to hold it to from here, whatever it was
     * recorded producing before. This is the way past a refusal, and it exists as its own method rather
     * than as a flag on the one above because the two are different acts: one is a machine checking,
     * the other is a person having looked and said to carry on.
     *
     * <p>It goes through this class rather than writing to the store directly so that what is accepted
     * is byte-for-byte what the next start will compute. A second renderer of the same columns would
     * drift from this one eventually, and the shape that takes is an accept that does not clear the
     * refusal it was run for.
     */
    void record(String pipelineId, String stepId, String sql, JoinPlan plan, List<SourceTable> tables) {
        records.record(pipelineId, stepId, columnsOf(plan), fingerprintOf(sql), fingerprintOf(tables),
                DERIVED_BY);
    }

    /** How one derived column's type is written down, on every side that writes one down. */
    static String declaredType(OutputField field) {
        return field.type() + (field.nullable() ? " NULL" : " NOT NULL");
    }

    /**
     * The refusal, attributed. The two codes carry the same difference because the difference is the
     * same; what differs is who has to act on it, and that is what the code says.
     */
    private static TapstateException drift(String pipelineId, String stepId, DerivedSchema recorded,
            Map<String, String> columns, String derivedFrom) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("pipeline", pipelineId);
        params.put("step", stepId);
        params.put("added", join(added(recorded.schema(), columns)));
        params.put("removed", join(added(columns, recorded.schema())));
        params.put("retyped", join(retyped(recorded.schema(), columns)));
        if (recorded.derivedFrom().equals(derivedFrom)) {
            // Same query, same source columns, different answer: the derivation changed under both of
            // them, which is ours to fix and not the operator's.
            params.put("recordedBy", recorded.derivedBy());
            params.put("nowBy", DERIVED_BY);
            return new TapstateException(
                    ActuationError.JOIN_OUTPUT_SCHEMA_ENGINE_CHANGED, Map.copyOf(params), null);
        }
        return new TapstateException(
                ActuationError.JOIN_OUTPUT_SCHEMA_SOURCE_CHANGED, Map.copyOf(params), null);
    }

    /** The columns this join publishes: output name to declared type, in the order it publishes them. */
    private static Map<String, String> columnsOf(JoinPlan plan) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (OutputField field : plan.outputFields()) {
            columns.put(field.name(), declaredType(field));
        }
        return columns;
    }

    /** A fingerprint of what the author wrote. */
    private static String fingerprintOf(String sql) {
        return ContentHash.of(sql.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A fingerprint of what the derivation read from the world: every source column it could see, by
     * table, name, type and nullability.
     *
     * <p>Sorted, and de-duplicated by the rendered line rather than by table name. A table reaches here
     * under both the name the SQL aliased it to and its own, so the same columns appear twice; keeping
     * the order the caller happened to build them in would make the fingerprint depend on map iteration
     * rather than on the columns, and a fingerprint that moves on its own reports the sources as having
     * changed when nothing did.
     */
    private static String fingerprintOf(List<SourceTable> tables) {
        Set<String> lines = new LinkedHashSet<>();
        for (SourceTable table : tables) {
            StringBuilder line = new StringBuilder(table.name()).append('(');
            for (SourceColumn column : table.columns()) {
                line.append(column.name()).append(':').append(column.type())
                        .append(column.nullable() ? "?" : "!").append(',');
            }
            lines.add(line.append(')').toString());
        }
        List<String> sorted = new ArrayList<>(lines);
        sorted.sort(null);
        return ContentHash.of(String.join(";", sorted).getBytes(StandardCharsets.UTF_8));
    }

    /** The column names present in {@code now} and absent from {@code before}. */
    private static List<String> added(Map<String, String> before, Map<String, String> now) {
        List<String> names = new ArrayList<>();
        for (String name : now.keySet()) {
            if (!before.containsKey(name)) {
                names.add(name);
            }
        }
        return names;
    }

    /** The columns present in both whose declared type moved, rendered as {@code name: was -> now}. */
    private static List<String> retyped(Map<String, String> before, Map<String, String> now) {
        List<String> changes = new ArrayList<>();
        now.forEach((name, type) -> {
            String was = before.get(name);
            if (was != null && !was.equals(type)) {
                changes.add(name + ": " + was + " -> " + type);
            }
        });
        return changes;
    }

    /**
     * The list as one cell, or {@code none} where it is empty. Never the empty string: a message reading
     * "columns appeared: " leaves the reader working out whether that means none appeared or whether
     * something failed to render.
     */
    private static String join(List<String> items) {
        return items.isEmpty() ? "none" : String.join(", ", items);
    }
}
