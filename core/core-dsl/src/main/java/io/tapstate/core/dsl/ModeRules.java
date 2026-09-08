package io.tapstate.core.dsl;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mode × read-axis compatibility — the offline semantic half of the capability matrix that needs no
 * connector catalog (intra-DSL rules only; connector × mode legality is checked separately against
 * the catalog). Runs after reference closure as part of {@link Workspace#of}; the first violation
 * throws {@link DslError#MODE_MISMATCH}.
 *
 * <p>The read axis is pipeline-level: whether a pipeline's read has an <em>incremental tail</em>
 * depends on the referenced source modes and the pipeline's {@code read_mode}. A tail exists when a
 * pure stream source is referenced, or a cdc source is read with any {@code read_mode} other than
 * {@code snapshot_only}. The rules:
 *
 * <ul>
 *   <li>an {@code srs} block is legal only for {@code mode: cdc} — SRS derives solely from unbounded
 *       + keyed change semantics;</li>
 *   <li>{@code settings.read_mode: snapshot_only} is illegal when a pure stream source is referenced
 *       — an unbounded stream has no one-shot read;</li>
 *   <li>{@code settings.start_from} is legal only when the read has an incremental tail to position a
 *       consumer cursor into;</li>
 *   <li>{@code settings.schedule} is legal only for a bounded read — a read with an incremental tail
 *       has no re-run concept.</li>
 * </ul>
 */
final class ModeRules {

    private ModeRules() {
    }

    static void validate(Collection<Resource> batch) {
        Map<String, SourceResource> sources = new LinkedHashMap<>();
        for (Resource r : batch) {
            if (r instanceof SourceResource s) {
                sources.put(s.id(), s);
            }
        }
        for (Resource r : batch) {
            if (r instanceof SourceResource s) {
                checkSource(s);
            } else if (r instanceof PipelineResource p) {
                checkPipeline(p, sources);
            }
        }
    }

    private static void checkSource(SourceResource s) {
        if (s.srs() != null && s.mode() != SourceMode.CDC) {
            throw modeMismatch("srs", "srs", s.mode());
        }
    }

    private static void checkPipeline(PipelineResource p, Map<String, SourceResource> sources) {
        ReadMode readMode = readMode(p);
        boolean referencesStream = referencesMode(p, sources, SourceMode.STREAM);
        boolean referencesCdc = referencesMode(p, sources, SourceMode.CDC);

        // snapshot_only means "read the current rows once"; a pure stream source can't be read once.
        if (readMode == ReadMode.SNAPSHOT_ONLY && referencesStream) {
            throw modeMismatch("read_mode", "settings.read_mode", SourceMode.STREAM);
        }

        boolean hasIncrementalTail = referencesStream
                || (referencesCdc && readMode != ReadMode.SNAPSHOT_ONLY);

        // start_from positions a consumer cursor into an incremental tail; without a tail it is meaningless.
        if (startFrom(p) != null && !hasIncrementalTail) {
            throw modeMismatch("start_from", "settings.start_from", boundednessReason(p, sources, readMode));
        }

        // schedule re-runs the whole pipeline; a read with a tail never terminates, so it has no re-run.
        if (schedule(p) != null && hasIncrementalTail) {
            SourceMode witness = referencesStream ? SourceMode.STREAM : SourceMode.CDC;
            throw modeMismatch("schedule", "settings.schedule", witness);
        }
    }

    /**
     * The token that best explains why a read is bounded, reported as the {@code mode} of a
     * {@code start_from} mismatch: the {@code read_mode} that removed the tail when a cdc source is
     * read {@code snapshot_only}, else a referenced bounded source mode.
     */
    private static String boundednessReason(PipelineResource p, Map<String, SourceResource> sources,
                                            ReadMode readMode) {
        if (readMode == ReadMode.SNAPSHOT_ONLY && referencesMode(p, sources, SourceMode.CDC)) {
            return ReadMode.SNAPSHOT_ONLY.yaml();
        }
        for (String sid : p.sourceIds()) {
            SourceResource s = sources.get(sid);
            if (s != null && s.mode() != null) {
                return s.mode().yaml();
            }
        }
        return "(bounded)";
    }

    private static boolean referencesMode(PipelineResource p, Map<String, SourceResource> sources,
                                          SourceMode mode) {
        for (String sid : p.sourceIds()) {
            SourceResource s = sources.get(sid);    // existence already proved by reference closure
            if (s != null && s.mode() == mode) {
                return true;
            }
        }
        return false;
    }

    private static ReadMode readMode(PipelineResource p) {
        ReadMode rm = p.settings() == null ? null : p.settings().readMode();
        return rm == null ? ReadMode.SNAPSHOT_AND_CDC : rm;
    }

    private static String startFrom(PipelineResource p) {
        return p.settings() == null ? null : p.settings().startFrom();
    }

    private static String schedule(PipelineResource p) {
        return p.settings() == null ? null : p.settings().schedule();
    }

    private static DslException modeMismatch(String field, String path, SourceMode mode) {
        return modeMismatch(field, path, mode == null ? "(none)" : mode.yaml());
    }

    private static DslException modeMismatch(String field, String path, String mode) {
        return new DslException(DslError.MODE_MISMATCH, path, 0, 0, null,
                Map.of("field", field, "mode", mode));
    }
}
