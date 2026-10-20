package io.tapstate.spi.store;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

/** One bounded Cluster recovery confirmation, independent from cache leases and business actuation. */
public interface StorageRecoveryCommands {
    enum Extent { PIPELINE, CLUSTER }
    enum Phase { PLANNED, CONFIRMED, RUNNING, COMPLETE }
    record Selection(Extent extent, Set<String> pipelineIds) {
        public Selection {
            Objects.requireNonNull(extent, "extent"); pipelineIds = Collections.unmodifiableSet(new TreeSet<>(pipelineIds));
            if (pipelineIds.stream().anyMatch(id -> id == null || id.isBlank())
                    || (extent == Extent.PIPELINE && pipelineIds.isEmpty()) || (extent == Extent.CLUSTER && !pipelineIds.isEmpty()))
                throw new IllegalArgumentException("invalid recovery selection");
        }
    }
    /** The trusted storage inspector supplies the actual closure, never the browser's claimed influence or safety. */
    record Manifest(Selection selection, Set<String> affectedPipelineIds, Map<String,String> definitionHashes,
            String dependencyHash, long configurationRevision) {
        public Manifest {
            Objects.requireNonNull(selection, "selection");
            affectedPipelineIds = Collections.unmodifiableSet(new TreeSet<>(affectedPipelineIds));
            definitionHashes = Collections.unmodifiableMap(new TreeMap<>(definitionHashes));
            if (affectedPipelineIds.stream().anyMatch(String::isBlank) || !affectedPipelineIds.containsAll(selection.pipelineIds())
                    || !definitionHashes.keySet().containsAll(affectedPipelineIds)
                    || definitionHashes.entrySet().stream().anyMatch(entry -> entry.getKey().isBlank() || !hash(entry.getValue()))
                    || !hash(dependencyHash) || configurationRevision < 0 || configurationRevision >= Long.MAX_VALUE-4)
                throw new IllegalArgumentException("invalid recovery manifest");
        }
    }
    record Plan(String operationId, String previousOperationId, CacheAuthScope originalScope, Manifest manifest,
            String planHash, long controlRevision, Phase phase) {
        public Plan {
            Objects.requireNonNull(originalScope, "originalScope"); Objects.requireNonNull(manifest, "manifest"); Objects.requireNonNull(phase, "phase");
            if (!UUID.fromString(operationId).toString().equals(operationId)
                    || (previousOperationId != null && (!UUID.fromString(previousOperationId).toString().equals(previousOperationId) || previousOperationId.equals(operationId)))
                    || !hash(planHash)
                    || controlRevision != manifest.configurationRevision()+1 || controlRevision >= Long.MAX_VALUE)
                throw new IllegalArgumentException("invalid recovery plan");
        }
    }
    StorageRecoveryCommands guardedBy(Runnable requireActive);
    /** Returns the same current plan when its actual closure is unchanged; changed scope requires a new confirmation. */
    Plan plan(CacheAuthScope current, Manifest verified, String operationId, String previousOperationId);
    Optional<Plan> current(CacheAuthScope current);
    Optional<Plan> find(CacheAuthScope current, String originalId);
    /** Exact original confirmation only. Publication grants no business execution or cache initialization. */
    Plan confirm(Plan original);
    /** Internal runtime entry, after original writers are isolated; no frontend exposes this transition. */
    Plan start(Plan confirmed);
    /** Internal runtime completion, guarded by verified cache, execution and target results. */
    Plan complete(Plan running);
    private static boolean hash(String value) { return value != null && value.matches("[0-9a-f]{64}"); }
}
