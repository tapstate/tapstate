package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationRunState;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One owner's bounded local problem-root reservations for one pipeline counting generation.
 * Only complete, located problem observations enter here, never count deltas or unresolved candidates.
 * A capacity reservation does not confirm cross-database timing or upgrade an observation's confidence.
 * Guards and publication submission must be bounded, nonblocking local operations. A submission is
 * still fenced by its run/report/observation versions in the result store; this class is not durable
 * authority, a completion receipt, or a replacement for the shared stop latch. On LIMIT_REACHED the
 * controller must seal that generation in shared control, keeping it closed while outcome is unknown.
 * A new owner or counting generation always gets a new instance and new runs, never restored slots.
 */
public final class ValidationProblemQuota {
    public static final int MAX_PROBLEM_ROOTS = 1000;
    public enum State { OPEN, LIMIT_REACHED, CLOSED }
    public enum Join { JOINED, ALREADY_JOINED, MODE_BUSY, CLOSED }
    public enum Admission { RESERVED, EXISTING, LIMIT_REACHED, CLOSED }
    public record Reservation(Admission admission, Optional<PublicationPermit> permit) {}

    /** Opaque local permission; actual store publication still verifies the full durable fence. */
    public static final class PublicationPermit {
        private final ValidationRunIdentity run;
        private final RootScope root;
        private PublicationPermit(ValidationRunIdentity run, RootScope root) { this.run = run; this.root = root; }
    }

    // Descendant findings belong to the same top-level problem root. Their full paths remain in the
    // observation payload, not in the quota key, so sibling issues cannot consume separate root slots.
    private record RootScope(String mapping, String output, ValidationKey key) {
        static RootScope of(ValidationRoot root) { return new RootScope(root.mappingId(), root.outputId(), root.rootKey()); }
        long bytes() { return 128L + 2L * mapping.length() + 2L * output.length() + key.retainedBytes(); }
    }
    private static final class RunSlots {
        private final Runnable requireAuthorized;
        private final Map<RootScope, PublicationPermit> roots = new HashMap<>();
        private long bytes;
        private boolean sealed;
        private RunSlots(Runnable requireAuthorized, long bytes) { this.requireAuthorized = requireAuthorized; this.bytes = bytes; }
    }

    private final ValidationOwner owner;
    private final long generation;
    private final int maxActiveRuns;
    private final long maxMetadataBytes;
    private final Runnable requireOwnerAuthorized;
    private final Map<ValidationRunIdentity, RunSlots> runs = new HashMap<>();
    private State state = State.OPEN;
    private int problems;
    private long metadataBytes;

    public ValidationProblemQuota(ValidationOwner owner, long generation, int maxActiveRuns,
            long maxMetadataBytes, Runnable requireOwnerAuthorized) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.requireOwnerAuthorized = Objects.requireNonNull(requireOwnerAuthorized, "requireOwnerAuthorized");
        if (generation < 1 || maxActiveRuns < 1 || maxActiveRuns > 1024
                || maxMetadataBytes < 1 || maxMetadataBytes > 128L * 1024 * 1024) throw invalid("problemQuota.limits");
        this.generation = generation;
        this.maxActiveRuns = maxActiveRuns;
        this.maxMetadataBytes = maxMetadataBytes;
        metadataBytes = 256L + 2L * (owner.clusterId().length() + owner.pipelineId().length()
                + owner.nodeId().length() + owner.bootId().length());
        requireSpace(0);
    }

    /** Invoke only after shared run admission; the guard must irreversibly reject a retired run. */
    public synchronized Join join(ValidationRunIdentity run, Runnable requireRunAuthorized) {
        identity(run);
        Objects.requireNonNull(requireRunAuthorized, "requireRunAuthorized");
        if (!open()) return Join.CLOSED;
        RunSlots existing = runs.get(run);
        if (existing != null) {
            if (!authorized(existing)) return Join.CLOSED;
            return Join.ALREADY_JOINED;
        }
        for (ValidationRunIdentity active : runs.keySet()) {
            if (active.runId().equals(run.runId())) throw invalid("run.identity");
            if (active.slot().equals(run.slot())) return Join.MODE_BUSY;
        }
        requireRunAuthorized.run();
        if (state != State.OPEN) return Join.CLOSED;
        if (runs.size() == maxActiveRuns) throw limit("activeValidationRuns");
        long bytes = 256L + 2L * (run.taskId().length() + run.runId().length()
                + run.specHash().length() + run.comparisonRulesVersion().length());
        requireSpace(bytes);
        runs.put(run, new RunSlots(requireRunAuthorized, bytes));
        metadataBytes += bytes;
        return Join.JOINED;
    }

    public synchronized Reservation reserve(ValidationRunIdentity run, ValidationRoot root) {
        identity(run);
        Objects.requireNonNull(root, "root");
        if (!open()) return refused();
        RunSlots active = runs.get(run);
        if (active == null || !authorized(active)) return closed();
        RootScope key = RootScope.of(root);
        PublicationPermit existing = active.roots.get(key);
        if (existing != null) return new Reservation(Admission.EXISTING, Optional.of(existing));
        if (problems == MAX_PROBLEM_ROOTS) {
            state = State.LIMIT_REACHED;
            return refused();
        }
        long bytes = key.bytes();
        requireSpace(bytes);
        var permit = new PublicationPermit(run, key);
        active.roots.put(key, permit);
        active.bytes += bytes;
        metadataBytes += bytes;
        problems++;
        return new Reservation(Admission.RESERVED, Optional.of(permit));
    }

    /**
     * Atomically gate a nonblocking submission against local overflow/seal/authority changes.
     * A throw or unknown result does not free the reservation. Previously submitted writes still
     * require durable report sealing before any run can leave this generation.
     */
    public synchronized boolean publish(PublicationPermit permit, Runnable submit) {
        Objects.requireNonNull(permit, "permit");
        Objects.requireNonNull(submit, "submit");
        if (!open()) return false;
        RunSlots active = runs.get(permit.run);
        if (active == null || active.roots.get(permit.root) != permit || !authorized(active)) return false;
        submit.run();
        return true;
    }

    /** Close local publication first. Durable closure/terminal uncertainty must retain all slots. */
    public synchronized void seal(ValidationRunIdentity run) {
        identity(run);
        RunSlots active = runs.get(run);
        if (active != null) active.sealed = true;
    }

    /**
     * Call only after durable result publication is sealed AND control terminal state is known.
     * The caller supplies that external proof; terminal state alone does not establish it here.
     * Repair flags, cleanup, visible result counts or an unknown terminal CAS must never call this.
     */
    public synchronized void retireSealedTerminalRun(ValidationRunIdentity run, ValidationRunState terminal) {
        identity(run);
        Objects.requireNonNull(terminal, "terminal");
        if (!terminal.terminal() || terminal == ValidationRunState.COMPLETED
                && run.mode() == io.tapstate.core.validation.ValidationMode.INCREMENTAL) throw invalid("run.terminal");
        discardSealed(run);
    }

    /** Caller proves irreversible original birth closure and expired stores; this is local disposal, not a terminal claim. */
    public synchronized void discardExpiredSealedRun(ValidationRunIdentity run){identity(run);discardSealed(run);}
    private void discardSealed(ValidationRunIdentity run){
        RunSlots active = runs.get(run);
        if (active == null) return;
        if (!active.sealed) throw new IllegalStateException("local publication must be sealed before retirement");
        runs.remove(run);
        problems -= active.roots.size();
        metadataBytes -= active.bytes;
        // Retirement never unlocks overflow. Empty generations are closed, not reused for later runs.
        if (runs.isEmpty() && state == State.OPEN) state = State.CLOSED;
    }

    /** Owner loss or explicit generation retirement is irreversible; unknown reservations remain. */
    public synchronized void close() { if (state == State.OPEN) state = State.CLOSED; }
    public synchronized State state() { return state; }
    public synchronized int reservedRoots() { return problems; }
    public synchronized long metadataBytes() { return metadataBytes; }
    public synchronized List<ValidationRunIdentity> activeRuns() { return List.copyOf(runs.keySet()); }
    public long generation() { return generation; }
    public ValidationOwner owner() { return owner; }

    private boolean open() {
        if (state != State.OPEN) return false;
        try { requireOwnerAuthorized.run(); }
        catch (RuntimeException | Error lost) { state = State.CLOSED; throw lost; }
        return state == State.OPEN;
    }
    private boolean authorized(RunSlots run) {
        if (run.sealed) return false;
        try { run.requireAuthorized.run(); }
        catch (RuntimeException | Error lost) { run.sealed = true; throw lost; }
        return !run.sealed && state == State.OPEN;
    }
    private void identity(ValidationRunIdentity run) {
        Objects.requireNonNull(run, "run");
        if (!owner.equals(run.owner())) throw new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,
                Map.of("pipeline", owner.pipelineId()), null);
    }
    private void requireSpace(long bytes) {
        if (bytes > maxMetadataBytes - metadataBytes) throw limit("problemMetadataBytes");
    }
    private Reservation refused() {
        return new Reservation(state == State.LIMIT_REACHED ? Admission.LIMIT_REACHED : Admission.CLOSED, Optional.empty());
    }
    private static Reservation closed() { return new Reservation(Admission.CLOSED, Optional.empty()); }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", dimension), null);
    }
}
