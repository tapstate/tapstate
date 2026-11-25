package io.tapstate.core.validation;

import java.util.List;
import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Bounded timing metadata. Native positions are opaque and are never recovery offsets or ACKs. */
public final class ValidationTiming {
    private ValidationTiming() {}

    /** The production execution is distinct from the validation owner's tenure. */
    public record Execution(String clusterId, String pipelineId, String definitionRevision,
                            String executionId, long generation) {
        public Execution {
            clusterId = id(clusterId, "cluster"); pipelineId = id(pipelineId, "pipeline");
            definitionRevision = id(definitionRevision, "definition"); executionId = id(executionId, "execution");
            ValidationError.positive(generation, "timing.executionGeneration");
        }
    }

    /** A partitioned source must declare all domains needed by the dependency scope. */
    public record Domain(String sourceId, String captureDomain, String memberId, String bootId,
                         String logEpoch, String semanticsVersion) {
        public Domain {
            sourceId = id(sourceId, "source"); captureDomain = id(captureDomain, "domain");
            memberId = id(memberId, "member"); bootId = id(bootId, "boot");
            logEpoch = id(logEpoch, "epoch"); semanticsVersion = id(semanticsVersion, "semantics");
        }
    }

    public enum Capability {
        CURRENT_COMMITTED_BOUNDARY, NATIVE_PASSED_COMPARISON, COMPLETE_CAPTURE,
        COMPLETE_HINT_DELIVERY, COMPLETE_DEPENDENCY_CHANGES, RELATED_PROCESSING,
        ALL_TARGET_COMMITS, FRESH_COMMITTED_READS
    }
    public record Capabilities(Domain domain, Set<Capability> supported) {
        public Capabilities { Objects.requireNonNull(domain, "domain"); supported = Set.copyOf(supported); }
        public boolean supportsStrictWindow() { return supported.containsAll(Set.of(Capability.values())); }
    }

    /** Registration observes absent/new dependencies as well as existing keys, before sampling L0. */
    public record Scope(ValidationObservation observation, Execution execution, String attemptId,
                        String dependencyScopeId, List<Domain> domains, Instant registeredAt, long windowMillis) {
        public Scope {
            Objects.requireNonNull(observation, "observation"); Objects.requireNonNull(execution, "execution");
            Objects.requireNonNull(registeredAt, "registeredAt");
            attemptId = id(attemptId, "attempt"); dependencyScopeId = id(dependencyScopeId, "dependencies");
            if (!observation.root().ancestors().isEmpty()
                    || !observation.run().owner().clusterId().equals(execution.clusterId())
                    || !observation.run().owner().pipelineId().equals(execution.pipelineId())
                    || domains == null || domains.isEmpty() || domains.size() > 32
                    || windowMillis < 1 || windowMillis > 300_000) throw invalid("scope");
            domains = List.copyOf(domains);
            if (domains.stream().distinct().count() != domains.size()
                    || domains.stream().map(d -> List.of(d.sourceId(), d.captureDomain())).distinct().count() != domains.size()) {
                throw invalid("domains");
            }
        }
    }

    public enum Phase { L0, L1 }
    /** No ordering is defined on tokens; only the native provider may prove a domain has passed one. */
    public record Batch(Scope scope, String batchId, Phase phase, Instant sampledAt, Map<Domain, String> tokens) {
        public Batch {
            Objects.requireNonNull(scope, "scope"); batchId = id(batchId, "batch");
            Objects.requireNonNull(phase, "phase"); Objects.requireNonNull(sampledAt, "sampledAt");
            if (sampledAt.isBefore(scope.registeredAt())
                    || Duration.between(scope.registeredAt(), sampledAt).compareTo(Duration.ofMillis(scope.windowMillis())) > 0) {
                throw invalid("sampleWindow");
            }
            if (tokens == null || !tokens.keySet().equals(Set.copyOf(scope.domains()))) throw invalid("batchDomains");
            tokens.values().forEach(token -> id(token, "nativeBoundary"));
            tokens = Map.copyOf(tokens);
        }
    }

    public enum Milestone { CAPTURED, HINTS_DELIVERED, IMPACTS_SETTLED, PROCESSING_SETTLED, TARGETS_COMMITTED }
    public record Progress(Set<Milestone> completed, boolean relevantChange) {
        public Progress { completed = Set.copyOf(completed); }
    }
    /**
     * evidencedAt is when the provider obtained this evidence, not a guessed source commit time.
     * L0 must be evidenced before S1; late evidence cannot validate an earlier target read.
     * L1 deliberately needs no processing/commit wait, and never waits for this candidate itself.
     */
    public record Coverage(Batch batch, Instant evidencedAt, Map<Domain, Progress> domains) {
        public Coverage {
            Objects.requireNonNull(batch, "batch"); Objects.requireNonNull(evidencedAt, "evidencedAt");
            if (evidencedAt.isBefore(batch.sampledAt())
                    || Duration.between(batch.scope().registeredAt(), evidencedAt).compareTo(Duration.ofMillis(batch.scope().windowMillis())) > 0) {
                throw invalid("coverageWindow");
            }
            if (domains == null || !domains.keySet().equals(batch.tokens().keySet())) throw invalid("coverageDomains");
            domains = Map.copyOf(domains);
        }
        public boolean complete() {
            Set<Milestone> required = batch.phase() == Phase.L0 ? Set.of(Milestone.values())
                    : Set.of(Milestone.CAPTURED, Milestone.HINTS_DELIVERED, Milestone.IMPACTS_SETTLED);
            return domains.values().stream().allMatch(progress -> progress.completed().containsAll(required));
        }
        public boolean changed() { return domains.values().stream().anyMatch(Progress::relevantChange); }
    }

    /**
     * Provider-attested facts, not a durable permission. Callers revalidate current scope, authority,
     * freshness and expiry at use. Read intervals do not themselves attest freshness or equality.
     */
    public record Window(Coverage l0, Coverage l1, ValidationCheckEvent.ReadBoundary sourceBefore,
                         ValidationCheckEvent.ReadBoundary target, ValidationCheckEvent.ReadBoundary sourceAfter,
                         boolean freshCommittedReads, boolean sameSourceFacts) {
        public Window {
            Objects.requireNonNull(l0, "l0"); Objects.requireNonNull(l1, "l1");
            Objects.requireNonNull(sourceBefore, "sourceBefore"); Objects.requireNonNull(target, "target");
            Objects.requireNonNull(sourceAfter, "sourceAfter");
            if (l0.batch().phase() != Phase.L0 || l1.batch().phase() != Phase.L1
                    || !l0.batch().scope().equals(l1.batch().scope())
                    || l0.batch().batchId().equals(l1.batch().batchId())
                    || List.of(sourceBefore.readId(), target.readId(), sourceAfter.readId()).stream().distinct().count() != 3
                    || sourceBefore.startedAt().isBefore(l0.evidencedAt())
                    || sourceAfter.finishedAt().isAfter(l1.batch().sampledAt())
                    || sourceBefore.finishedAt().isAfter(target.startedAt())
                    || target.finishedAt().isAfter(sourceAfter.startedAt())) throw invalid("window");
        }
        public Scope scope() { return l0.batch().scope(); }
        public boolean settled() {
            return l0.complete() && l1.complete() && !l0.changed() && !l1.changed()
                    && freshCommittedReads && sameSourceFacts;
        }
    }

    private static String id(String value, String field) { return ValidationError.identifier(value, "timing." + field); }
    private static io.tapstate.core.common.TapstateException invalid(String field) { return ValidationError.invalid("timing." + field); }
}
