package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.tapstate.core.validation.ValidationCheckEvent.Participation;

/**
 * A completed local root observation, not a confirmed difference, publication permit or repair permit.
 * The checker finishes only after its independent reads and every applicable obligation settle.
 * Read timing and agreement here do not establish snapshot consistency or strict confirmation.
 */
public final class ValidationRootCheck {
    public enum Check { IDENTITY, PARTICIPATION, STRUCTURE, VALUES }
    public enum Status { COMPLETE, BLOCKED, NOT_APPLICABLE }
    public enum TargetPresence { PRESENT, ABSENT, NOT_READ }
    public enum Side { SOURCE, TARGET }
    public enum Kind {
        TARGET_MISSING, TARGET_EXTRA, TARGET_DUPLICATE, TARGET_ROW_DUPLICATE, TARGET_IDENTITY, TARGET_STRUCTURE, TARGET_VALUE, TARGET_TYPE,
        SOURCE_DUPLICATE, SOURCE_ROW_DUPLICATE, SOURCE_CARDINALITY, SOURCE_REFERENCE_MISSING, SOURCE_FIELD_MISSING,
        SOURCE_FIELD_CONFLICT, SOURCE_FIELD_INVALID;

        public boolean sourceProblem() {
            return switch (this) {
                case SOURCE_DUPLICATE, SOURCE_ROW_DUPLICATE, SOURCE_CARDINALITY, SOURCE_REFERENCE_MISSING, SOURCE_FIELD_MISSING,
                        SOURCE_FIELD_CONFLICT, SOURCE_FIELD_INVALID -> true;
                default -> false;
            };
        }
        private boolean valueCheck() { return this == TARGET_VALUE || this == TARGET_TYPE; }
    }

    /** Mandatory scopes have a hard bound; diagnostic sample exhaustion alone never stops a check. */
    public record Limits(long bytes, int findings, int blockedScopes, int reads, int samples) {
        public Limits {
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || findings < 1 || findings > 1024
                    || blockedScopes < 1 || blockedScopes > 1024 || reads < 1 || reads > 1024
                    || samples < 0 || samples > 256) throw ValidationError.invalid("rootCheck.limits");
        }
    }

    /** Full ancestor identity is separate from a field path, whose integer segments are ordinals. */
    public record Location(ValidationRoot scope, List<Object> fieldPath) {
        public Location {
            Objects.requireNonNull(scope, "scope");
            if (fieldPath == null || fieldPath.size() > 64) throw ValidationError.invalid("rootCheck.path");
            for (Object segment : fieldPath) {
                if (segment instanceof String text) ValidationError.fieldName(text, "rootCheck.path");
                else if (!(segment instanceof Integer ordinal) || ordinal < 0) {
                    throw ValidationError.invalid("rootCheck.path");
                }
            }
            fieldPath = List.copyOf(fieldPath);
        }
        public boolean wholeRoot() { return scope.ancestors().isEmpty() && fieldPath.isEmpty(); }
    }

    /** A controlled early count may prove at least two, without pretending to have an exact total. */
    public record Multiplicity(long occurrences, boolean exact) {
        public Multiplicity {
            if (occurrences < 2) throw ValidationError.invalid("rootCheck.multiplicity");
        }
    }

    /** Compact finding metadata is mandatory even when its row sample is omitted. */
    public record Finding(Kind kind, Location location, Optional<Multiplicity> multiplicity) {
        public Finding {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(location, "location");
            Objects.requireNonNull(multiplicity, "multiplicity");
            boolean repeated = kind == Kind.SOURCE_DUPLICATE || kind == Kind.SOURCE_ROW_DUPLICATE || kind == Kind.SOURCE_CARDINALITY
                    || kind == Kind.TARGET_DUPLICATE || kind == Kind.TARGET_ROW_DUPLICATE;
            if (repeated != multiplicity.isPresent()) throw ValidationError.invalid("rootCheck.findingMultiplicity");
        }
        public Finding(Kind kind, Location location) { this(kind, location, Optional.empty()); }
    }

    public record Blocked(Check check, Location location, Finding cause) {
        public Blocked {
            Objects.requireNonNull(check, "check");
            Objects.requireNonNull(location, "location");
            Objects.requireNonNull(cause, "cause");
            if (!cause.kind().sourceProblem()) throw ValidationError.invalid("rootCheck.blockedCause");
        }
    }

    /** A known target structural difference makes only its dependent checks inapplicable. */
    public record Skipped(Check check, Location location, Finding cause) {
        public Skipped {
            Objects.requireNonNull(check, "check");
            Objects.requireNonNull(location, "location");
            Objects.requireNonNull(cause, "cause");
            if (check != Check.STRUCTURE && check != Check.VALUES || cause.kind().sourceProblem()
                    || cause.kind() == Kind.TARGET_VALUE || cause.kind() == Kind.TARGET_IDENTITY) {
                throw ValidationError.invalid("rootCheck.skippedCause");
            }
        }
    }

    /** Actual read identity is not reconstructed from the logical root or a transformed sample field. */
    public record SampledSide(ValidationPhysicalIdentity identity, ValidationValues.Row row, String readId) {
        public SampledSide {
            Objects.requireNonNull(identity, "identity");
            Objects.requireNonNull(row, "row");
            readId = ValidationError.identifier(readId, "rootCheck.sampleReadId");
        }
        public long accountedBytes() { return 128L + identity.retainedBytes() + row.accountedBytes() + textBytes(readId); }
    }

    /** Empty means a sample was not retained, never that a row or a field is NULL or absent. */
    public record Sample(Finding finding, Optional<SampledSide> source, Optional<SampledSide> target) {
        public Sample {
            Objects.requireNonNull(finding, "finding");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
            if (source.isEmpty() && target.isEmpty()) throw ValidationError.invalid("rootCheck.emptySample");
        }
    }

    public record Read(Side side, ValidationCheckEvent.ReadBoundary boundary) {
        public Read {
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(boundary, "boundary");
        }
    }

    private final ValidationObservation observation;
    private final ValidationMethod method;
    private final Participation participation;
    private final TargetPresence targetPresence;
    private final Map<Check, Status> checks;
    private final Map<Finding, Long> findings;
    private final Map<Kind, Long> counts;
    private final List<Blocked> notCompared;
    private final List<Skipped> notApplicableScopes;
    private final List<Read> reads;
    private final List<Sample> samples;
    private final boolean samplesTruncated;
    private final long accountedBytes;

    private ValidationRootCheck(Builder builder) {
        observation = builder.observation;
        method = builder.method;
        participation = builder.participation;
        targetPresence = builder.targetPresence;
        checks = Collections.unmodifiableMap(new EnumMap<>(builder.checks));
        findings = Collections.unmodifiableMap(new LinkedHashMap<>(builder.findings));
        counts = Collections.unmodifiableMap(new EnumMap<>(builder.counts));
        notCompared = List.copyOf(builder.blocked);
        notApplicableScopes = List.copyOf(builder.skipped);
        reads = List.copyOf(builder.reads.values());
        samples = List.copyOf(builder.samples);
        samplesTruncated = builder.samplesTruncated;
        accountedBytes = builder.bytes;
    }

    public ValidationObservation observation() { return observation; }
    public ValidationMethod method() { return method; }
    public Participation participation() { return participation; }
    public TargetPresence targetPresence() { return targetPresence; }
    public Map<Check, Status> checks() { return checks; }
    public Map<Finding, Long> findings() { return findings; }
    public Map<Kind, Long> counts() { return counts; }
    public List<Blocked> notCompared() { return notCompared; }
    public List<Skipped> notApplicableScopes() { return notApplicableScopes; }
    public List<Read> reads() { return reads; }
    public List<Sample> samples() { return samples; }
    public boolean samplesTruncated() { return samplesTruncated; }
    public long accountedBytes() { return accountedBytes; }
    public boolean observedAgreement() { return findings.isEmpty() && notCompared.isEmpty(); }

    public static Builder builder(ValidationObservation observation, ValidationMethod method,
                                  Participation participation, TargetPresence targetPresence, Limits limits) {
        return new Builder(observation, method, participation, targetPresence, limits);
    }

    /** Thread-confined staging. Closing discards partial facts; any failure prevents subsequent finish. */
    public static final class Builder implements AutoCloseable {
        private final ValidationObservation observation;
        private final ValidationMethod method;
        private final Participation participation;
        private final TargetPresence targetPresence;
        private final Limits limits;
        private final EnumMap<Check, Status> checks = new EnumMap<>(Check.class);
        private final Map<Finding, Long> findings = new LinkedHashMap<>();
        private final EnumMap<Kind, Long> counts = new EnumMap<>(Kind.class);
        private final Set<Blocked> blocked = new LinkedHashSet<>();
        private final Set<Skipped> skipped = new LinkedHashSet<>();
        private final Map<ReadKey, Read> reads = new LinkedHashMap<>();
        private final List<Sample> samples = new ArrayList<>();
        private final ValidationFailureLatch failure = new ValidationFailureLatch();
        private long bytes;
        private boolean samplesTruncated;
        private boolean closed;

        private Builder(ValidationObservation observation, ValidationMethod method, Participation participation,
                        TargetPresence targetPresence, Limits limits) {
            this.observation = Objects.requireNonNull(observation, "observation");
            this.method = Objects.requireNonNull(method, "method");
            this.participation = Objects.requireNonNull(participation, "participation");
            this.targetPresence = Objects.requireNonNull(targetPresence, "targetPresence");
            this.limits = Objects.requireNonNull(limits, "limits");
            if (!observation.root().ancestors().isEmpty() || method == ValidationMethod.COUNT
                    || !method.supports(observation.run().mode())) throw ValidationError.invalid("rootCheck.scope");
            // Fixed identity strings are bounded independently; include them in the local retained charge.
            charge(16 * 1024L + locationBytes(new Location(observation.root(), List.of())));
            if (method == ValidationMethod.UNIQUE_KEY) checks.put(Check.VALUES, Status.NOT_APPLICABLE);
        }

        public ValidationObservation observation() { return observation; }
        public ValidationMethod method() { return method; }
        public Participation participation() { return participation; }
        public TargetPresence targetPresence() { return targetPresence; }

        public Builder checked(Check check) {
            return mutate(() -> {
                Objects.requireNonNull(check, "check");
                if (checks.get(check) == Status.NOT_APPLICABLE) throw ValidationError.invalid("rootCheck.check");
                checks.putIfAbsent(check, Status.COMPLETE);
            });
        }

        public Builder notApplicable(Check check) {
            return mutate(() -> {
                if (check != Check.STRUCTURE && check != Check.VALUES || checks.containsKey(check)
                        && checks.get(check) != Status.NOT_APPLICABLE) throw ValidationError.invalid("rootCheck.notApplicable");
                checks.put(check, Status.NOT_APPLICABLE);
            });
        }

        public Builder read(Side side, ValidationCheckEvent.ReadBoundary boundary) {
            return mutate(() -> {
                Read read = new Read(side, boundary);
                ReadKey key = new ReadKey(side, boundary.readId());
                Read old = reads.get(key);
                if (old != null) {
                    if (!old.equals(read)) throw ValidationError.invalid("rootCheck.readId");
                    return;
                }
                if (reads.size() == limits.reads()) throw limit("reads");
                charge(256L + textBytes(boundary.readId()) + textBytes(boundary.contractVersion()));
                reads.put(key, read);
            });
        }

        /** Each call records one observed finding, not one confirmed root or one duplicate row. */
        public Builder finding(Finding finding, Optional<SampledSide> source, Optional<SampledSide> target) {
            return mutate(() -> {
                Objects.requireNonNull(finding, "finding");
                Objects.requireNonNull(source, "source");
                Objects.requireNonNull(target, "target");
                sameRoot(finding.location());
                source.ifPresent(side -> sampleRead(Side.SOURCE, side));
                target.ifPresent(side -> sampleRead(Side.TARGET, side));
                if (method == ValidationMethod.UNIQUE_KEY && finding.kind().valueCheck()) {
                    throw ValidationError.invalid("rootCheck.method");
                }
                if (!findings.containsKey(finding)) {
                    if (findings.size() == limits.findings()) throw limit("findings");
                    charge(256L + locationBytes(finding.location()));
                }
                findings.put(finding, increment(findings.getOrDefault(finding, 0L)));
                counts.put(finding.kind(), increment(counts.getOrDefault(finding.kind(), 0L)));
                if (source.isEmpty() && target.isEmpty()) return;
                if (samples.size() == limits.samples()) { samplesTruncated = true; return; }
                charge(128L + source.map(SampledSide::accountedBytes).orElse(0L)
                        + target.map(SampledSide::accountedBytes).orElse(0L));
                samples.add(new Sample(finding, source, target));
            });
        }

        public Builder finding(Finding finding) { return finding(finding, Optional.empty(), Optional.empty()); }

        public Builder notCompared(Check check, Location location, Finding cause) {
            return mutate(() -> {
                Blocked item = new Blocked(check, location, cause);
                sameRoot(location);
                sameRoot(cause.location());
                if (!findings.containsKey(cause) || checks.get(check) == Status.NOT_APPLICABLE) {
                    throw ValidationError.invalid("rootCheck.blockedCause");
                }
                if (blocked.contains(item)) return;
                if (blocked.size() + skipped.size() == limits.blockedScopes()) throw limit("blockedScopes");
                charge(128L + locationBytes(location) + locationBytes(cause.location()));
                blocked.add(item);
                checks.put(check, Status.BLOCKED);
            });
        }

        public Builder notApplicable(Check check, Location location, Finding cause) {
            return mutate(() -> {
                Skipped item = new Skipped(check, location, cause);
                sameRoot(location);
                sameRoot(cause.location());
                if (!findings.containsKey(cause) || method == ValidationMethod.UNIQUE_KEY && check == Check.VALUES) {
                    throw ValidationError.invalid("rootCheck.skippedCause");
                }
                if (skipped.contains(item)) return;
                if (blocked.size() + skipped.size() == limits.blockedScopes()) throw limit("blockedScopes");
                charge(128L + locationBytes(location) + locationBytes(cause.location()));
                skipped.add(item);
            });
        }

        public ValidationRootCheck finish() {
            ensureOpen();
            try {
                validateComplete();
                ValidationRootCheck result = new ValidationRootCheck(this);
                close();
                return result;
            } catch (RuntimeException | Error error) {
                fail(error);
                throw error;
            }
        }

        private void validateComplete() {
            if (checks.size() != Check.values().length || reads.values().stream().noneMatch(r -> r.side() == Side.SOURCE)) {
                throw ValidationError.invalid("rootCheck.incomplete");
            }
            boolean targetRead = reads.values().stream().anyMatch(r -> r.side() == Side.TARGET);
            if (targetPresence == TargetPresence.NOT_READ) {
                if (targetRead || participation != Participation.UNKNOWN || checks.get(Check.STRUCTURE) != Status.BLOCKED
                        || method == ValidationMethod.FULL_COLUMN && checks.get(Check.VALUES) != Status.BLOCKED) {
                    throw ValidationError.invalid("rootCheck.targetRead");
                }
            } else if (!targetRead) throw ValidationError.invalid("rootCheck.targetRead");
            if (participation == Participation.UNKNOWN) {
                if (checks.get(Check.PARTICIPATION) != Status.BLOCKED || checks.get(Check.STRUCTURE) != Status.BLOCKED
                        || method == ValidationMethod.FULL_COLUMN && checks.get(Check.VALUES) != Status.BLOCKED
                        || findings.keySet().stream().anyMatch(f -> !f.kind().sourceProblem())) {
                    throw ValidationError.invalid("rootCheck.unknownParticipation");
                }
            } else if (checks.get(Check.PARTICIPATION) != Status.COMPLETE) {
                throw ValidationError.invalid("rootCheck.participation");
            }
            boolean compareContent = participation == Participation.SHOULD_EXIST && targetPresence == TargetPresence.PRESENT;
            if (checks.get(Check.STRUCTURE) == Status.NOT_APPLICABLE && (compareContent || participation == Participation.UNKNOWN)
                    || method == ValidationMethod.FULL_COLUMN && checks.get(Check.VALUES) == Status.NOT_APPLICABLE
                    && (compareContent || participation == Participation.UNKNOWN)) {
                throw ValidationError.invalid("rootCheck.contentChecks");
            }
            if (!compareContent && findings.keySet().stream().anyMatch(f -> !f.kind().sourceProblem()
                    && !(f.location().wholeRoot() && (f.kind() == Kind.TARGET_MISSING || f.kind() == Kind.TARGET_EXTRA
                    || f.kind() == Kind.TARGET_DUPLICATE && targetPresence == TargetPresence.PRESENT)))) {
                throw ValidationError.invalid("rootCheck.contentFinding");
            }
            boolean missing = wholeRootFinding(Kind.TARGET_MISSING);
            boolean extra = wholeRootFinding(Kind.TARGET_EXTRA);
            if (missing != (participation == Participation.SHOULD_EXIST && targetPresence == TargetPresence.ABSENT)
                    || extra != (participation == Participation.SHOULD_NOT_EXIST && targetPresence == TargetPresence.PRESENT)) {
                throw ValidationError.invalid("rootCheck.presenceFinding");
            }
        }

        Builder requireSampleRead(Side side, SampledSide sample) {
            return mutate(() -> sampleRead(Objects.requireNonNull(side, "side"), Objects.requireNonNull(sample, "sample")));
        }

        private void sampleRead(Side side, SampledSide sample) {
            if (!reads.containsKey(new ReadKey(side, sample.readId()))) throw ValidationError.invalid("rootCheck.sampleRead");
        }

        private boolean wholeRootFinding(Kind kind) {
            return findings.keySet().stream().anyMatch(f -> f.kind() == kind && f.location().wholeRoot());
        }

        private Builder mutate(Runnable action) {
            ensureOpen();
            try { action.run(); return this; }
            catch (RuntimeException | Error error) { fail(error); throw error; }
        }
        private void sameRoot(Location location) {
            ValidationRoot expected = observation.root();
            ValidationRoot actual = location.scope();
            if (!expected.mappingId().equals(actual.mappingId()) || !expected.outputId().equals(actual.outputId())
                    || !expected.rootKey().equals(actual.rootKey())) throw ValidationError.invalid("rootCheck.foreignRoot");
        }
        private void charge(long amount) {
            if (amount > limits.bytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
        private static long increment(long count) {
            if (count == Long.MAX_VALUE) throw limit("findingCount");
            return count + 1;
        }
        private void ensureOpen() {
            Throwable cause = failure.get();
            if (cause instanceof Error error) throw error;
            if (cause instanceof RuntimeException error) throw error;
            if (closed) throw new IllegalStateException("Root check staging is closed");
        }
        private void fail(Throwable cause) { failure.record(cause); close(); }
        @Override public void close() {
            closed = true;
            checks.clear(); findings.clear(); counts.clear(); blocked.clear(); skipped.clear(); reads.clear(); samples.clear();
        }
        private record ReadKey(Side side, String id) {}
    }

    private static long textBytes(String text) { return 32L + 2L * text.length(); }
    private static long locationBytes(Location location) {
        ValidationRoot root = location.scope();
        long bytes = 128L + textBytes(root.mappingId()) + textBytes(root.outputId()) + root.rootKey().retainedBytes();
        for (ValidationRoot.Ancestor ancestor : root.ancestors()) {
            bytes += 64L + ancestor.key().retainedBytes();
            for (String segment : ancestor.fieldPath()) bytes += 8L + textBytes(segment);
        }
        for (Object segment : location.fieldPath()) bytes += segment instanceof String text ? 8L + textBytes(text) : 24L;
        return bytes;
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "rootCheck." + dimension), null);
    }
}
