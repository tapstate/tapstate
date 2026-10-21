package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Advisory keys for one fixed table mapping, without lineage, sample reads or implicit confirmation.
 * The supplied domains come from independently evaluated model/codec key rules, not from matching
 * native type strings or sample values. Missing domains remain unknown, including coarse temporal types.
 *
 * <p>The write model is the already resolved model of this output, not a model recomputed here from
 * production transformations. Discovery timestamps do not bind models to configuration versions;
 * callers must establish that provenance separately. No recommendation changes a saved selection,
 * Pipeline write key or database index, nor proves uniqueness, index coverage or native read/order.
 */
public final class ValidationKeyRecommendations {
    private ValidationKeyRecommendations() {}
    public enum Origin { PIPELINE_WRITE, PRIMARY_KEY, UNIQUE_INDEX }
    public enum Issue { MISSING_FIELD, KEY_RULE_REQUIRED, UNSUPPORTED_ARITY, DUPLICATE_COMPONENT, INDEX_COVERAGE_REQUIRED }
    public enum Basis { SAME_NAMES_AND_DOMAINS, MANUAL_MAPPING_REQUIRED, COMPONENT_COUNT_MISMATCH, INCOMPATIBLE_DOMAINS, MODEL_INFORMATION_REQUIRED, UNSUPPORTED_KEY_SHAPE }

    public record Candidate(Origin origin, Optional<String> indexName, List<String> fields,
                            Map<String, ValidationKey.Kind> domains, Set<Issue> issues) {
        public Candidate {
            Objects.requireNonNull(origin, "origin"); Objects.requireNonNull(indexName, "indexName");
            fields = List.copyOf(fields); domains = Map.copyOf(domains); issues = Set.copyOf(issues);
        }
        private boolean knownShape() {
            return !issues.contains(Issue.UNSUPPORTED_ARITY) && !issues.contains(Issue.DUPLICATE_COMPONENT);
        }
        private boolean knownComponents() {
            return knownShape() && !issues.contains(Issue.MISSING_FIELD) && !issues.contains(Issue.KEY_RULE_REQUIRED);
        }
    }
    public enum Pending { PIPELINE_WRITE_KEY }
    /** Only complete, same-name mappings are suggested. Different names never pair by position. */
    public record FieldPair(String source, String target) {}
    public record Proposal(Candidate source, Candidate target, List<FieldPair> mapping, Basis basis) {
        public Proposal { mapping = List.copyOf(mapping); }
    }
    public record Report(String mappingId, String sourceTable, String targetTable, List<Candidate> sourceCandidates,
                         List<Candidate> targetCandidates, Optional<Proposal> proposal, Set<Pending> pending, long accountedMetadataBytes) {
        public Report { sourceCandidates = List.copyOf(sourceCandidates); targetCandidates = List.copyOf(targetCandidates); pending = Set.copyOf(pending); }
    }
    public record Limits(int fieldsPerModel, int indexesPerModel, int candidates, int comparisons, long bytes) {
        public Limits {
            if (fieldsPerModel < 1 || fieldsPerModel > 4096 || indexesPerModel < 1 || indexesPerModel > 1024
                    || candidates < 1 || candidates > 2048 || comparisons < 1 || comparisons > 65536
                    || bytes < 1 || bytes > 16L * 1024 * 1024) throw invalid("keyRecommendations.limits");
        }
        public static Limits defaults() { return new Limits(4096, 128, 256, 16384, 1024 * 1024); }
    }

    /**
     * Inputs are owned, stable, admitted metadata. An absent write model means unknown write keys,
     * not proof that no write key exists; no default pairing is proposed until that fact is supplied.
     * These observations never substitute for action preflight or user confirmation.
     */
    public static Report recommend(String mappingId, SourceTable source, Map<String, ValidationKey.Kind> sourceDomains,
            SourceTable target, Map<String, ValidationKey.Kind> targetDomains, Optional<TargetTable> writeModel, Limits limits) {
        name(mappingId); Objects.requireNonNull(source, "source"); Objects.requireNonNull(target, "target");
        Objects.requireNonNull(sourceDomains, "sourceDomains"); Objects.requireNonNull(targetDomains, "targetDomains");
        Objects.requireNonNull(writeModel, "writeModel"); Objects.requireNonNull(limits, "limits");
        name(source.name()); name(target.name());
        var budget = new Budget(limits);
        budget.charge(512L + 2L * (mappingId.length() + source.name().length() + target.name().length()));
        Set<String> sourceFields = fields(source, sourceDomains, budget);
        Set<String> targetFields = fields(target, targetDomains, budget);
        List<String> writeKey = List.of();
        if (writeModel.isPresent()) {
            TargetTable write = writeModel.orElseThrow();
            if (!write.name().equals(target.name())) throw invalid("keyRecommendations.writeTarget");
            if (write.fields().size() > limits.fieldsPerModel()) throw limit("fields");
            budget.charge(128L + 64L * write.fields().size());
            var seen = new HashSet<String>();
            var key = new ArrayList<String>();
            for (var field : write.fields()) {
                name(field.name());
                if (!seen.add(field.name())) throw invalid("keyRecommendations.duplicateWriteField");
                if (field.primaryKey()) key.add(field.name());
            }
            writeKey = key;
        }
        var sourceCandidates = candidates(source, sourceFields, sourceDomains, List.of(), budget);
        var targetCandidates = candidates(target, targetFields, targetDomains, writeKey, budget);
        Optional<Proposal> proposal = Optional.empty();
        if (writeModel.isPresent() && !sourceCandidates.isEmpty() && !targetCandidates.isEmpty()) {
            Candidate preferredSource = sourceCandidates.getFirst();
            Candidate preferredTarget = targetCandidates.getFirst();
            List<FieldPair> mapping = List.of();
            // Preserve the target write-key priority even when another target index has a better name match.
            for (Candidate candidate : sourceCandidates) {
                if (matches(candidate, preferredTarget, budget)) {
                    preferredSource = candidate;
                    budget.charge(64L + 64L * candidate.fields().size());
                    mapping = candidate.fields().stream().map(field -> new FieldPair(field, field)).toList();
                    break;
                }
            }
            Basis basis = !mapping.isEmpty() ? Basis.SAME_NAMES_AND_DOMAINS
                    : !preferredSource.knownShape() || !preferredTarget.knownShape() ? Basis.UNSUPPORTED_KEY_SHAPE
                    : preferredSource.fields().size() != preferredTarget.fields().size() ? Basis.COMPONENT_COUNT_MISMATCH
                    : !preferredSource.knownComponents() || !preferredTarget.knownComponents() ? Basis.MODEL_INFORMATION_REQUIRED
                    : !sameDomainCounts(preferredSource, preferredTarget, budget) ? Basis.INCOMPATIBLE_DOMAINS
                    : Basis.MANUAL_MAPPING_REQUIRED;
            budget.charge(128);
            proposal = Optional.of(new Proposal(preferredSource, preferredTarget, mapping, basis));
        }
        return new Report(mappingId, source.name(), target.name(), sourceCandidates, targetCandidates, proposal,
                writeModel.isEmpty() ? Set.of(Pending.PIPELINE_WRITE_KEY) : Set.of(), budget.bytes);
    }

    private static Set<String> fields(SourceTable table, Map<String, ValidationKey.Kind> domains, Budget budget) {
        if (table.fields().size() > budget.limits.fieldsPerModel() || domains.size() > budget.limits.fieldsPerModel()) throw limit("fields");
        var fields = new HashSet<String>();
        for (var field : table.fields()) {
            ValidationError.fieldName(field.name(), "keyRecommendations.field");
            budget.charge(96L + 2L * field.name().length());
            if (!fields.add(field.name())) throw invalid("keyRecommendations.duplicateField");
        }
        return fields;
    }

    private static List<Candidate> candidates(SourceTable table, Set<String> fields, Map<String, ValidationKey.Kind> domains,
            List<String> writeKey, Budget budget) {
        if (table.indexes().size() > budget.limits.indexesPerModel()) throw limit("indexes");
        var result = new ArrayList<Candidate>();
        if (!writeKey.isEmpty()) result.add(candidate(Origin.PIPELINE_WRITE, Optional.empty(), writeKey, fields, domains, budget));
        if (!table.primaryKey().isEmpty()) result.add(candidate(Origin.PRIMARY_KEY, Optional.empty(), table.primaryKey(), fields, domains, budget));
        budget.charge(128L + 128L * table.indexes().size());
        var indexes = new ArrayList<SourceIndex>();
        var names = new HashSet<String>();
        for (SourceIndex index : table.indexes()) {
            name(index.name());
            if (!names.add(index.name())) throw invalid("keyRecommendations.duplicateIndex");
            if (index.unique()) indexes.add(index);
        }
        indexes.sort(Comparator.comparing(SourceIndex::name));
        for (SourceIndex index : indexes)
            result.add(candidate(Origin.UNIQUE_INDEX, Optional.of(index.name()), index.fields(), fields, domains, budget));
        return List.copyOf(result);
    }

    private static Candidate candidate(Origin origin, Optional<String> index, List<String> key, Set<String> fields,
            Map<String, ValidationKey.Kind> domains, Budget budget) {
        if (++budget.candidates > budget.limits.candidates()) throw limit("candidates");
        if (key.size() > budget.limits.fieldsPerModel()) throw limit("keyFields");
        budget.charge(256L + index.map(name -> 2L * name.length()).orElse(0L));
        var issues = EnumSet.noneOf(Issue.class);
        if (key.isEmpty() || key.size() > ValidationKey.MAX_COMPONENTS) issues.add(Issue.UNSUPPORTED_ARITY);
        if (origin == Origin.UNIQUE_INDEX) issues.add(Issue.INDEX_COVERAGE_REQUIRED);
        var distinct = new HashSet<String>();
        var knownDomains = new LinkedHashMap<String, ValidationKey.Kind>();
        for (String field : key) {
            name(field); budget.charge(128L + 2L * field.length());
            if (!distinct.add(field)) issues.add(Issue.DUPLICATE_COMPONENT);
            if (!fields.contains(field)) issues.add(Issue.MISSING_FIELD);
            ValidationKey.Kind domain = domains.get(field);
            if (domain == null) issues.add(Issue.KEY_RULE_REQUIRED);
            else knownDomains.put(field, domain);
        }
        return new Candidate(origin, index, key, knownDomains, issues);
    }

    private static boolean matches(Candidate source, Candidate target, Budget budget) {
        budget.compare();
        if (!source.knownComponents() || !target.knownComponents() || source.fields().size() != target.fields().size()) return false;
        for (String field : source.fields()) {
            budget.compare();
            if (source.domains().get(field) != target.domains().get(field)) return false;
        }
        return true;
    }
    private static boolean sameDomainCounts(Candidate source, Candidate target, Budget budget) {
        budget.charge(128L + 32L * (source.fields().size() + target.fields().size()));
        var counts = new java.util.EnumMap<ValidationKey.Kind, Integer>(ValidationKey.Kind.class);
        for (String field : source.fields()) { budget.compare(); counts.merge(source.domains().get(field), 1, Integer::sum); }
        for (String field : target.fields()) { budget.compare(); counts.merge(target.domains().get(field), -1, Integer::sum); }
        return counts.values().stream().allMatch(count -> count == 0);
    }
    private static final class Budget {
        private final Limits limits;
        private long bytes;
        private int candidates;
        private int comparisons;
        private Budget(Limits limits) { this.limits = limits; }
        private void compare() { if (comparisons++ == limits.comparisons()) throw limit("comparisons"); }
        private void charge(long amount) { if (amount > limits.bytes() - bytes) throw limit("bytes"); bytes += amount; }
    }
    private static void name(String value) {
        if (value == null || value.isBlank()) throw invalid("keyRecommendations.name");
        ValidationError.fieldName(value, "keyRecommendations.name");
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String field) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "keyRecommendations." + field), null);
    }
}
