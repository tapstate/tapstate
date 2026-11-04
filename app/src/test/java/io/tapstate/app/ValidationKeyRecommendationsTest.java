package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceTable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ValidationKeyRecommendationsTest {
    private static final ValidationKeyRecommendations.Limits LIMITS = ValidationKeyRecommendations.Limits.defaults();
    private static final ValidationKey.Kind NUMBER = ValidationKey.Kind.EXACT_NUMBER;
    private static final ValidationKey.Kind TEXT = ValidationKey.Kind.TEXT;

    @Test
    void explicitTargetWriteKeyWinsAndCanRecommendASourceUniqueIndexAheadOfItsPrimaryKey() {
        var source = table("source", List.of("id", "order_no"), List.of("id"),
                List.of(new SourceIndex("unique_order", List.of("order_no"), true)));
        var target = table("target", List.of("_id", "order_no"), List.of("_id"), List.of());
        var result = recommend(source, Map.of("id", NUMBER, "order_no", TEXT), target,
                Map.of("_id", TEXT, "order_no", TEXT), Optional.of(write("target", List.of("order_no"))));
        var proposal = result.proposal().orElseThrow();
        assertThat(proposal.target().origin()).isEqualTo(ValidationKeyRecommendations.Origin.PIPELINE_WRITE);
        assertThat(proposal.target().fields()).containsExactly("order_no");
        assertThat(proposal.source().origin()).isEqualTo(ValidationKeyRecommendations.Origin.UNIQUE_INDEX);
        assertThat(proposal.mapping()).containsExactly(new ValidationKeyRecommendations.FieldPair("order_no", "order_no"));
        assertThat(proposal.basis()).isEqualTo(ValidationKeyRecommendations.Basis.SAME_NAMES_AND_DOMAINS);
        assertThat(proposal.source().issues()).contains(ValidationKeyRecommendations.Issue.INDEX_COVERAGE_REQUIRED);
        assertThat(source.primaryKey()).containsExactly("id");
        assertThat(target.primaryKey()).containsExactly("_id");
    }

    @Test
    void differentNamesRequireManualCorrespondenceAndNeverPairByPosition() {
        var result = recommend(table("source", List.of("id"), List.of("id"), List.of()), Map.of("id", NUMBER),
                table("target", List.of("orderNo"), List.of("orderNo"), List.of()), Map.of("orderNo", NUMBER),
                Optional.of(write("target", List.of("orderNo"))));
        assertThat(result.proposal().orElseThrow().mapping()).isEmpty();
        assertThat(result.proposal().orElseThrow().basis()).isEqualTo(ValidationKeyRecommendations.Basis.MANUAL_MAPPING_REQUIRED);
    }

    @Test
    void anUnknownWriteModelCannotFallBackToAnApparentlyMatchingTargetPrimaryKey() {
        var source = table("source", List.of("id"), List.of("id"), List.of());
        var target = table("target", List.of("id"), List.of("id"), List.of());
        var result = recommend(source, Map.of("id", NUMBER), target, Map.of("id", NUMBER), Optional.empty());
        assertThat(result.targetCandidates()).hasSize(1);
        assertThat(result.proposal()).isEmpty();
        assertThat(result.pending()).containsExactly(ValidationKeyRecommendations.Pending.PIPELINE_WRITE_KEY);
    }

    @Test
    void missingWriteKeyFieldsDoNotSilentlyChooseAnotherTargetIndex() {
        var result = recommend(table("source", List.of("id"), List.of("id"), List.of()), Map.of("id", NUMBER),
                table("target", List.of("id"), List.of("id"), List.of()), Map.of("id", NUMBER),
                Optional.of(write("target", List.of("missing"))));
        var proposal = result.proposal().orElseThrow();
        assertThat(proposal.target().origin()).isEqualTo(ValidationKeyRecommendations.Origin.PIPELINE_WRITE);
        assertThat(proposal.target().issues()).contains(ValidationKeyRecommendations.Issue.MISSING_FIELD,
                ValidationKeyRecommendations.Issue.KEY_RULE_REQUIRED);
        assertThat(proposal.mapping()).isEmpty();
        assertThat(proposal.basis()).isEqualTo(ValidationKeyRecommendations.Basis.MODEL_INFORMATION_REQUIRED);
    }

    @Test
    void compositeSameNamesMatchInSourceOrderRatherThanTargetIndexPosition() {
        var source = table("source", List.of("a", "literal.b"), List.of("a", "literal.b"), List.of());
        var target = table("target", List.of("a", "literal.b"), List.of("literal.b", "a"), List.of());
        var domains = Map.of("a", NUMBER, "literal.b", TEXT);
        var result = recommend(source, domains, target, domains, Optional.of(write("target", List.of())));
        assertThat(result.proposal().orElseThrow().mapping()).containsExactly(
                new ValidationKeyRecommendations.FieldPair("a", "a"), new ValidationKeyRecommendations.FieldPair("literal.b", "literal.b"));
        assertThat(target.primaryKey()).containsExactly("literal.b", "a");
    }

    @Test
    void coarseTypesDoNotInventLogicalKeyRulesAndIncompatibleDomainsStayDistinct() {
        var source = table("source", List.of("id"), List.of("id"), List.of());
        var target = table("target", List.of("id"), List.of("id"), List.of());
        var unknown = recommend(source, Map.of(), target, Map.of(), Optional.of(write("target", List.of())));
        assertThat(unknown.proposal().orElseThrow().basis()).isEqualTo(ValidationKeyRecommendations.Basis.MODEL_INFORMATION_REQUIRED);
        assertThat(unknown.proposal().orElseThrow().mapping()).isEmpty();
        var incompatible = recommend(source, Map.of("id", NUMBER), target, Map.of("id", TEXT), Optional.of(write("target", List.of())));
        assertThat(incompatible.proposal().orElseThrow().basis()).isEqualTo(ValidationKeyRecommendations.Basis.INCOMPATIBLE_DOMAINS);
        assertThat(incompatible.proposal().orElseThrow().mapping()).isEmpty();
    }

    @Test
    void differentComponentCountsAreExplainedWithoutSuggestingAPartialPairing() {
        var source = table("source", List.of("a", "b"), List.of("a", "b"), List.of());
        var target = table("target", List.of("a"), List.of("a"), List.of());
        var result = recommend(source, Map.of("a", NUMBER, "b", NUMBER), target, Map.of("a", NUMBER),
                Optional.of(write("target", List.of())));
        assertThat(result.proposal().orElseThrow().basis()).isEqualTo(ValidationKeyRecommendations.Basis.COMPONENT_COUNT_MISMATCH);
        assertThat(result.proposal().orElseThrow().mapping()).isEmpty();
    }

    @Test
    void indexOrderIsStableAndNonUniqueIndexesAreNotCandidates() {
        var indexes = List.of(new SourceIndex("z", List.of("b"), true), new SourceIndex("ordinary", List.of("a"), false),
                new SourceIndex("a", List.of("a"), true));
        var source = table("source", List.of("a", "b"), List.of(), indexes);
        var target = table("target", List.of("a"), List.of("a"), List.of());
        var first = recommend(source, Map.of("a", NUMBER, "b", TEXT), target, Map.of("a", NUMBER), Optional.of(write("target", List.of())));
        var reordered = table("source", source.fields().stream().map(SourceField::name).toList(), List.of(), indexes.reversed());
        var second = recommend(reordered, Map.of("a", NUMBER, "b", TEXT), target, Map.of("a", NUMBER), Optional.of(write("target", List.of())));
        assertThat(first.sourceCandidates()).extracting(candidate -> candidate.indexName().orElseThrow()).containsExactly("a", "z");
        assertThat(second.proposal()).isEqualTo(first.proposal());
    }

    @Test
    void emptyOrDuplicateDeclaredKeysRemainVisibleAsUnusableShapes() {
        var source = table("source", List.of("id"), List.of("id", "id"), List.of());
        var target = table("target", List.of("id"), List.of("id"), List.of());
        var result = recommend(source, Map.of("id", NUMBER), target, Map.of("id", NUMBER), Optional.of(write("target", List.of())));
        assertThat(result.proposal().orElseThrow().basis()).isEqualTo(ValidationKeyRecommendations.Basis.UNSUPPORTED_KEY_SHAPE);
        assertThat(result.proposal().orElseThrow().source().issues()).contains(ValidationKeyRecommendations.Issue.DUPLICATE_COMPONENT);
        var empty = recommend(table("source", List.of("id"), List.of(), List.of()), Map.of("id", NUMBER),
                target, Map.of("id", NUMBER), Optional.of(write("target", List.of())));
        assertThat(empty.sourceCandidates()).isEmpty(); assertThat(empty.proposal()).isEmpty();
    }

    @Test
    void anOverwideCompoundKeyIsReportedInsteadOfBeingTruncatedIntoASuggestion() {
        var names = java.util.stream.IntStream.range(0, ValidationKey.MAX_COMPONENTS + 1)
                .mapToObj(i -> "part_" + i).toList();
        var domains = names.stream().collect(java.util.stream.Collectors.toMap(name -> name, name -> NUMBER));
        var source = table("source", names, names, List.of());
        var target = table("target", names, names, List.of());
        var result = recommend(source, domains, target, domains, Optional.of(write("target", names)));
        var proposal = result.proposal().orElseThrow();
        assertThat(proposal.source().fields()).hasSize(ValidationKey.MAX_COMPONENTS + 1);
        assertThat(proposal.source().issues()).contains(ValidationKeyRecommendations.Issue.UNSUPPORTED_ARITY);
        assertThat(proposal.target().origin()).isEqualTo(ValidationKeyRecommendations.Origin.PIPELINE_WRITE);
        assertThat(proposal.basis()).isEqualTo(ValidationKeyRecommendations.Basis.UNSUPPORTED_KEY_SHAPE);
        assertThat(proposal.mapping()).isEmpty();
    }

    @Test
    void nonUniqueIndexInventoryStillConsumesTheBoundedMetadataEnvelope() {
        var indexes = java.util.stream.IntStream.range(0, 256)
                .mapToObj(i -> new SourceIndex("non_unique_" + i, List.of("id"), false)).toList();
        var source = table("source", List.of("id"), List.of("id"), indexes);
        var target = table("target", List.of("id"), List.of("id"), List.of());
        assertThatThrownBy(() -> ValidationKeyRecommendations.recommend("mapping", source, Map.of("id", NUMBER), target,
                Map.of("id", NUMBER), Optional.of(write("target", List.of())),
                new ValidationKeyRecommendations.Limits(10, 512, 10, 100, 8192)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("keyRecommendations.bytes");
    }

    @Test
    void boundedCandidateMetadataFailsWithoutReturningAPartialRecommendation() {
        var source = table("source", List.of("id"), List.of("id"), List.of());
        var target = table("target", List.of("id"), List.of("id"), List.of());
        for (var limits : List.of(new ValidationKeyRecommendations.Limits(10, 10, 1, 10, 65536),
                new ValidationKeyRecommendations.Limits(10, 10, 10, 1, 65536),
                new ValidationKeyRecommendations.Limits(10, 10, 10, 10, 512))) {
            assertThatThrownBy(() -> ValidationKeyRecommendations.recommend("mapping", source, Map.of("id", NUMBER), target,
                    Map.of("id", NUMBER), Optional.of(write("target", List.of())), limits))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("validation.value-limit-exceeded");
        }
        var report = recommend(source, Map.of("id", NUMBER), target, Map.of("id", NUMBER), Optional.of(write("target", List.of())));
        assertThat(report.accountedMetadataBytes()).isBetween(512L, LIMITS.bytes());
        assertThatThrownBy(() -> report.sourceCandidates().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> recommend(source, Map.of("id", NUMBER), target, Map.of("id", NUMBER), Optional.of(write("wrong-table", List.of("id")))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("keyRecommendations.writeTarget");
    }

    private static ValidationKeyRecommendations.Report recommend(SourceTable source, Map<String, ValidationKey.Kind> sourceDomains,
            SourceTable target, Map<String, ValidationKey.Kind> targetDomains, Optional<TargetTable> write) {
        return ValidationKeyRecommendations.recommend("mapping", source, sourceDomains, target, targetDomains, write, LIMITS);
    }
    private static SourceTable table(String name, List<String> fields, List<String> key, List<SourceIndex> indexes) {
        return new SourceTable(name, fields.stream().map(field -> new SourceField(field, "native-spelling", TapstateType.STRING)).toList(), key, indexes);
    }
    private static TargetTable write(String name, List<String> keys) {
        return new TargetTable(name, keys.stream().map(field -> new TargetField(field, "native-spelling", true)).toList());
    }
}
