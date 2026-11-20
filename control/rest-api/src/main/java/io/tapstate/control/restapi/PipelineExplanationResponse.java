package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineExplanation;
import io.tapstate.messages.MessageCatalog;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Stable JSON projection of the shared pipeline explanation, with the plan the pipeline's current run was submitted
 * on beside it where one is recorded, and the members of the cluster that plan was not worked out for where there
 * are any; each is omitted otherwise.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineExplanationResponse(
        String pipelineId,
        String state,
        String kind,
        String message,
        String observedAt,
        Long observedAgeMillis,
        String freshness,
        List<Evidence> evidence,
        List<String> cannotSay,
        @JsonInclude(JsonInclude.Include.ALWAYS) Next next,
        Pending pending,
        ExecutionPlanResponse plan,
        List<String> awaitingRebalance) {

    record Evidence(String source, String field,
            @JsonInclude(JsonInclude.Include.ALWAYS) Object value) {
    }

    record Next(String action, String message) {
    }

    record Pending(String reason) {
    }

    static PipelineExplanationResponse of(PipelineExplanation explanation) {
        return of(explanation, null, Locale.ENGLISH);
    }

    static PipelineExplanationResponse of(
            PipelineExplanation explanation, MessageCatalog catalog, Locale locale) {
        return new PipelineExplanationResponse(
                explanation.pipelineId(),
                explanation.state().name(),
                explanation.kind().name(),
                explanation.message(),
                explanation.observedAt() == null ? null : explanation.observedAt().toString(),
                explanation.observedAgeMillis(),
                explanation.freshness().name(),
                explanation.evidence().stream().map(evidence -> evidence(evidence, catalog, locale)).toList(),
                explanation.cannotSay(),
                explanation.next() == null ? null
                        : new Next(explanation.next().action().name(), explanation.next().message()),
                explanation.pending() == null ? null
                        : new Pending(explanation.pending().reason().name()),
                ExecutionPlanResponse.of(explanation.plan()),
                explanation.awaitingRebalance().isEmpty() ? null : explanation.awaitingRebalance());
    }

    private static Evidence evidence(
            PipelineExplanation.Evidence evidence, MessageCatalog catalog, Locale locale) {
        return new Evidence(evidence.source().name().toLowerCase(java.util.Locale.ROOT),
                evidence.field(), evidenceValue(evidence.value(), catalog, locale));
    }

    private static Object evidenceValue(Object value, MessageCatalog catalog, Locale locale) {
        if (value instanceof PipelineExplanation.Failure failure) {
            Map<String, Object> params = new TreeMap<>(failure.params());
            if (catalog == null) {
                return new ApiError(failure.code(), new TreeMap<>(failure.params()), failure.message(), failure.diagnostic());
            }
            MessageCatalog.Rendered rendered = catalog.render(failure.code(), params, locale);
            String message = failure.code().equals(rendered.message()) ? failure.message() : rendered.message();
            return new ApiError(failure.code(), new TreeMap<>(failure.params()), message,
                    rendered.solution(), ApiError.localizeDiagnostic(failure.diagnostic(), catalog, locale));
        }
        return value;
    }
}
