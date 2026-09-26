package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineEvents;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Stable public event JSON, deliberately excluding storage and execution identity. */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineEventsResponse(String pipelineId, String from, String to,
        String effectiveFrom, String effectiveTo, String retentionCutoff, String completeness,
        List<Event> events, List<KnownGap> knownGaps,
        @JsonInclude(JsonInclude.Include.ALWAYS) String nextCursor) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Event(String id, String occurredAt, String kind, String message,
            String beforeState, String afterState, Failure failure, String reason) {
    }

    record Failure(String code, Map<String, String> params, String message) {
    }

    record KnownGap(String eventId, String from, String to, List<String> reasons) {
    }

    static PipelineEventsResponse of(PipelineEvents source) {
        return new PipelineEventsResponse(source.pipelineId(), source.from().toString(),
                source.to().toString(), source.effectiveFrom().toString(), source.effectiveTo().toString(),
                source.retentionCutoff().toString(), source.completeness().name(),
                source.events().stream().map(PipelineEventsResponse::event).toList(),
                source.knownGaps().stream().map(PipelineEventsResponse::gap).toList(),
                source.nextCursor());
    }

    private static Event event(PipelineEvents.Event event) {
        PipelineEvents.Failure failure = event.failure();
        return new Event(event.id(), event.occurredAt().toString(), event.kind().name(), event.message(),
                event.beforeState() == null ? null : event.beforeState().name(),
                event.afterState() == null ? null : event.afterState().name(),
                failure == null ? null : new Failure(failure.code(), new TreeMap<>(failure.params()),
                        failure.message()), event.reason());
    }

    private static KnownGap gap(PipelineEvents.KnownGap gap) {
        return new KnownGap(gap.eventId(), gap.from().toString(), gap.to().toString(),
                gap.reasons().stream().map(Enum::name).toList());
    }
}
