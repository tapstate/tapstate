package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.PipelineEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded, best-effort event history for one live pipeline incarnation. */
public interface PipelineEventStore {

    int MAX_PAGE_SIZE = 500;

    /** Stable ascending position in the event history. */
    record Key(Instant occurredAt, String id) {
        public Key {
            Objects.requireNonNull(occurredAt, "occurredAt");
            occurredAt = occurredAt.truncatedTo(ChronoUnit.MILLIS);
            Objects.requireNonNull(id, "id");
            if (id.isBlank()) {
                throw new IllegalArgumentException("an event id is not blank");
            }
        }
    }

    /** At most one bounded page, sorted by {@code (occurredAt,id)}. */
    record Page(List<PipelineEvent> events, boolean hasMore) {
        public Page {
            events = List.copyOf(Objects.requireNonNull(events, "events"));
            if (events.size() > MAX_PAGE_SIZE || (hasMore && events.isEmpty())) {
                throw new IllegalArgumentException("an event page is bounded and a successor has an anchor");
            }
        }

        public Optional<Key> lastKey() {
            if (events.isEmpty()) {
                return Optional.empty();
            }
            PipelineEvent last = events.get(events.size() - 1);
            return Optional.of(new Key(last.occurredAt(), last.id()));
        }
    }

    /** Appends once; replaying an identical id and body succeeds, but a conflicting id fails visibly. */
    void append(PipelineEvent event);

    /** Reads one incarnation in {@code [from,to)}, strictly after {@code after} when present. */
    Page readPage(String pipelineId, String incarnationId, Instant from, Instant to, Key after, int limit);

    /** Best-effort removal of only the incarnation captured when its artifact was deleted. */
    void deleteIncarnation(String pipelineId, String incarnationId);

    Duration retention();
}
