package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.HistoryCursorCodec;
import io.tapstate.control.core.EventsCursorCodec;
import io.tapstate.control.core.PipelineEventsQueryService;
import io.tapstate.control.core.PipelineExplainService;
import io.tapstate.control.core.PipelineHistoryQueryService;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.messages.EventCatalog;
import io.tapstate.messages.ExplanationCatalog;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.PipelineEventStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Inert dependencies that let full-face tests mount observation controllers they do not drive. */
@Configuration(proxyBeanMethods = false)
class ObservabilityTestConfiguration {

    @Bean
    RateHistoryStore emptyRateHistoryStore() {
        return new RateHistoryStore() {
            @Override
            public void append(RateSample sample) {
                throw new AssertionError("this full-face test does not write rate history");
            }

            @Override
            public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
                return new Page(List.of(), false);
            }

            @Override
            public Page readPageVisible(String pipelineId, Visibility visibility,
                    Instant from, Instant to, Key after, int limit) {
                return new Page(List.of(), false);
            }

            @Override
            public Optional<Entry> read(String pipelineId, Key key) {
                return Optional.empty();
            }

            @Override
            public Optional<Entry> readVisible(String pipelineId, Visibility visibility, Key key) {
                return Optional.empty();
            }

            @Override
            public Optional<Entry> predecessor(String pipelineId, Instant at) {
                return Optional.empty();
            }

            @Override
            public Optional<Entry> predecessorVisible(String pipelineId, Visibility visibility, Instant at) {
                return Optional.empty();
            }

            @Override
            public Optional<Entry> successor(String pipelineId, Instant at) {
                return Optional.empty();
            }

            @Override
            public Optional<Entry> successorVisible(String pipelineId, Visibility visibility, Instant at) {
                return Optional.empty();
            }

            @Override
            public void deleteAll(String pipelineId) {
                throw new AssertionError("this full-face test does not remove rate history");
            }

            @Override
            public Duration retention() {
                return Duration.ofDays(15);
            }
        };
    }

    @Bean
    PipelineHistoryQueryService pipelineHistoryQueryService(
            ArtifactQueryService artifacts, RateHistoryStore history, Clock clock) {
        return new PipelineHistoryQueryService(artifacts, history, Duration.ofMinutes(1), clock,
                new HistoryCursorCodec("test-history-secret".getBytes(StandardCharsets.UTF_8), clock));
    }

    @Bean
    PipelineEventsQueryService pipelineEventsQueryService(ArtifactQueryService artifacts, Clock clock) {
        PipelineEventStore empty = new PipelineEventStore() {
            @Override public void append(PipelineEvent event) { throw new AssertionError("no event write expected"); }
            @Override public Page readPage(String pipelineId, String incarnationId, Instant from, Instant to,
                    Key after, int limit) { return new Page(List.of(), false); }
            @Override public void deleteIncarnation(String pipelineId, String incarnationId) {
                throw new AssertionError("no event delete expected");
            }
            @Override public Duration retention() { return Duration.ofDays(15); }
        };
        EventCatalog messages = EventCatalog.bundled();
        return new PipelineEventsQueryService(artifacts, empty,
                new EventsCursorCodec("test-events-secret".getBytes(StandardCharsets.UTF_8), clock),
                clock, messages::render);
    }

    @Bean
    PipelineExplainService pipelineExplainService(
            ArtifactQueryService artifacts, ObservationStore observations, Clock clock) {
        ExplanationCatalog messages = ExplanationCatalog.bundled();
        return new PipelineExplainService(artifacts, observations, clock, messages::render);
    }
}
