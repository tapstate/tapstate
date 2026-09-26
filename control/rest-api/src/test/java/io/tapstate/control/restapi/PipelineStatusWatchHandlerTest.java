package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketExtension;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The status watch channel re-polls the same store-backed status read the one-shot {@code GET} uses.
 * That read now answers two different exceptions for "no observation": {@code monitor.no-observation}
 * (applied, not yet converged -- transient, worth waiting out) and {@code lifecycle.unknown-pipeline}
 * (no such pipeline, ever -- permanent). A watch loop that cannot tell them apart hangs silently forever
 * on the permanent case, which is exactly the ambiguity the read side was fixed to remove.
 */
class PipelineStatusWatchHandlerTest {

    private static final MessageCatalog CATALOG = MessageCatalog.bundled();

    private static PipelineObservationQueryService serviceWith(ObservationStore observations, String... pipelineIds) {
        return serviceWith(observations, ExecutionPlans.NONE, pipelineIds);
    }

    private static PipelineObservationQueryService serviceWith(ObservationStore observations, ExecutionPlans plans,
            String... pipelineIds) {
        Map<String, Resource> byId = new HashMap<>();
        for (String id : pipelineIds) {
            byId.put(id, new io.tapstate.core.dsl.DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_x
                    serve:
                      from: /.*/
                      sync:
                        - id: sink1
                          source: tgt_x
                          write_mode: upsert
                          ddl: apply
                    """.formatted(id)));
        }
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(r -> byId.put(r.id(), r));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(byId.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(byId.values());
            }
        });
        return new PipelineObservationQueryService(artifacts, observations, plans);
    }

    private static ObservationStore emptyObservations() {
        return new ObservationStore() {
            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("removal is not exercised by this double");
            }

            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.empty();
            }
        };
    }

    @Test
    void aPipelineAppliedButNotYetConvergedKeepsThePollLoopOpen() {
        // monitor.no-observation is transient: nothing to stream yet, but the watch must not give up.
        PipelineObservationQueryService observations = serviceWith(emptyObservations(), "orders");
        PipelineStatusWatchHandler handler =
                new PipelineStatusWatchHandler(observations, CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "orders");

        assertThat(session.closeStatus).isNull();
        assertThat(session.sent).isEmpty();
    }

    @Test
    void aPipelineThatWasNeverAppliedClosesTheSessionInsteadOfPollingForever() {
        // lifecycle.unknown-pipeline is permanent -- no future tick will ever answer differently, so
        // treating it like the transient case would hang the watch silently for as long as the client
        // holds the socket open.
        PipelineObservationQueryService observations = serviceWith(emptyObservations());
        PipelineStatusWatchHandler handler =
                new PipelineStatusWatchHandler(observations, CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "ghost");

        assertThat(session.closeStatus).isNotNull();
        assertThat(session.closeStatus.getReason()).isEqualTo("lifecycle.unknown-pipeline");
        assertThat(session.sent).isEmpty();
    }

    @Test
    void aTypoedIdThatNamesAnotherResourceKindAlsoClosesRatherThanHangs() {
        // Same permanent code, reached the other way the read side tells apart: an id that resolves to
        // some other kind of artifact, not just one that resolves to nothing.
        Map<String, Resource> byId = new HashMap<>();
        byId.put("src_orders", new io.tapstate.core.dsl.DslParser().parse("""
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                mode: cdc
                tables: [ orders ]
                """));
        ArtifactQueryService artifacts = new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(byId.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(byId.values());
            }
        });
        PipelineObservationQueryService observations =
                new PipelineObservationQueryService(artifacts, emptyObservations());
        PipelineStatusWatchHandler handler =
                new PipelineStatusWatchHandler(observations, CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "src_orders");

        assertThat(session.closeStatus).isNotNull();
        assertThat(session.closeStatus.getReason()).isEqualTo("lifecycle.unknown-pipeline");
    }

    @Test
    void aHealthyPipelineStreamsItsStateOnce() {
        Observation running = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of());
        ObservationStore store = new ObservationStore() {
            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("removal is not exercised by this double");
            }

            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.of(running);
            }
        };
        PipelineObservationQueryService observations = serviceWith(store, "orders");
        PipelineStatusWatchHandler handler =
                new PipelineStatusWatchHandler(observations, CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "orders");

        assertThat(session.sent).hasSize(1);
        assertThat(session.closeStatus).isNull();
    }

    @Test
    void aWatchFollowsTheStateWithoutReadingThePlanOnEveryPoll() {
        // A frame does not carry the plan, and the plan changes only when a new run is submitted: read on every
        // poll, it would be read once a second for every open watch and thrown away each time.
        Observation running = new Observation("orders", PipelineState.RUNNING, Map.of(), Map.of());
        ObservationStore store = new ObservationStore() {
            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("removal is not exercised by this double");
            }

            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.of(running);
            }
        };
        List<List<String>> planReads = new ArrayList<>();
        ExecutionPlans plans = pipelineIds -> {
            planReads.add(List.copyOf(pipelineIds));
            return Map.of();
        };
        PipelineStatusWatchHandler handler = new PipelineStatusWatchHandler(serviceWith(store, plans, "orders"),
                CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "orders");
        handler.poll(session, "orders");

        assertThat(session.sent).hasSize(1);
        assertThat(planReads).isEmpty();
    }

    @Test
    void aFailedPipelineStreamsTheCodedReasonInTheSameFrameThatReportsItDead() {
        // A frame is only sent on a state change, so the one frame reporting a death is also the only
        // frame that will ever carry the reason -- nothing later repeats it. If that frame goes out
        // reasonless the watcher never learns why, no matter how long it keeps watching.
        Observation dead = new Observation("orders", PipelineState.FAILED, Map.of("errorCount", 1L),
                Map.of(), Map.of(), new io.tapstate.core.lifecycle.ObservationFailure(
                        "engine.job-failed", Map.of("pipeline", "orders", "cause", "sink refused the batch")));
        ObservationStore store = new ObservationStore() {
            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("removal is not exercised by this double");
            }

            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.of(dead);
            }
        };
        PipelineObservationQueryService observations = serviceWith(store, "orders");
        PipelineStatusWatchHandler handler =
                new PipelineStatusWatchHandler(observations, CATALOG, new NoOpTaskScheduler(), Duration.ofSeconds(1));
        FakeWebSocketSession session = new FakeWebSocketSession();

        handler.poll(session, "orders");

        assertThat(session.sent).hasSize(1);
        String frame = ((org.springframework.web.socket.TextMessage) session.sent.get(0)).getPayload();
        assertThat(frame).contains("\"code\":\"engine.job-failed\"");
        assertThat(frame).contains("\"cause\":\"sink refused the batch\"");
    }

    /**
     * A scheduler double that never actually schedules anything: every test drives {@code poll} directly,
     * so none of these are ever called, but {@link PollingStreamHandler}'s constructor requires a real
     * implementation of the interface.
     */
    private static final class NoOpTaskScheduler implements org.springframework.scheduling.TaskScheduler {
        @Override
        public java.util.concurrent.ScheduledFuture<?> schedule(
                Runnable task, org.springframework.scheduling.Trigger trigger) {
            return null;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> schedule(Runnable task, java.time.Instant startTime) {
            return null;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(
                Runnable task, java.time.Instant startTime, Duration period) {
            return null;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Duration period) {
            return null;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable task, java.time.Instant startTime, Duration delay) {
            return null;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Duration delay) {
            return null;
        }
    }

    /** A minimal in-memory {@link WebSocketSession}: real attributes, and records what would go on the wire. */
    private static final class FakeWebSocketSession implements WebSocketSession {
        private final Map<String, Object> attributes = new HashMap<>();
        private final List<WebSocketMessage<?>> sent = new ArrayList<>();
        private boolean open = true;
        private CloseStatus closeStatus;

        @Override
        public String getId() {
            return "test-session";
        }

        @Override
        public URI getUri() {
            return URI.create("ws://localhost/api/pipelines/orders/status/watch");
        }

        @Override
        public HttpHeaders getHandshakeHeaders() {
            return new HttpHeaders();
        }

        @Override
        public Map<String, Object> getAttributes() {
            return attributes;
        }

        @Override
        public Principal getPrincipal() {
            return null;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public String getAcceptedProtocol() {
            return null;
        }

        @Override
        public void setTextMessageSizeLimit(int messageSizeLimit) {
        }

        @Override
        public int getTextMessageSizeLimit() {
            return 0;
        }

        @Override
        public void setBinaryMessageSizeLimit(int messageSizeLimit) {
        }

        @Override
        public int getBinaryMessageSizeLimit() {
            return 0;
        }

        @Override
        public List<WebSocketExtension> getExtensions() {
            return List.of();
        }

        @Override
        public void sendMessage(WebSocketMessage<?> message) {
            sent.add(message);
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public void close(CloseStatus status) throws IOException {
            open = false;
            closeStatus = status;
        }
    }
}
