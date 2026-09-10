package io.tapstate.control.restapi;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.control.core.DataBrowserService;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DataBrowserChange;
import io.tapstate.spi.store.DataBrowserChangeListener;
import io.tapstate.spi.store.DataBrowserTableInfo;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketExtension;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

/**
 * How a follow ends when the reader did not let go of it.
 *
 * <p>Both endings here are invisible from a reader's seat unless the connection is closed with them,
 * and that is the whole reason they are pinned. A follow whose stream died and a collection nobody is
 * changing produce the identical screen — nothing, indefinitely — and a follow left running by
 * somebody who walked away holds a connector instance and a place in the host's ceiling that nothing
 * evicts, so it is a place no other reader can ever have.
 */
class DataBrowserFollowEndingTest {

    private static final String SOURCE = "views";

    private static final Duration IDLE_LIMIT = Duration.ofMinutes(10);

    @Test
    @DisplayName("a stream that failed closes its connection, naming why, and gives the instance back")
    void endsAFollowWhoseStreamFailed() throws Exception {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        AtomicInteger closes = new AtomicInteger();
        MutableClock clock = new MutableClock(Instant.parse("2026-08-19T10:00:00Z"));
        DataBrowserTailHandler handler =
                new DataBrowserTailHandler(service(opened, closes), clock, IDLE_LIMIT);
        FakeSession session = new FakeSession("s1");

        handler.afterConnectionEstablished(session);
        opened.get().onError(new IllegalStateException("the driver went away"));

        assertThat(session.closedWith)
                .as("the stream is over and the connection is not; a reader left on it is watching "
                        + "what a collection nobody is changing looks like")
                .isNotNull();
        assertThat(session.closedWith.getReason()).isEqualTo("data-browser.follow-stopped");
        assertThat(session.closedWith.getCode())
                .as("closed as a refusal the client cannot fix by reconnecting, so it stops rather "
                        + "than re-attaching into the same failure")
                .isEqualTo(1008);
        assertThat(closes.get())
                .as("what the follow held was a connector instance and a place in the ceiling")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a follow that has shown nothing for longer than the limit is reclaimed")
    void reclaimsAFollowThatWentQuiet() throws Exception {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        AtomicInteger closes = new AtomicInteger();
        MutableClock clock = new MutableClock(Instant.parse("2026-08-19T10:00:00Z"));
        DataBrowserTailHandler handler =
                new DataBrowserTailHandler(service(opened, closes), clock, IDLE_LIMIT);
        FakeSession session = new FakeSession("s1");

        handler.afterConnectionEstablished(session);
        clock.set(Instant.parse("2026-08-19T10:10:01Z"));
        handler.sweepIdle();

        assertThat(session.closedWith).isNotNull();
        assertThat(session.closedWith.getReason()).isEqualTo("data-browser.follow-idle");
        assertThat(closes.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("a follow still showing changes is left alone, and each change restarts its clock")
    void leavesAFollowThatIsStillShowingChangesAlone() throws Exception {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        AtomicInteger closes = new AtomicInteger();
        MutableClock clock = new MutableClock(Instant.parse("2026-08-19T10:00:00Z"));
        DataBrowserTailHandler handler =
                new DataBrowserTailHandler(service(opened, closes), clock, IDLE_LIMIT);
        FakeSession session = new FakeSession("s1");

        handler.afterConnectionEstablished(session);
        // Nine minutes on, a change arrives: the limit is measured from what the reader was last
        // shown, so this has to move the deadline. A sweep that reclaimed on age since the handshake
        // would pass the case above and cut this reader off mid-stream, which is the failure worth
        // discriminating - it looks like a dropped connection and there is nothing to connect it to.
        clock.set(Instant.parse("2026-08-19T10:09:00Z"));
        opened.get().onChange(new DataBrowserChange(
                DataBrowserChange.Kind.INSERT, null, Map.of("id", 1), 1L));
        clock.set(Instant.parse("2026-08-19T10:18:00Z"));
        handler.sweepIdle();

        assertThat(session.closedWith)
                .as("nine minutes of quiet, then a change, then nine more: never ten in a row")
                .isNull();
        assertThat(closes.get()).isZero();
        assertThat(session.sent)
                .as("and the change itself still reached the reader")
                .hasSize(1);
    }

    // ---- fakes ----

    /** A service whose follow records the listener it was given and counts what closing it releases. */
    private static DataBrowserService service(
            AtomicReference<DataBrowserChangeListener> opened, AtomicInteger closes) {
        return new DataBrowserService(
                store(),
                new EmptySchemaStore(),
                config -> List.of("orders"),
                (config, collection) -> new DataBrowserTableInfo(0L, 0L, 0L),
                (config, query) -> {
                    throw new AssertionError("a follow must not run a bounded read");
                },
                (config, request, listener) -> {
                    opened.set(listener);
                    return closes::incrementAndGet;
                });
    }

    private static ArtifactStore store() {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return id.equals(SOURCE)
                        ? Optional.of(new SourceResource(id, null, "mongodb",
                                Map.of("uri", "mongodb://db.local"), null, null, null, null))
                        : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of();
            }
        };
    }

    /** A clock the case sets, because a limit measured in minutes is not a thing to prove by waiting. */
    private static final class MutableClock extends Clock {
        private volatile Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void set(Instant now) {
            this.now = now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /** A session that records what was sent to it and how it was closed. */
    private static final class FakeSession implements WebSocketSession {
        private final String id;
        private final List<String> sent = new ArrayList<>();
        private volatile CloseStatus closedWith;

        private FakeSession(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public URI getUri() {
            return URI.create("ws://host/api/data-browser/" + SOURCE + "/orders/tail");
        }

        @Override
        public HttpHeaders getHandshakeHeaders() {
            return HttpHeaders.EMPTY;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return new HashMap<>();
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
            sent.add(String.valueOf(message.getPayload()));
        }

        @Override
        public boolean isOpen() {
            return closedWith == null;
        }

        @Override
        public void close() {
            closedWith = CloseStatus.NORMAL;
        }

        @Override
        public void close(CloseStatus status) {
            closedWith = status;
        }
    }
}
