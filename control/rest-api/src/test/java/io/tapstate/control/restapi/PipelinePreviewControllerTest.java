package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.PipelinePreviewCommand;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.PipelinePreviewEvent;
import io.tapstate.control.core.PipelinePreviewSession;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewControllerTest {

    @Test
    void readsARequestWithinTheByteLimit() throws IOException {
        byte[] body = {1, 2, 3, 4};

        assertThat(PipelinePreviewController.readBounded(new ByteArrayInputStream(body), body.length))
                .containsExactly(body);
    }

    @Test
    void refusesAnOversizedRequestWithACodedInputError() {
        byte[] body = {1, 2, 3, 4, 5};

        assertThatThrownBy(() -> PipelinePreviewController.readBounded(new ByteArrayInputStream(body), 4))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void decodesOnlyJsonObjectsAndWrapsMalformedOrNonObjectBodies() {
        ObjectMapper json = new ObjectMapper();
        Map<?, ?> decoded = PipelinePreviewController.decodeRequest(
                "{\"pipelineId\":\"orders\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8), json);
        assertThat(decoded.get("pipelineId")).isEqualTo("orders");
        assertThat(PipelinePreviewController.decodeRequest(
                "null".getBytes(java.nio.charset.StandardCharsets.UTF_8), json)).isNull();
        assertThatThrownBy(() -> PipelinePreviewController.decodeRequest(
                "[]".getBytes(java.nio.charset.StandardCharsets.UTF_8), json))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.decodeRequest(
                "{".getBytes(java.nio.charset.StandardCharsets.UTF_8), json))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void parsesOptionalPreviewFieldsAndDraftPreconditions() {
        PipelinePreviewCommand command = PipelinePreviewController.parse(Map.of(
                "pipelineId", "orders", "outputId", "mongo", "rootLimit", 20, "sampleId", "sample-a",
                "drafts", List.of(Map.of("source", "editor", "content", "candidate",
                        "expectedContentHash", "abc123"))));

        assertThat(command.pipelineId()).isEqualTo("orders");
        assertThat(command.outputId()).isEqualTo("mongo");
        assertThat(command.rootLimit()).isEqualTo(20);
        assertThat(command.sampleId()).isEqualTo("sample-a");
        assertThat(command.drafts()).containsExactly(new ArtifactDraft("editor", "candidate", "abc123"));
        assertThat(PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts", List.of()))
                .outputId()).isNull();
    }

    @Test
    void refusesMalformedPreviewShapesUnknownFieldsAndInvalidIntegerValues() {
        assertThatThrownBy(() -> PipelinePreviewController.parse(null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "extra", true)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", " ")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "rootLimit", 1.5)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "rootLimit", "2")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "outputId", 3)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts", "bad")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts",
                List.of("bad")))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts",
                List.of(Map.of("content", "ok", "unexpected", true))))).isInstanceOf(TapstateException.class);
    }

    @Test
    void streamsUntilTerminalEventAndCancelsOnDisconnectOrInterrupt() throws IOException {
        ObjectMapper json = new ObjectMapper();
        PipelinePreviewEvent progress = event("sample.completed");
        PipelinePreviewEvent terminal = event("run.completed");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TestSession complete = new TestSession(progress, terminal, event("ignored"));
        PipelinePreviewController.writeEvents(json, output, complete);
        assertThat(output.toString(java.nio.charset.StandardCharsets.UTF_8).split("\\n"))
                .hasSize(2).anyMatch(line -> line.contains("sample.completed"))
                .anyMatch(line -> line.contains("run.completed"));
        assertThat(complete.cancelled).isFalse();

        TestSession disconnected = new TestSession(progress);
        assertThatThrownBy(() -> PipelinePreviewController.writeEvents(json, new OutputStream() {
            @Override
            public void write(int value) {
                // Accept serialized bytes so the disconnect occurs at the streaming flush boundary.
            }

            @Override
            public void flush() throws IOException {
                throw new IOException("client disconnected");
            }
        }, disconnected)).isInstanceOf(IOException.class);
        assertThat(disconnected.cancelled.get()).isTrue();

        TestSession interrupted = new TestSession();
        interrupted.interruptNext = true;
        assertThatThrownBy(() -> PipelinePreviewController.writeEvents(json, new ByteArrayOutputStream(), interrupted))
                .isInstanceOf(IOException.class).hasMessageContaining("interrupted");
        assertThat(interrupted.cancelled.get()).isTrue();
        Thread.interrupted();
    }

    @Test
    void keepsTheServletOutputOpenForEveryEventUntilItsOwnerClosesIt() throws IOException {
        ObjectMapper json = new ObjectMapper();
        TestSession session = new TestSession(event("sample.completed"), event("run.completed"));
        CloseAwareServletOutputStream output = new CloseAwareServletOutputStream();

        PipelinePreviewController.writeEvents(json, output, session);

        String[] lines = output.content.toString(java.nio.charset.StandardCharsets.UTF_8).split("\\n");
        assertThat(lines).hasSize(2);
        assertThat(json.readValue(lines[0], Map.class)).containsEntry("kind", "sample.completed");
        assertThat(json.readValue(lines[1], Map.class)).containsEntry("kind", "run.completed");
        assertThat(output.closed).isFalse();
        assertThat(session.cancelled).isFalse();

        output.close();
        assertThat(output.closed).isTrue();
        assertThat(output.closeCount).isEqualTo(1);
    }

    @Test
    void aSerializerWriteDisconnectCancelsThePreviewAndPropagatesTheTransportFailure() {
        TestSession session = new TestSession(event("sample.completed"));
        IOException disconnected = new IOException("client disconnected during serialization");
        OutputStream output = new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw disconnected;
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                throw disconnected;
            }
        };

        assertThatThrownBy(() -> PipelinePreviewController.writeEvents(new ObjectMapper(), output, session))
                .isSameAs(disconnected);
        assertThat(session.cancelled).isTrue();
    }

    private static final class CloseAwareServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream content = new ByteArrayOutputStream();
        private boolean closed;
        private int closeCount;

        @Override
        public boolean isReady() {
            return !closed;
        }

        @Override
        public void setWriteListener(WriteListener listener) {
            throw new UnsupportedOperationException("this synchronous test stream has no write listener");
        }

        @Override
        public void write(int value) throws IOException {
            if (closed) {
                throw new IOException("the servlet response is already closed");
            }
            content.write(value);
        }

        @Override
        public void close() {
            closed = true;
            closeCount++;
        }
    }

    private static PipelinePreviewEvent event(String kind) {
        return new PipelinePreviewEvent("run", "candidate", 0, kind, Instant.parse("2026-10-04T00:00:00Z"),
                Map.of("ok", true));
    }

    private static final class TestSession implements PipelinePreviewSession {
        private final ArrayDeque<PipelinePreviewEvent> events = new ArrayDeque<>();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private boolean interruptNext;

        private TestSession(PipelinePreviewEvent... events) {
            this.events.addAll(List.of(events));
        }

        @Override
        public PipelinePreviewEvent next() throws InterruptedException {
            if (interruptNext) {
                interruptNext = false;
                throw new InterruptedException("test interruption");
            }
            return events.poll();
        }

        @Override
        public boolean sampleCacheHit() {
            return false;
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }
    }
}
