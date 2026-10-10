package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.PipelinePreviewEvent;
import io.tapstate.control.core.PipelinePreviewCommand;
import io.tapstate.control.core.PipelinePreviewSession;
import io.tapstate.control.core.PipelinePreviewService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.async.CallableProcessingInterceptor;
import org.springframework.web.context.request.async.WebAsyncUtils;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/** NDJSON presentation for bounded, no-write Pipeline preview runs. */
@RestController
class PipelinePreviewController {

    private static final Set<String> REQUEST_FIELDS =
            Set.of("pipelineId", "outputId", "rootLimit", "sampleId", "drafts");
    private static final Set<String> DRAFT_FIELDS = Set.of("source", "content", "expectedContentHash");
    static final int MAX_REQUEST_BYTES = 4 * 1024 * 1024;

    private final PipelinePreviewService previews;
    private final ObjectMapper json;

    PipelinePreviewController(PipelinePreviewService previews, ObjectMapper json) {
        this.previews = Objects.requireNonNull(previews, "previews");
        this.json = Objects.requireNonNull(json, "json");
    }

    @Verb("pipeline.preview")
    @PostMapping(value = "/artifacts:preview", consumes = "application/json", produces = "application/x-ndjson")
    ResponseEntity<StreamingResponseBody> preview(HttpServletRequest request) throws IOException {
        if (request.getContentLengthLong() > MAX_REQUEST_BYTES) {
            throw MalformedRequest.rejecting("preview request exceeds the 4 MiB limit", null);
        }
        byte[] bytes = readBounded(request.getInputStream(), MAX_REQUEST_BYTES);
        PipelinePreviewCommand command = parse(decodeRequest(bytes, json));
        PreviewResponse response = new PreviewResponse(json, previews.open(AuthenticatedCaller.subject(), command));
        try {
            WebAsyncUtils.getAsyncManager(request).registerCallableInterceptor(response, response);
            return ResponseEntity.ok()
                    .header(HttpHeaders.CACHE_CONTROL, "no-store")
                    .contentType(MediaType.parseMediaType("application/x-ndjson"))
                    .body(response);
        } catch (RuntimeException | Error setupFailure) {
            try {
                response.close();
            } catch (RuntimeException | Error cleanupFailure) {
                if (cleanupFailure != setupFailure) {
                    setupFailure.addSuppressed(cleanupFailure);
                }
            }
            throw setupFailure;
        }
    }

    private static final class PreviewResponse
            implements StreamingResponseBody, CallableProcessingInterceptor, AutoCloseable {
        private final ObjectMapper json;
        private final PipelinePreviewSession stream;
        private final AtomicBoolean closed = new AtomicBoolean();

        private PreviewResponse(ObjectMapper json, PipelinePreviewSession stream) {
            this.json = json;
            this.stream = stream;
        }

        @Override
        public void writeTo(OutputStream output) throws IOException {
            PreviewResponse owner = this;
            try (owner) {
                writeEvents(json, output, stream);
            }
        }

        @Override
        public <T> void afterCompletion(NativeWebRequest request, Callable<T> task) {
            // Async scheduling can fail before writeTo gets an opportunity to release the session.
            close();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                stream.close();
            }
        }
    }

    static Map<?, ?> decodeRequest(byte[] bytes, ObjectMapper json) {
        Object decoded;
        try {
            decoded = json.readValue(bytes, Object.class);
        } catch (JacksonException malformedJson) {
            throw MalformedRequest.rejecting("request body is not valid JSON", malformedJson);
        }
        Map<?, ?> body;
        if (decoded instanceof Map<?, ?> object) {
            body = object;
        } else if (decoded == null) {
            body = null;
        } else {
            throw MalformedRequest.rejecting("preview request body must be a JSON object", null);
        }
        return body;
    }

    static byte[] readBounded(InputStream input, int maxBytes) throws IOException {
        byte[] bytes = input.readNBytes(maxBytes + 1);
        if (bytes.length > maxBytes) {
            throw MalformedRequest.rejecting("preview request exceeds the 4 MiB limit", null);
        }
        return bytes;
    }

    static PipelinePreviewCommand parse(Map<?, ?> body) {
        if (body == null) {
            throw MalformedRequest.rejecting("request body is required", null);
        }
        rejectUnknown(body, REQUEST_FIELDS, "preview request");
        String pipelineId = text(body.get("pipelineId"), "pipelineId is required");
        String outputId = optionalText(body.get("outputId"), "outputId must be a string");
        Integer rootLimit = integer(body.get("rootLimit"), "rootLimit must be an integer");
        String sampleId = optionalText(body.get("sampleId"), "sampleId must be a string");
        Object rawDrafts = MalformedRequest.require(body.get("drafts"), "drafts must be an array");
        if (!(rawDrafts instanceof java.util.List<?> list)) {
            throw MalformedRequest.rejecting("drafts must be an array", null);
        }
        if (list.size() > PipelinePreviewService.MAX_DRAFTS) {
            throw MalformedRequest.rejecting(
                    "drafts exceed the " + PipelinePreviewService.MAX_DRAFTS + " resource limit", null);
        }
        ArrayList<ArtifactDraft> drafts = new ArrayList<>(list.size());
        for (Object raw : list) {
            if (!(raw instanceof Map<?, ?> draft)) {
                throw MalformedRequest.rejecting("each draft must be an object", null);
            }
            rejectUnknown(draft, DRAFT_FIELDS, "draft");
            String content = text(draft.get("content"), "each draft must carry non-blank content");
            drafts.add(new ArtifactDraft(optionalText(draft.get("source"), "draft source must be a string"),
                    content,
                    optionalText(draft.get("expectedContentHash"), "expectedContentHash must be a string")));
        }
        return new PipelinePreviewCommand(pipelineId, outputId, rootLimit, sampleId, drafts);
    }

    private static void rejectUnknown(Map<?, ?> body, Set<String> allowed, String label) {
        for (Object key : body.keySet()) {
            if (!(key instanceof String name) || !allowed.contains(name)) {
                throw MalformedRequest.rejecting(label + " contains an unknown field", null);
            }
        }
    }

    private static String text(Object value, String reason) {
        if (!(value instanceof String text) || text.isBlank()) {
            throw MalformedRequest.rejecting(reason, null);
        }
        return text;
    }

    private static String optionalText(Object value, String reason) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text) || text.isBlank()) {
            throw MalformedRequest.rejecting(reason, null);
        }
        return text;
    }

    private static Integer integer(Object value, String reason) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw MalformedRequest.rejecting(reason, null);
        }
        try {
            return new BigDecimal(number.toString()).intValueExact();
        } catch (ArithmeticException | NumberFormatException failure) {
            throw MalformedRequest.rejecting(reason, failure);
        }
    }

    static void writeEvents(ObjectMapper json, OutputStream output, PipelinePreviewSession stream) throws IOException {
        SerializationOutputStream serializationOutput = new SerializationOutputStream(output);
        try {
            PipelinePreviewEvent event;
            while ((event = stream.next()) != null) {
                json.writeValue(serializationOutput, event);
                output.write('\n');
                output.flush();
                if ("run.completed".equals(event.kind()) || "run.failed".equals(event.kind())) {
                    break;
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            stream.cancel();
            throw new IOException("Pipeline preview stream was interrupted", interrupted);
        } catch (RuntimeException serializationFailure) {
            if (serializationOutput.ioFailure != null) {
                stream.cancel();
                throw serializationOutput.ioFailure;
            }
            throw serializationFailure;
        } catch (IOException disconnected) {
            stream.cancel();
            throw disconnected;
        }
    }

    private static final class SerializationOutputStream extends OutputStream {
        private final OutputStream target;
        private IOException ioFailure;

        private SerializationOutputStream(OutputStream target) {
            this.target = target;
        }

        @Override
        public void write(int value) throws IOException {
            try {
                target.write(value);
            } catch (IOException failure) {
                ioFailure = failure;
                throw failure;
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            try {
                target.write(bytes, offset, length);
            } catch (IOException failure) {
                ioFailure = failure;
                throw failure;
            }
        }

        @Override
        public void flush() throws IOException {
            try {
                target.flush();
            } catch (IOException failure) {
                ioFailure = failure;
                throw failure;
            }
        }

        @Override
        public void close() {
            // Serialization owns its generator; the servlet owns the response stream.
        }
    }
}
