package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLogs;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.logging.LogCursor;
import io.tapstate.core.logging.LogLine;
import io.tapstate.messages.MessageCatalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Renders a streamed status or logs frame to the same compact JSON the one-shot {@code GET} read faces
 * return, so the CLI decodes a frame from the websocket with the exact decoder it uses for a polled
 * read. The rest-api ring carries no JSON library (only Spring's message converters serialise the REST
 * bodies, and those are not reachable as a plain object-to-string call here), so the tree is built by
 * hand and rendered through the core {@link JsonWriter}, which does the escaping.
 */
final class StreamFrames {

    private StreamFrames() {
    }

    /**
     * A status frame: {@code {"pipelineId":..,"state":..,"failure":{"code":..,"params":..,"message":..}}},
     * the state as its wire name and {@code failure} omitted while the pipeline is healthy — the identical
     * shape {@code PipelineStatusResponse} sends for the one-shot {@code GET}, so a stream frame and a
     * polled read decode through the exact same client-side code. A status with no failure to report is
     * not the same information as "no failure was checked" (see the class javadoc), so the field is
     * omitted here rather than serialized as null, matching {@code PipelineStatusResponse}'s own contract.
     */
    static String status(PipelineStatus status, MessageCatalog catalog) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("pipelineId", status.pipelineId());
        frame.put("state", status.state().name());
        Map<String, Object> failure = failure(status.failure(), catalog);
        if (failure != null) {
            frame.put("failure", failure);
        }
        return JsonWriter.write(frame);
    }

    private static Map<String, Object> failure(ObservationFailure failure, MessageCatalog catalog) {
        if (failure == null) {
            return null;
        }
        Map<String, Object> params = new TreeMap<>(failure.params());
        Map<String, Object> encoded = new LinkedHashMap<>();
        encoded.put("code", failure.code());
        encoded.put("params", params);
        encoded.put("message", catalog.render(failure.code(), params).message());
        return encoded;
    }

    /** A logs frame with a resumable cursor and any retention-truncation indication. */
    static String logs(PipelineLogs logs) {
        List<Object> lines = new ArrayList<>();
        for (LogLine line : logs.lines()) {
            Map<String, Object> encoded = new LinkedHashMap<>();
            encoded.put("timestampMillis", line.timestampMillis());
            encoded.put("level", line.level());
            encoded.put("message", line.message());
            lines.add(encoded);
        }
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("pipelineId", logs.pipelineId());
        frame.put("lines", lines);
        LogCursor nextCursor = logs.nextCursor();
        if (nextCursor != null) {
            Map<String, Object> cursor = new LinkedHashMap<>();
            cursor.put("generation", nextCursor.generation());
            cursor.put("sequence", nextCursor.sequence());
            frame.put("nextCursor", cursor);
        }
        frame.put("truncated", logs.truncated());
        return JsonWriter.write(frame);
    }
}
