package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineLogs;
import io.tapstate.core.logging.LogCursor;
import org.springframework.web.socket.CloseStatus;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * The logs follow channel resumes from a server-owned cursor and sends only lines strictly after that
 * cursor. A ring rollover is reported in the next frame as truncation instead of re-sending an ambiguous
 * tail, so a reconnect can continue without heuristic de-duplication.
 */
final class PipelineLogsFollowHandler extends PollingStreamHandler {

    /** Session attribute holding the last cursor the follower accepted. */
    private static final String LAST_CURSOR = "tapstate.stream.lastLogCursor";

    private final PipelineLogQueryService logs;

    PipelineLogsFollowHandler(PipelineLogQueryService logs, TaskScheduler scheduler, Duration interval) {
        super(scheduler, interval);
        this.logs = Objects.requireNonNull(logs, "logs");
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        try {
            LogCursor requested = requestedCursor(session);
            if (requested != null) {
                session.getAttributes().put(LAST_CURSOR, requested);
            }
            super.afterConnectionEstablished(session);
        } catch (IllegalArgumentException malformedCursor) {
            closeBadCursor(session);
        }
    }

    @Override
    protected void poll(WebSocketSession session, String pipelineId) {
        LogCursor after = (LogCursor) session.getAttributes().get(LAST_CURSOR);
        PipelineLogs page = logs.logs(pipelineId, after, Integer.MAX_VALUE);
        if (!page.lines().isEmpty() || page.truncated()) {
            send(session, StreamFrames.logs(page));
        }
        if (page.nextCursor() != null) {
            session.getAttributes().put(LAST_CURSOR, page.nextCursor());
        }
    }

    private static LogCursor requestedCursor(WebSocketSession session) {
        URI uri = session.getUri();
        if (uri == null || uri.getRawQuery() == null) {
            return null;
        }
        for (String parameter : uri.getRawQuery().split("&")) {
            int equals = parameter.indexOf('=');
            if (equals < 0 || !"after".equals(parameter.substring(0, equals))) {
                continue;
            }
            String token = URLDecoder.decode(parameter.substring(equals + 1), StandardCharsets.UTF_8);
            return LogCursor.parse(token);
        }
        return null;
    }

    private static void closeBadCursor(WebSocketSession session) {
        try {
            session.close(CloseStatus.BAD_DATA);
        } catch (IOException ignored) {
            // The malformed session is already unusable; there is no stream to recover here.
        }
    }
}
