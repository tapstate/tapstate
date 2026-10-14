package io.tapstate.control.restapi;

import io.tapstate.control.core.MonitorError;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelineStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.messages.MessageCatalog;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * The status watch channel: streams a pipeline's lifecycle state, then the next state whenever it
 * changes. It polls the store-backed runtime observation, not the aggregate one-shot status, so it is a
 * re-poll over the eventually-consistent observation doc, not a live push. Only a changed observed state
 * emits a frame. An applied pipeline or known draft without an observation emits nothing and keeps polling.
 */
final class PipelineStatusWatchHandler extends PollingStreamHandler {

    /** Session attribute holding the last state already sent to this watcher, so only changes are pushed. */
    private static final String LAST_STATE = "tapstate.stream.lastState";

    private final PipelineObservationQueryService observations;
    private final Predicate<String> knownDraft;
    private final MessageCatalog catalog;

    PipelineStatusWatchHandler(PipelineObservationQueryService observations, Predicate<String> knownDraft,
            MessageCatalog catalog, TaskScheduler scheduler, Duration interval) {
        super(scheduler, interval);
        this.observations = Objects.requireNonNull(observations, "observations");
        this.knownDraft = Objects.requireNonNull(knownDraft, "knownDraft");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    protected void poll(WebSocketSession session, String pipelineId) {
        PipelineStatus status;
        try {
            status = observations.lifecycleStatus(pipelineId);
        } catch (TapstateException coded) {
            if (coded.code() == MonitorError.NO_OBSERVATION) {
                // Applied but not yet converged: transient, so nothing to stream and keep polling.
                return;
            }
            if (coded.code() == LifecycleError.UNKNOWN_PIPELINE) {
                if (knownDraft.test(pipelineId)) {
                    // A saved draft is visible to the one-shot aggregate status but has no runtime stream yet.
                    return;
                }
            }
            // Anything else -- most notably lifecycle.unknown-pipeline, permanent per this same read
            // face's javadoc -- will never resolve into an observation on its own. Waiting it out here
            // would hang the watch silently forever (the same failure mode the read face was fixed not
            // to have), so close the session with the coded reason instead of continuing to poll.
            closeUnrecoverable(session, coded);
            return;
        }
        PipelineState previous = (PipelineState) session.getAttributes().get(LAST_STATE);
        if (!status.state().equals(previous)) {
            send(session, StreamFrames.status(status, catalog));
            session.getAttributes().put(LAST_STATE, status.state());
        }
    }

    /** Closes the session carrying the coded reason it cannot be watched, rather than hanging silently. */
    private static void closeUnrecoverable(WebSocketSession session, TapstateException coded) {
        try {
            if (session.isOpen()) {
                session.close(new CloseStatus(1008, coded.code().code()));
            }
        } catch (IOException connectionGone) {
            // The peer closed first; nothing left to notify.
        }
    }
}
