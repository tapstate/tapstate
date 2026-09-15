package io.tapstate.adapters.pdk;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What a connector says about itself, written into the host's own log.
 *
 * <p>A connector explains its own failures better than anything outside it can: which setting is wrong,
 * which permission is missing, that it is retrying and why. It says so through the log on the context it
 * is driven with -- this one -- and through the shared static channel the contract also gives it. Both
 * ends arrive here so that the mapping from a connector's levels to the host's is written once.
 *
 * <p><b>Warnings and errors are kept; everything below them is lowered to debug.</b> The read face holds
 * a bounded window of recent lines, so a connector's routine progress chatter would push out the one
 * error a reader came for. Lowered rather than dropped: it stays reachable by turning the level up,
 * which is what a connector author debugging their own connector needs, and it is no longer printed
 * straight to the console by the contract's default -- so it goes through the same format, the same
 * redaction and the same attribution as every other line.
 *
 * <p>Each line is filed against the pipeline this connector was opened for, whatever thread writes it --
 * a connector may write from a thread of its own, long after the call that created it returned. A drive
 * that belongs to no single pipeline (a schema discovery, a connection test) names none, and its lines
 * stay in the host log without being filed against any pipeline: a source is discovered on behalf of
 * whoever asked, and any number of pipelines may read the same one.
 */
final class ConnectorLog implements Log {

    /** One logger per connector id, so a reader can see which connector said it and silence one alone. */
    private static final String LOGGER_PREFIX = "io.tapstate.connector.";

    /** The shared static channel is installed once per process, against the same mapping as the rest. */
    private static boolean sharedChannelInstalled;

    private final Logger host;
    private final String pipelineId;

    /**
     * A log for one connector handle. {@code pipelineId} is the pipeline whose drive opened it, or null
     * for a drive that names no pipeline.
     */
    ConnectorLog(String connectorId, String pipelineId) {
        this.host = LoggerFactory.getLogger(LOGGER_PREFIX + connectorId);
        this.pipelineId = pipelineId;
    }

    /**
     * Routes the contract's shared static log channel into the host log as well.
     *
     * <p>That channel is a process-wide singleton on the contract every connector binds to, and with
     * nobody listening it prints to standard output -- outside the log, outside the format, outside
     * redaction, and invisible to the read face. Installed here, on the first connector opened, because
     * this module is the only one that may name the contract at all.
     *
     * <p>It carries no pipeline of its own: a static channel cannot say which of several running
     * pipelines a call belongs to. Lines written while the host is driving a connector are attributed by
     * that drive; lines a connector writes from a thread of its own are left unattributed rather than
     * filed against whichever pipeline happens to be running.
     */
    static synchronized void installSharedChannel() {
        if (sharedChannelInstalled) {
            return;
        }
        sharedChannelInstalled = true;
        TapLogger.setLogListener(new SharedChannel());
    }

    @Override
    public void debug(String message, Object... params) {
        host.debug(message, params);
    }

    @Override
    public void info(String message, Object... params) {
        host.debug(message, params);
    }

    @Override
    public void trace(String message, Object... params) {
        host.trace(message, params);
    }

    @Override
    public void warn(String message, Object... params) {
        attributed(() -> host.warn(message, params));
    }

    @Override
    public void error(String message, Object... params) {
        attributed(() -> host.error(message, params));
    }

    @Override
    public void error(String message, Throwable throwable) {
        attributed(() -> host.error(message, throwable));
    }

    @Override
    public void fatal(String message, Object... params) {
        attributed(() -> host.error(message, params));
    }

    /** Writes with this connector's pipeline named, putting back whatever the thread carried before. */
    private void attributed(Runnable write) {
        if (pipelineId == null) {
            write.run();
            return;
        }
        String previous = ConnectorAttribution.claim(pipelineId);
        try {
            write.run();
        } finally {
            ConnectorAttribution.restore(previous);
        }
    }

    /**
     * The contract's static channel, arriving with its message already rendered. Its lines are attributed
     * by the drive that is running, if one is -- see {@link #installSharedChannel()}.
     */
    private static final class SharedChannel implements TapLogger.LogListener {

        private static final long serialVersionUID = 1L;

        private static final Logger SHARED = LoggerFactory.getLogger(LOGGER_PREFIX + "shared");

        @Override
        public void debug(String message) {
            SHARED.debug(message);
        }

        @Override
        public void info(String message) {
            SHARED.debug(message);
        }

        @Override
        public void warn(String message) {
            SHARED.warn(message);
        }

        @Override
        public void error(String message) {
            SHARED.error(message);
        }

        @Override
        public void fatal(String message) {
            SHARED.error(message);
        }

        @Override
        public void memory(String message) {
            SHARED.trace(message);
        }
    }
}
