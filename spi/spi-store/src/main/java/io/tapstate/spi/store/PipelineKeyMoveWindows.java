package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** A recovery interlock in the original configuration database, independent of the target. */
public interface PipelineKeyMoveWindows {
    /** Physical source coordinates needed to inspect the original progress before a new reader opens. */
    record Stream(String chain, String source, String table, String privateCapture) implements java.io.Serializable {
        public Stream {
            if (chain.isBlank() || source.isBlank() || table.isBlank() || privateCapture != null && privateCapture.isBlank())
                throw new IllegalArgumentException("missing physical stream identity");
        }
    }

    record Range(long epoch, long first, long last) {
        public Range {
            if (epoch < 0 || first < 0 || last < first) throw new IllegalArgumentException("invalid change interval");
        }

        public boolean coveredBy(SourceOrder floor) {
            return floor != null && floor.epoch() == epoch && floor.seq() >= last;
        }
    }

    record Window(PipelineIntentScope scope, long executionId, String activation, UUID revision, Map<Stream, Range> ranges) {
        public Window {
            Objects.requireNonNull(scope);
            if (activation.isBlank()) throw new IllegalArgumentException("missing pipeline activation");
            Objects.requireNonNull(revision);
            ranges = Map.copyOf(ranges);
            if (ranges.isEmpty())
                throw new IllegalArgumentException("missing change stream");
        }
    }

    Optional<Window> read(String pipelineId);

    /** Must return durably before releasing a change to any operator. */
    Window arm(PipelineIntentScope scope, long executionId, String activation, Stream stream, Range changes,
            Window lastAdmission, Runnable verifyAuthority);

    /** The two floors come from separate authorities. Each must cover every affected stream. */
    boolean retire(Window observed, Map<Stream, SourceOrder> resumable, Map<Stream, SourceOrder> settled);

    default void requireClear(String pipelineId) {
        if (read(pipelineId).isPresent()) throw unresolved(pipelineId);
    }

    static TapstateException unresolved(String pipelineId) {
        return new TapstateException(IoError.KEY_MOVE_RECOVERY_UNCERTAIN, Map.of("pipeline", pipelineId), null);
    }
}
