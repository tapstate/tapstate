package io.tapstate.spi.store;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One fixed CACHE command over selected shared chains and private source channels. */
@FunctionalInterface
public interface SourcePositionEdits {
    void rewrite(List<Move> moves, Map<PipelineIntentScope, WorkloadClaimFence> readers, Instant editedAt);

    sealed interface Move permits Logged, Direct {
        String token();
        WorkloadClaimFence capture();
    }
    record Logged(SrsChainState original, Map<SrsConsumerContext, SrsConsumerProgress> consumers,
                  String token, WorkloadClaimFence capture) implements Move {
        public Logged {
            Objects.requireNonNull(original); consumers = Map.copyOf(consumers);
            Objects.requireNonNull(token); Objects.requireNonNull(capture);
        }
    }
    record Direct(DirectChannelProgress original, String token, WorkloadClaimFence capture) implements Move {
        public Direct { Objects.requireNonNull(original); Objects.requireNonNull(token); Objects.requireNonNull(capture); }
    }
}
