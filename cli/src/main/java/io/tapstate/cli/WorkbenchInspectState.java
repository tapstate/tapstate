package io.tapstate.cli;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Current, selected-Pipeline observability data rendered by the Inspect tab. */
sealed interface WorkbenchInspectState {

    String pipelineId();

    record Loading(String pipelineId) implements WorkbenchInspectState {
        public Loading {
            Objects.requireNonNull(pipelineId, "pipelineId");
        }
    }

    record Available(
            String pipelineId,
            Map<String, Long> metrics,
            Map<String, String> targetAckedPosition,
            List<String> positionsNotCollected,
            List<MetricsOutcome.FactPoint> facts,
            MovementReading previous,
            MovementReading current,
            Instant receivedAt) implements WorkbenchInspectState {
        public Available {
            Objects.requireNonNull(pipelineId, "pipelineId");
            metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
            targetAckedPosition = targetAckedPosition == null ? Map.of() : Map.copyOf(targetAckedPosition);
            positionsNotCollected = positionsNotCollected == null ? List.of() : List.copyOf(positionsNotCollected);
            facts = facts == null ? List.of() : List.copyOf(facts);
            Objects.requireNonNull(receivedAt, "receivedAt");
        }
    }

    record Rejected(String pipelineId, String code, String message) implements WorkbenchInspectState {
        public Rejected {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(code, "code");
            Objects.requireNonNull(message, "message");
        }
    }

    record Unreachable(String pipelineId) implements WorkbenchInspectState {
        public Unreachable {
            Objects.requireNonNull(pipelineId, "pipelineId");
        }
    }

    record Unavailable(String pipelineId) implements WorkbenchInspectState {
        public Unavailable {
            Objects.requireNonNull(pipelineId, "pipelineId");
        }
    }
}
