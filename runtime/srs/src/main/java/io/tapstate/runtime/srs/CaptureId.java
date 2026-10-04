package io.tapstate.runtime.srs;

import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Stable identity of one normalized source/read contract, shared by every pipeline using it.
 * Pipeline/source node identity is deliberately excluded from a shared tail: every table selection on
 * one physical mining chain is served by the same capture. Bounded and direct reads name their streams
 * because they read only that selection and belong to their own pipeline/source node.
 *
 * <p>A direct tail -- one with the shared ring switched off -- streams to the pipeline that opened it
 * and writes nothing anybody else could read. A bounded snapshot carries that pipeline's own rows.
 * Their identities therefore name the pipeline and source, so each pipeline holds its own capture.
 */
public record CaptureId(String value) {

    private static final String PREFIX = "capture-";

    public CaptureId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("capture id value must be non-blank");
        }
    }

    /** Derives identity without the pipeline-scoped node carried by a connector invocation. */
    public static CaptureId of(CaptureConfig config, String srsKey) {
        return of(config, srsKey, "tail:ring", false);
    }

    /** Derives the product capture identity from the normalized source and read axes of a run. */
    public static CaptureId of(CaptureRunSpec spec) {
        Objects.requireNonNull(spec, "spec");
        if (spec.readMode() == ReadMode.SNAPSHOT_ONLY) {
            String reader = Objects.requireNonNull(spec.pipelineId(), "spec.pipelineId");
            String source = Objects.requireNonNull(spec.sourceId(), "spec.sourceId");
            return of(spec.config(), spec.srsKey(), "snapshot:"
                    + reader.length() + ':' + reader + '|' + source.length() + ':' + source, true);
        }
        if (spec.srsEnabled()) {
            return of(spec.config(), spec.srsKey(), "tail:ring", false);
        }
        String reader = Objects.requireNonNull(spec.pipelineId(), "spec.pipelineId");
        String source = Objects.requireNonNull(spec.sourceId(), "spec.sourceId");
        return of(spec.config(), spec.srsKey(), "tail:direct:" + directStart(spec.startFrom())
                + '|' + reader.length() + ':' + reader + '|' + source.length() + ':' + source, true);
    }

    private static String directStart(StartFrom startFrom) {
        return switch (startFrom) {
            case StartFrom.Earliest ignored -> "earliest";
            case StartFrom.Latest ignored -> "latest";
            case StartFrom.At at -> "at:" + at.instant();
        };
    }

    private static CaptureId of(CaptureConfig config, String srsKey, String readAxis, boolean namesStreams) {
        Objects.requireNonNull(config, "config");
        StringBuilder contract = new StringBuilder(MiningChainId.resolve(config, srsKey).value())
                .append('|').append(readAxis);
        if (namesStreams) {
            List<String> streams = new ArrayList<>(Objects.requireNonNull(config.streams(), "config.streams"));
            streams.sort(String::compareTo);
            contract.append('|').append(streams.size());
            for (String stream : streams) {
                contract.append('|').append(stream.length()).append(':').append(stream);
            }
        }
        return new CaptureId(PREFIX + CanonicalHash.ofText(contract.toString()));
    }
}
