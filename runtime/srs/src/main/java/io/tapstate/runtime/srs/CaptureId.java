package io.tapstate.runtime.srs;

import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Stable identity of one normalized source/read contract, shared by every pipeline using it.
 * Pipeline/source node identity is deliberately excluded; connector settings, explicit mining key and the
 * tail shape are included so contracts that cannot share a reader cannot collide.
 *
 * <p>A tail through the shared ring is identified by its mining chain alone, not by the tables a pipeline
 * selects from it. A chain is read from its source once, for every pipeline on it, over the union of what
 * they select; two captures of one chain would be two readers moving one recorded position, each unaware of
 * the tables the other has not landed. A bounded read names its streams, because each one reads exactly
 * those.
 *
 * <p>The one read no two pipelines can share is a direct tail -- one with the shared ring switched off --
 * which streams to the pipeline that opened it and writes nothing anybody else could read. Its identity
 * therefore names that pipeline and source as well, so each such pipeline holds a capture of its own.
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
            return of(spec.config(), spec.srsKey(), "snapshot", true);
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
