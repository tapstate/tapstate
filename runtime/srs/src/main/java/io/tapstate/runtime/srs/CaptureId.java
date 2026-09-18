package io.tapstate.runtime.srs;

import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Stable identity of one normalized source/read contract, shared by every pipeline using it.
 * Pipeline/source node identity is deliberately excluded; connector settings, selected streams, explicit
 * mining key and the tail shape are included so contracts that cannot share a reader cannot collide.
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
        return of(config, srsKey, "tail:ring");
    }

    /** Derives the product capture identity from the normalized source and read axes of a run. */
    public static CaptureId of(CaptureRunSpec spec) {
        Objects.requireNonNull(spec, "spec");
        String readAxis = spec.readMode() == ReadMode.SNAPSHOT_ONLY
                ? "snapshot"
                : spec.srsEnabled() ? "tail:ring" : "tail:direct:" + directStart(spec.startFrom());
        return of(spec.config(), spec.srsKey(), readAxis);
    }

    private static String directStart(StartFrom startFrom) {
        return switch (startFrom) {
            case StartFrom.Earliest ignored -> "earliest";
            case StartFrom.Latest ignored -> "latest";
            case StartFrom.At at -> "at:" + at.instant();
        };
    }

    private static CaptureId of(CaptureConfig config, String srsKey, String readAxis) {
        Objects.requireNonNull(config, "config");
        List<String> streams = new ArrayList<>(Objects.requireNonNull(config.streams(), "config.streams"));
        streams.sort(String::compareTo);
        StringBuilder contract = new StringBuilder(MiningChainId.resolve(config, srsKey).value())
                .append('|').append(readAxis);
        contract.append('|').append(streams.size());
        for (String stream : streams) {
            contract.append('|').append(stream.length()).append(':').append(stream);
        }
        return new CaptureId(PREFIX + CanonicalHash.ofText(contract.toString()));
    }
}
