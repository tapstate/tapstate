package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.Stage;

/** Fixed component names for ordinary output refusal and completed retry intervals. */
public final class OutputPressureMetricNames {

    private static final String PREFIX = "stage.";
    private static final String OUTPUT = ".output.";

    public enum Kind {
        READY("ready"), SINCE("since"), REFUSED("refused"),
        COUNT("retry.count"), SUM_NANOS("retry.sumNanos"), BUCKET("retry.bucket.");

        private final String suffix;

        Kind(String suffix) {
            this.suffix = suffix;
        }
    }

    public record Part(String stage, Kind kind, int bucket) { }

    private OutputPressureMetricNames() { }

    public static String name(Stage stage, Kind kind) {
        return PREFIX + stage.attributeValue() + OUTPUT + kind.suffix;
    }

    public static String bucketName(Stage stage, int bucket) {
        if (bucket < 0 || bucket >= HistogramBounds.PROCESS_DURATION.buckets()) {
            throw new IllegalArgumentException("output retry bucket is outside the fixed bounds");
        }
        return name(stage, Kind.BUCKET) + bucket;
    }

    public static Part partOf(String name) {
        if (!name.startsWith(PREFIX)) {
            return null;
        }
        int split = name.indexOf(OUTPUT, PREFIX.length());
        if (split < 0) {
            return null;
        }
        String stage = name.substring(PREFIX.length(), split);
        if (!Stage.attributeValues().contains(stage)) {
            return null;
        }
        String suffix = name.substring(split + OUTPUT.length());
        for (Kind kind : Kind.values()) {
            if (kind != Kind.BUCKET && suffix.equals(kind.suffix)) {
                return new Part(stage, kind, -1);
            }
        }
        if (suffix.startsWith(Kind.BUCKET.suffix)) {
            try {
                int bucket = Integer.parseInt(suffix.substring(Kind.BUCKET.suffix.length()));
                if (bucket >= 0 && bucket < HistogramBounds.PROCESS_DURATION.buckets()) {
                    return new Part(stage, Kind.BUCKET, bucket);
                }
            } catch (NumberFormatException invalid) {
                return null;
            }
        }
        return null;
    }
}
