package io.tapstate.adapters.mongostore;

/** Exercises only the latest-observation binary encoder from the selected immutable artifact. */
public final class BenchmarkJdiLatestEncoderTarget {

    private BenchmarkJdiLatestEncoderTarget() {
    }

    public static void main(String[] args) {
        try {
            BenchmarkJdiEncoderTarget.phase("LATEST_ORIGINS");
            BenchmarkJdiEncoderTarget.origin(LatestObservationPayloadCodec.class);
            BenchmarkJdiEncoderTarget.origin(io.tapstate.core.lifecycle.Observation.class);
            BenchmarkJdiEncoderTarget.ready();
            BenchmarkJdiEncoderTarget.phase("LATEST_ENCODING");
            for (int i = 0; i < 3; i++) {
                var encoded = LatestObservationPayloadCodec.encode(
                        BenchmarkJdiEncoderTarget.observation(), Chunks.INSTANCE);
                if (!encoded.inline() || encoded.encodedBytes() <= 0) {
                    throw new AssertionError("the small observation was not encoded inline");
                }
            }
            BenchmarkJdiEncoderTarget.done();
        } catch (Throwable failure) {
            BenchmarkJdiEncoderTarget.reportFailure(failure);
            System.exit(2);
        }
    }

    private enum Chunks implements LatestObservationPayloadCodec.ChunkWriter {
        INSTANCE;

        @Override
        public void begin() {
            throw new AssertionError("the inline witness must not open chunk storage");
        }

        @Override
        public void write(LatestObservationPayloadCodec.Chunk chunk) {
            throw new AssertionError("the inline witness must not write a chunk");
        }
    }
}
