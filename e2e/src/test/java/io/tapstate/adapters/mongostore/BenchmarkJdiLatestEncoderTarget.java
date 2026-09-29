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
            if (args[0].equals("latest-chunk")) {
                chunk();
                return;
            }
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

    private static void chunk() throws Exception {
        var source = new io.tapstate.core.lifecycle.Observation("proof",
                io.tapstate.core.lifecycle.PipelineState.RUNNING, java.util.Map.of(), java.util.Map.of(),
                java.util.Map.of("orders", "x".repeat(600 * 1024)), null, java.time.Instant.ofEpochSecond(1_000));
        ChunkFixture chunks = new ChunkFixture();
        BenchmarkJdiEncoderTarget.ready();
        BenchmarkJdiEncoderTarget.phase("LATEST_ENCODING");
        var encoded = LatestObservationPayloadCodec.encode(source, chunks);
        if (encoded.inline() || encoded.inlinePayload() != null || encoded.chunkCount() != 1
                || chunks.begins != 1 || chunks.parts.size() != 1
                || chunks.parts.getFirst().ordinal() != 0
                || chunks.parts.getFirst().payload().length != encoded.encodedBytes()) {
            throw new AssertionError("the nonempty chunk encoding exceeded its fixed callback budget");
        }
        BenchmarkJdiEncoderTarget.done();
        // Decoding verifies the bytes after the encoder window has closed.
        if (!LatestObservationPayloadCodec.decodeChunks(chunks.parts, encoded.chunkCount(),
                encoded.encodedBytes(), encoded.payloadDigest()).equals(source)) {
            throw new AssertionError("the protected chunk encoding did not round trip");
        }
    }

    private static final class ChunkFixture implements LatestObservationPayloadCodec.ChunkWriter {
        private int begins;
        private final java.util.List<LatestObservationPayloadCodec.Chunk> parts = new java.util.ArrayList<>();

        @Override public void begin() {
            if (++begins != 1) { throw new AssertionError("chunk storage began more than once"); }
        }

        @Override public void write(LatestObservationPayloadCodec.Chunk chunk) {
            if (begins != 1 || !parts.isEmpty()) { throw new AssertionError("an extra chunk was written"); }
            parts.add(chunk);
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
