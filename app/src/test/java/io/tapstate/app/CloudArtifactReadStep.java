package io.tapstate.app;

import java.util.List;

/** Fixed labels from the current synchronous driver call, without inspecting an exception. */
final class CloudArtifactReadStep {
    private static final int MAX_FRAMES = 256;
    private static final String REGISTRY = "io.tapstate.adapters.mongostore.MongoConnectorRegistry";
    private static final String BUCKET = "com.mongodb.client.gridfs.GridFSBucketImpl";
    private static final String FIND = "com.mongodb.client.gridfs.GridFSFindIterableImpl";

    private CloudArtifactReadStep() {}

    static Step current() {
        try {
            return StackWalker.getInstance().walk(frames -> classify(frames.limit(MAX_FRAMES + 1L)
                    .map(frame -> new Frame(frame.getClassName(), frame.getMethodName())).toList()));
        } catch (RuntimeException unavailable) {
            return Step.UNCLASSIFIED;
        }
    }

    static Step classify(List<Frame> frames) {
        if (frames == null || frames.size() > MAX_FRAMES) return Step.UNCLASSIFIED;
        boolean originalArtifact = false;
        boolean download = false;
        boolean metadataFind = false;
        for (Frame frame : frames) {
            if (frame == null || frame.className() == null || frame.methodName() == null) {
                return Step.UNCLASSIFIED;
            }
            originalArtifact |= REGISTRY.equals(frame.className()) && "artifact".equals(frame.methodName());
            download |= BUCKET.equals(frame.className()) && "downloadToStream".equals(frame.methodName());
            metadataFind |= FIND.equals(frame.className())
                    && ("first".equals(frame.methodName()) || "into".equals(frame.methodName()));
        }
        if (!originalArtifact) return Step.UNCLASSIFIED;
        // A download can perform its own file lookup before reading chunks.
        if (download) return Step.DOWNLOAD;
        return metadataFind ? Step.METADATA_FIND : Step.UNCLASSIFIED;
    }

    record Frame(String className, String methodName) {}

    enum Step {
        METADATA_FIND("metadata-find"), DOWNLOAD("download"), UNCLASSIFIED("unclassified");

        private final String wireValue;

        Step(String wireValue) { this.wireValue = wireValue; }

        String wireValue() { return wireValue; }
    }
}
