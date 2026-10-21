package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CloudArtifactReadStepTest {
    private static final CloudArtifactReadStep.Frame ARTIFACT =
            frame("io.tapstate.adapters.mongostore.MongoConnectorRegistry", "artifact");
    private static final CloudArtifactReadStep.Frame METADATA =
            frame("com.mongodb.client.gridfs.GridFSFindIterableImpl", "first");
    private static final CloudArtifactReadStep.Frame DOWNLOAD =
            frame("com.mongodb.client.gridfs.GridFSBucketImpl", "downloadToStream");

    @Test
    void originalMetadataFirstIsIdentified() {
        assertThat(CloudArtifactReadStep.classify(List.of(METADATA, ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.METADATA_FIND);
    }

    @Test
    void originalDownloadIsIdentified() {
        assertThat(CloudArtifactReadStep.classify(List.of(DOWNLOAD, ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.DOWNLOAD);
    }

    @Test
    void aFileLookupInsideDownloadRemainsADownload() {
        assertThat(CloudArtifactReadStep.classify(List.of(
                frame("com.mongodb.client.internal.FindIterableImpl", "first"), DOWNLOAD, ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.DOWNLOAD);
        assertThat(CloudArtifactReadStep.classify(List.of(METADATA, DOWNLOAD, ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.DOWNLOAD);
    }

    @Test
    void genericFindFramesDoNotProveAnArtifactStep() {
        assertThat(CloudArtifactReadStep.classify(List.of(
                frame("com.mongodb.client.internal.FindIterableImpl", "first"), ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
    }

    @Test
    void similarlyNamedForeignFramesStayUnclassified() {
        assertThat(CloudArtifactReadStep.classify(List.of(
                frame("untrusted.GridFSFindIterableImpl", "first"), ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
        assertThat(CloudArtifactReadStep.Step.UNCLASSIFIED.wireValue()).isEqualTo("unclassified");
    }

    @Test
    void aDriverFrameWithoutTheOriginalArtifactCallerStaysUnclassified() {
        assertThat(CloudArtifactReadStep.classify(List.of(METADATA)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
        assertThat(CloudArtifactReadStep.classify(List.of(DOWNLOAD)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
    }

    @Test
    void incompleteFramesStayUnclassified() {
        assertThat(CloudArtifactReadStep.classify(null)).isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
        assertThat(CloudArtifactReadStep.classify(Arrays.asList(METADATA, null, ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
        assertThat(CloudArtifactReadStep.classify(List.of(METADATA, frame(null, "first"), ARTIFACT)))
                .isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
    }

    @Test
    void aTruncatedStackCannotProveAPhase() {
        List<CloudArtifactReadStep.Frame> frames = new ArrayList<>();
        frames.add(METADATA);
        frames.add(ARTIFACT);
        for (int index = 0; index < 255; index++) frames.add(frame("unrelated.Frame", "call"));
        assertThat(CloudArtifactReadStep.classify(frames)).isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
    }

    @Test
    void anActualStackOutsideTheArtifactCallStaysUnclassified() {
        assertThat(CloudArtifactReadStep.current()).isEqualTo(CloudArtifactReadStep.Step.UNCLASSIFIED);
    }

    private static CloudArtifactReadStep.Frame frame(String className, String methodName) {
        return new CloudArtifactReadStep.Frame(className, methodName);
    }
}
