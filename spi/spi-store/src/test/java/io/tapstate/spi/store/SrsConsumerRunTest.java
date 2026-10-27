package io.tapstate.spi.store;

import java.io.*;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SrsConsumerRunTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster",3,"cache","pipeline",7);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"pipeline"),
            new WorkloadOwner("node","boot"),2,5,9);
    private static SrsConsumerMembership member(boolean detached,long generation) {
        return new SrsConsumerMembership(new SrsConsumerContext(new SrsConsumerKey("chain","pipeline","source"),generation),4,detached,
                detached ? Set.of() : Set.of("orders"),FENCE);
    }
    @Test void originalCoordinatesRoundTripWithoutSerializingAStoreOrClaimEnum() throws Exception {
        var original = SrsConsumerRun.of(SCOPE,member(false,7)); var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) { out.writeObject(original); }
        SrsConsumerRun copy;
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) { copy = (SrsConsumerRun)in.readObject(); }
        assertThat(copy).isEqualTo(original); assertThat(copy.context()).isEqualTo(member(false,7).context());
        assertThat(copy.originalClaim()).isEqualTo(FENCE); assertThat(copy.cacheScope()).isEqualTo(new CacheBusinessScope("cluster",3,"cache"));
        assertThat(new String(bytes.toByteArray(),java.nio.charset.StandardCharsets.ISO_8859_1)).doesNotContain("WorkloadClaimType","WorkloadClaimFence");
    }
    @Test void detachedOrDifferentPhysicalStateCannotBecomeAnOriginalRun() {
        assertThatThrownBy(() -> SrsConsumerRun.of(SCOPE,member(true,7))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrsConsumerRun.of(SCOPE,member(false,8))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrsConsumerRun.of(new PipelineIntentScope("other",3,"cache","pipeline",7),member(false,7))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SrsConsumerRun(SCOPE,"chain","source",4,"node","boot",2,0,9)).isInstanceOf(IllegalArgumentException.class);
    }
}
