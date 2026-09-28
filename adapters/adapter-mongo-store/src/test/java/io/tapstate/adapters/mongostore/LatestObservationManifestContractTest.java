package io.tapstate.adapters.mongostore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LatestObservationManifestContractTest {

    @Test
    void manifestOwnerAndChunkKeysAreFixedDomainSeparatedIdentities() {
        var firstKey = MongoLatestObservationStorage.manifestKey("orders");
        var firstOwner = MongoLatestObservationStorage.ownerDigest("orders");
        var secondKey = MongoLatestObservationStorage.manifestKey("customers");

        assertThat(firstKey.getData()).hasSize(32).isNotEqualTo(firstOwner.getData());
        assertThat(secondKey.getData()).hasSize(32).isNotEqualTo(firstKey.getData());
        assertThat(MongoLatestObservationStorage.manifestKey("orders")).isEqualTo(firstKey);
        assertThat(MongoLatestObservationStorage.ownerDigest("orders")).isEqualTo(firstOwner);

        assertThat(MongoLatestObservationStorage.chunkId(firstKey, firstOwner, "token-a", 0))
                .isEqualTo(MongoLatestObservationStorage.chunkId(firstKey, firstOwner, "token-a", 0))
                .isNotEqualTo(MongoLatestObservationStorage.chunkId(firstKey, firstOwner, "token-a", 1))
                .isNotEqualTo(MongoLatestObservationStorage.chunkId(firstKey, firstOwner, "token-b", 0))
                .isNotEqualTo(MongoLatestObservationStorage.chunkId(secondKey, firstOwner, "token-a", 0));
    }

    @Test
    void retirementGraceOutlivesTheWholeReadAndHeartbeatRenewsBeforeLeaseExpiry() {
        assertThat(MongoLatestObservationStorage.RETIRE_GRACE_SECONDS)
                .isGreaterThan(MongoLatestObservationStorage.READ_DEADLINE_SECONDS);
        assertThat(MongoLatestObservationStorage.PUBLISH_HEARTBEAT_NANOS)
                .isLessThan(java.util.concurrent.TimeUnit.SECONDS.toNanos(
                        MongoLatestObservationStorage.PUBLISH_LEASE_SECONDS));
    }

    @Test
    void physicalFormatBudgetsAndVersionStayFixed() {
        assertThat(LatestObservationPayloadCodec.ENCODING_VERSION).isEqualTo(2);
        assertThat(MongoLatestObservationStorage.FORMAT_VERSION).isEqualTo(1);
        assertThat(LatestObservationPayloadCodec.INLINE_PAYLOAD_LIMIT).isEqualTo(512 * 1024);
        assertThat(LatestObservationPayloadCodec.CHUNK_PAYLOAD_LIMIT).isEqualTo(1024 * 1024);
        assertThat(MongoLatestObservationStorage.READ_CHUNK_BATCH_SIZE).isEqualTo(4);
        assertThat(MongoLatestObservationStorage.READ_ATTEMPT_SECONDS).isEqualTo(4);
        assertThat(MongoLatestObservationStorage.READ_DEADLINE_SECONDS).isEqualTo(10);
        assertThat(MongoLatestObservationStorage.RETIRE_GRACE_SECONDS).isEqualTo(15);
    }

    @Test
    void manifestKeyMatchesTheRawUtf8Sha256Vector() throws java.security.NoSuchAlgorithmException {
        assertThat(MongoLatestObservationStorage.manifestKey("orders").getData()).containsExactly(
                java.security.MessageDigest.getInstance("SHA-256")
                        .digest("orders".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }
}
