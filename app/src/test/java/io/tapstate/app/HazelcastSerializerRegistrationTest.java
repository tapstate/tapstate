package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.EnvelopeSerializer;
import io.tapstate.runtime.engine.FiniteEnvelopeSourceProcessor;
import io.tapstate.runtime.engine.join.JoinUpdate;
import io.tapstate.runtime.engine.join.JoinUpdateSerializer;
import io.tapstate.runtime.srs.SrsItemSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class HazelcastSerializerRegistrationTest {

    @Test
    void productionMemberStartsAndRoundTripsPreviewAndJoinValuesWithDistinctWireIds() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("serializer-registration-" + UUID.randomUUID());
        properties.setMemberPort(0);
        properties.getJet().setCooperativeThreadCount(2);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);

        HazelcastInstance member = assertDoesNotThrow(() -> Hazelcast.newHazelcastInstance(config),
                "the production member configuration must start with every registered serializer");
        try {
            assertThat(SrsItemSerializer.TYPE_ID).isEqualTo(10001);
            assertThat(EnvelopeSerializer.TYPE_ID).isEqualTo(10002);
            assertThat(JoinUpdateSerializer.TYPE_ID).isEqualTo(10003);
            assertThat(FiniteEnvelopeSourceProcessor.SampleSerializer.TYPE_ID).isEqualTo(10004);

            Instant sampledAt = Instant.parse("2026-10-10T05:30:00.123456789Z");
            Envelope row = Envelope.read(sampledAt.toEpochMilli(), "source.orders", Map.of("id", 1), Map.of());
            PreviewSampleCache.Entry cache = new PreviewSampleCache.Entry(List.of(row), true, sampledAt);
            var cached = member.<String, PreviewSampleCache.Entry>getMap(PreviewSampleCache.MAP_NAME);
            cached.put("sample", cache);
            assertThat(cached.get("sample")).isEqualTo(cache);

            JoinUpdate join = new JoinUpdate("fact-key", row);
            FiniteEnvelopeSourceProcessor.Sample finite = new FiniteEnvelopeSourceProcessor.Sample(List.of(row));
            var values = member.<String, Object>getMap("serializer-registration-values");
            values.put("join", join);
            values.put("finite", finite);
            assertThat(values.get("join")).isEqualTo(join);
            assertThat(values.get("finite")).isEqualTo(finite);
        } finally {
            member.shutdown();
        }
    }
}
