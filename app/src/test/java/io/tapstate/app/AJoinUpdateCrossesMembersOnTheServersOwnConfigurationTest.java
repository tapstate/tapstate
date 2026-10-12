package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.join.JoinUpdate;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What a join hands the projection that shapes it can be written for another member, on the server's own member
 * configuration.
 *
 * <p>The update is routed to the projection by its fact key, so on more than one member most updates go to another
 * member and are written for the crossing. Nothing but the member's configuration decides whether they can be: an
 * update with no serializer of its own falls back to Java's serialization, which cannot write the change inside it,
 * and the job dies on the first update routed away. That shows only on more than one member, and only when the
 * placement puts the join and its projection apart, so it is asserted here, on the configuration every member is
 * started with.
 */
class AJoinUpdateCrossesMembersOnTheServersOwnConfigurationTest {

    @Test
    void aJoinUpdateWrittenForAnotherMemberComesBackAsItWas() {
        InternalSerializationService serialization = new DefaultSerializationServiceBuilder()
                .setConfig(HazelcastConfiguration.memberConfig(new HazelcastProperties()).getSerializationConfig())
                .build();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 100);
        row.put("name", "ada");
        JoinUpdate written = new JoinUpdate("100", Envelope.insert(7L, "orders", row, null)
                .withOrder(new SourceOrder(2, 5)));

        JoinUpdate read = serialization.toObject(serialization.toData(written));

        assertThat(read.factKey()).isEqualTo("100");
        assertThat(read.event().after()).isEqualTo(row);
        assertThat(read.event().src()).isEqualTo("orders");
        assertThat(read.event().positions())
                .as("where the change sits travels with it, as it does on every other edge")
                .isEqualTo(written.event().positions());
    }
}
