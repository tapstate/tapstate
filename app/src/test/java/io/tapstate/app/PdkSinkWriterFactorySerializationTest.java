package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The sink factory is shipped onto the Jet DAG, so it and everything it carries must serialize. This guards
 * that a factory holding a resolved target model round-trips, which the engine relies on to run the sink on
 * the member that opens the connector.
 */
class PdkSinkWriterFactorySerializationTest {

    @Test
    void serializes_with_its_resolved_target_so_it_can_ship_onto_the_dag() throws Exception {
        PdkSinkWriterFactory factory = new PdkSinkWriterFactory(
                "mongodb", Map.of("uri", "u"), WriteMode.UPSERT, DdlPolicy.APPLY,
                new TargetTable("orders", List.of(
                        new TargetField("id", "INT", true),
                        new TargetField("amount", "DECIMAL", false))),
                new PipelineNode("p1", "to_mongo"));

        Object restored = roundTrip(factory);

        assertThat(restored).isInstanceOf(PdkSinkWriterFactory.class);
    }

    /**
     * The node has to survive the trip, because the member that deserializes this factory is where the
     * connector is actually opened and there is nothing there to re-derive it from. A node that did not
     * travel arrives null, and a null node is a legitimate value meaning "scope nothing" - so the sink
     * would open, write every row correctly, and quietly keep the connector's own notes nowhere. Nothing
     * downstream of the write can see the difference.
     */
    @Test
    void the_node_it_writes_for_survives_the_trip_onto_the_dag() throws Exception {
        PdkSinkWriterFactory factory = new PdkSinkWriterFactory(
                "mongodb", Map.of("uri", "u"), WriteMode.UPSERT, DdlPolicy.APPLY,
                (TargetTable) null, new PipelineNode("p1", "to_mongo"));

        Object restored = roundTrip(factory);

        assertThat(((PdkSinkWriterFactory) restored).node()).isEqualTo(new PipelineNode("p1", "to_mongo"));
    }

    private static Object roundTrip(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return in.readObject();
        }
    }
}
