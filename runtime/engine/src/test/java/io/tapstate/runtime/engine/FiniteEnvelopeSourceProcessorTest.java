package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FiniteEnvelopeSourceProcessorTest {

    private final InternalSerializationService serialization = new DefaultSerializationServiceBuilder()
            .setConfig(new Config().getSerializationConfig()
                    .addSerializerConfig(new SerializerConfig()
                            .setTypeClass(Envelope.class)
                            .setImplementation(new EnvelopeSerializer()))
                    .addSerializerConfig(new SerializerConfig()
                            .setTypeClass(FiniteEnvelopeSourceProcessor.Sample.class)
                            .setImplementation(new FiniteEnvelopeSourceProcessor.SampleSerializer())))
            .build();

    @Test
    void sourceVertexSupplierCarriesOnlySerializableCoordinates() {
        assertThatCode(() -> new DAG().newVertex("sample", FiniteEnvelopeSourceProcessor.metaSupplier(
                "sample", "__preview.inputs.run", "crm_customers")))
                .doesNotThrowAnyException();
    }

    @Test
    void emitsTheStoredSampleAcrossOutputBackpressureThenCompletes() throws Exception {
        Config config = new Config();
        config.setClusterName("finite-preview-source-" + java.util.UUID.randomUUID());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                        .setTypeClass(Envelope.class)
                        .setImplementation(new EnvelopeSerializer()))
                .addSerializerConfig(new SerializerConfig()
                        .setTypeClass(FiniteEnvelopeSourceProcessor.Sample.class)
                        .setImplementation(new FiniteEnvelopeSourceProcessor.SampleSerializer()));
        var member = Hazelcast.newHazelcastInstance(config);
        try {
            Envelope first = Envelope.read(1L, "orders", Map.of("id", 1), Map.of());
            Envelope second = Envelope.read(2L, "orders", Map.of("id", 2), Map.of());
            member.<String, FiniteEnvelopeSourceProcessor.Sample>getMap("samples")
                    .put("orders", new FiniteEnvelopeSourceProcessor.Sample(List.of(first, second)));
            FiniteEnvelopeSourceProcessor processor = source("samples", "orders");
            TestOutbox outbox = new TestOutbox(1);
            processor.init(outbox, new TestProcessorContext());

            assertThat(processor.complete()).isFalse();
            List<Object> emitted = new ArrayList<>();
            outbox.drainQueueAndReset(0, emitted, false);
            assertThat(emitted).containsExactly(first);
            assertThat(processor.complete()).isTrue();
            outbox.drainQueueAndReset(0, emitted, false);
            assertThat(emitted).containsExactly(first, second);
            processor.close();
        } finally {
            member.shutdown();
        }
    }

    @Test
    void stagedSampleUsesEnvelopeWireFormatAndKeepsTypedRows() {
        Envelope envelope = Envelope.insert(7L, "crm_customers",
                Map.of("customer_id", 42L, "company_name", "Acme"), null)
                .withOrder(new SourceOrder(3L, 17L));
        FiniteEnvelopeSourceProcessor.Sample written = new FiniteEnvelopeSourceProcessor.Sample(
                List.of(envelope));

        FiniteEnvelopeSourceProcessor.Sample read = serialization.toObject(serialization.toData(written));

        assertThat(read.rows()).containsExactly(envelope);
        assertThat(read.rows().getFirst()).isNotSameAs(envelope);
    }

    @Test
    void validatesCoordinatesAndKeepsSamplesBoundedAndImmutable() {
        assertThatThrownBy(() -> FiniteEnvelopeSourceProcessor.metaSupplier(null, "samples", "orders"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FiniteEnvelopeSourceProcessor.metaSupplier("source", null, "orders"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FiniteEnvelopeSourceProcessor.metaSupplier("source", "samples", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new FiniteEnvelopeSourceProcessor.Sample(null))
                .isInstanceOf(NullPointerException.class);

        Envelope row = Envelope.read(1L, "orders", Map.of("id", 1), Map.of());
        List<Envelope> mutable = new java.util.ArrayList<>(List.of(row));
        FiniteEnvelopeSourceProcessor.Sample sample = new FiniteEnvelopeSourceProcessor.Sample(mutable);
        mutable.clear();
        assertThat(sample.rows()).containsExactly(row);
        assertThatThrownBy(() -> sample.rows().add(row)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new FiniteEnvelopeSourceProcessor.Sample(
                Collections.nCopies(FiniteEnvelopeSourceProcessor.Sample.MAX_ROWS + 1, row)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds its bound");
    }

    @Test
    void rejectsInvalidSerializedSampleSizesAndValues() throws IOException {
        FiniteEnvelopeSourceProcessor.SampleSerializer serializer =
                new FiniteEnvelopeSourceProcessor.SampleSerializer();
        assertThat(serializer.getTypeId()).isEqualTo(10004);
        assertThatThrownBy(() -> serializer.read(input(-1))).isInstanceOf(IOException.class)
                .hasMessageContaining("out of range");
        assertThatThrownBy(() -> serializer.read(input(FiniteEnvelopeSourceProcessor.Sample.MAX_ROWS + 1)))
                .isInstanceOf(IOException.class).hasMessageContaining("out of range");
        assertThatThrownBy(() -> serializer.read(input(1, "not an envelope")))
                .isInstanceOf(IOException.class).hasMessageContaining("non-envelope");
    }

    private static com.hazelcast.nio.ObjectDataInput input(Object... values) {
        Iterator<Object> next = List.of(values).iterator();
        return (com.hazelcast.nio.ObjectDataInput) Proxy.newProxyInstance(
                com.hazelcast.nio.ObjectDataInput.class.getClassLoader(),
                new Class<?>[] {com.hazelcast.nio.ObjectDataInput.class}, (proxy, method, args) -> {
                    if (method.getName().equals("readInt") || method.getName().equals("readObject")) {
                        return next.next();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FiniteEnvelopeSourceProcessor source(String mapName, String sourceKey) throws Exception {
        var constructor = FiniteEnvelopeSourceProcessor.class.getDeclaredConstructor(String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(mapName, sourceKey);
    }
}
