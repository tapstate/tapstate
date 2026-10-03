package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
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
}
