package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
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
}
