package io.tapstate.runtime.engine;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import io.tapstate.core.event.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Emits a pre-fetched finite sample and then completes; it never opens a capture or CDC source. */
public final class FiniteEnvelopeSourceProcessor extends AbstractProcessor {

    private final String mapName;
    private final String sourceKey;
    private List<Envelope> rows;
    private int next;

    private FiniteEnvelopeSourceProcessor(String mapName, String sourceKey) {
        this.mapName = Objects.requireNonNull(mapName, "mapName");
        this.sourceKey = Objects.requireNonNull(sourceKey, "sourceKey");
    }

    /** The supplier carries only serializable coordinates; the bounded sample stays in its typed map. */
    public static ProcessorMetaSupplier metaSupplier(String vertexName, String mapName, String sourceKey) {
        Objects.requireNonNull(vertexName, "vertexName");
        Objects.requireNonNull(mapName, "mapName");
        Objects.requireNonNull(sourceKey, "sourceKey");
        SupplierEx<Processor> supplier = () -> new FiniteEnvelopeSourceProcessor(mapName, sourceKey);
        return ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(supplier), vertexName);
    }

    @Override
    protected void init(Processor.Context context) {
        IMap<String, Sample> samples = localMember().getMap(mapName);
        Sample sample = samples.get(sourceKey);
        if (sample == null) {
            throw new IllegalStateException("preview sample is missing for source '" + sourceKey + "'");
        }
        rows = sample.rows();
    }

    @Override
    public boolean complete() {
        while (next < rows.size()) {
            if (!tryEmit(rows.get(next))) {
                return false;
            }
            next++;
        }
        return true;
    }

    private static HazelcastInstance localMember() {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        if (instances.size() != 1) {
            throw new IllegalStateException("expected exactly one local Hazelcast member, found "
                    + instances.size());
        }
        HazelcastInstance member = instances.iterator().next();
        if (!member.getLifecycleService().isRunning()) {
            throw new IllegalStateException("preview member is not running");
        }
        return member;
    }

    /** A bounded typed batch stored with the same Envelope serializer used on pipeline edges. */
    public record Sample(List<Envelope> rows) {

        public static final int MAX_ROWS = 20_000;

        public Sample {
            rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
            if (rows.size() > MAX_ROWS) {
                throw new IllegalArgumentException("preview sample row count exceeds its bound");
            }
        }
    }

    /** Hazelcast wire form for one source's bounded sample; Envelope values use EnvelopeSerializer. */
    public static final class SampleSerializer implements StreamSerializer<Sample> {

        public static final int TYPE_ID = 10004;

        @Override
        public int getTypeId() {
            return TYPE_ID;
        }

        @Override
        public void write(ObjectDataOutput out, Sample sample) throws IOException {
            out.writeInt(sample.rows().size());
            for (Envelope row : sample.rows()) {
                out.writeObject(row);
            }
        }

        @Override
        public Sample read(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            if (size < 0 || size > Sample.MAX_ROWS) {
                throw new IOException("preview sample row count is out of range");
            }
            ArrayList<Envelope> rows = new ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                Object row = in.readObject();
                if (!(row instanceof Envelope envelope)) {
                    throw new IOException("preview sample contains a non-envelope row");
                }
                rows.add(envelope);
            }
            return new Sample(rows);
        }
    }
}
