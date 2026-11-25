package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.map.IMap;
import io.tapstate.core.event.Envelope;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

/** Observes final keyed documents even when one recovered handoff emits several versions. */
public final class RecoveredNestDocuments {
    private RecoveredNestDocuments() { }

    public static String key(Envelope event) {
        return (String) event.after().get("customer_id");
    }

    public static ProcessorMetaSupplier sink(String name) {
        return ProcessorMetaSupplier.of(ProcessorSupplier.of(() -> new Materializer(name)));
    }

    public static Map<String, Envelope> documents(HazelcastInstance member, String name) {
        return Map.copyOf(member.<String, Envelope>getMap(name));
    }

    public static void assertRecovered(HazelcastInstance member, String name) {
        var documents = documents(member, name);
        assertThat(documents).hasSize(2).containsOnlyKeys("C1", "C2");
        for (int index = 1; index <= 2; index++) {
            var row = documents.get("C" + index).after();
            assertThat(row).containsEntry("customer_id", "C" + index);
            var parents = (List<?>) row.get("$root");
            assertThat(parents).as("parent of C%s", index).hasSize(1);
            var parent = (Map<?, ?>) parents.getFirst();
            assertThat(parent.get("policy_id")).isEqualTo("P" + index);
            var children = (List<?>) parent.get("child");
            assertThat(children).as("child of C%s", index).hasSize(1);
            var child = (Map<?, ?>) children.getFirst();
            assertThat(child.get("claim_id")).isEqualTo("K" + index);
            assertThat(child.get("value")).isEqualTo(1);
        }
    }

    /** Each routed key is written synchronously, so an earlier version cannot overwrite its successor. */
    private static final class Materializer extends AbstractProcessor {
        private final String name;
        private IMap<String, Envelope> documents;

        private Materializer(String name) { this.name = name; }

        @Override public boolean isCooperative() { return false; }

        @Override protected void init(Context context) {
            documents = context.hazelcastInstance().getMap(name);
        }

        @Override protected boolean tryProcess(int ordinal, Object item) {
            var event = (Envelope) item;
            documents.set(key(event), event);
            return true;
        }
    }
}
