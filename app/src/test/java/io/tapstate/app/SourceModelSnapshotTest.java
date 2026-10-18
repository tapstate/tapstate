package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class SourceModelSnapshotTest {
    private static SourceResource source(String id,String connector) { return new SourceResource(id,null,connector,Map.of(),null,null,null,null); }
    private static DiscoveredSourceModel discovered(String id,String connector,String column) {
        return new DiscoveredSourceModel(id,connector,1,new SourceModel(List.of(new SourceTable("orders",List.of(new SourceField(column,"INT")),List.of(column),List.of()))));
    }
    @Test void eachSourceIsReadOnceAndLaterStartsCanChooseNewDiscoveryWithoutChangingThisStart() {
        var reads = new AtomicInteger(); var data = new HashMap<String,DiscoveredSourceModel>(); data.put("src",discovered("src","mysql","original"));
        SchemaStore live = new SchemaStore() {
            @Override public void save(DiscoveredSourceModel value) { data.put(value.connectionId(),value); }
            @Override public Optional<DiscoveredSourceModel> get(String id) { reads.incrementAndGet(); return Optional.ofNullable(data.get(id)); }
        };
        var src = source("src","mysql"); var snapshot = SourceModelSnapshot.capture("p",List.of(src,src),live);
        assertThat(reads.get()).isOne(); live.save(discovered("src","mysql","newer"));
        assertThat(snapshot.model(src).tables().getFirst().primaryKey()).containsExactly("original"); assertThat(reads.get()).isOne();
        assertThat(SourceModelSnapshot.capture("p",List.of(src),live).model(src).tables().getFirst().primaryKey()).containsExactly("newer");
    }
    @Test void absentMismatchedAndDiscoveredEmptyModelsKeepTheirDistinctMeanings() {
        var store = new InMemoryStorePort(); var absent = source("absent","mysql"); var mismatched = source("mismatched","mysql"); var empty = source("empty","mysql");
        store.schemas().save(discovered("mismatched","other","wrong"));
        store.schemas().save(new DiscoveredSourceModel("empty","mysql",1,new SourceModel(List.of())));
        var snapshot = SourceModelSnapshot.capture("p",List.of(absent,mismatched,empty),store.schemas());
        assertThat(snapshot.model(absent)).isNull(); assertThat(snapshot.model(mismatched)).isNull(); assertThat(snapshot.model(empty).tables()).isEmpty();
        assertThatThrownBy(() -> snapshot.model(source("unknown","mysql"))).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED));
        assertThatThrownBy(() -> snapshot.requirePipeline("foreign")).isInstanceOf(TapstateException.class);
    }
    @Test void aModelAwareStartNeverFallsBackToAnUnadaptedCaptureCoordinator() {
        var calls = new AtomicInteger(); var store = new InMemoryStorePort();
        var models = SourceModelSnapshot.capture("p",List.of(),store.schemas());
        PipelineCaptureCoordinator unknown = new PipelineCaptureCoordinator() {
            @Override public void startCapture(String pipeline) { calls.incrementAndGet(); }
            @Override public void stopCapture(String pipeline,boolean purge) { throw new AssertionError("no capture was opened"); }
        };
        assertThatThrownBy(() -> unknown.startCapture("p",store.artifacts(),models)).isInstanceOf(TapstateException.class);
        assertThat(calls.get()).isZero();
        assertThatThrownBy(() -> new DagSource.StartPreparation(DagSource.NestCapacity.none(),Set.of(),Optional.empty(),
                () -> fence -> null,Map.of(),Optional.of(models))).isInstanceOf(IllegalArgumentException.class);
    }
}
