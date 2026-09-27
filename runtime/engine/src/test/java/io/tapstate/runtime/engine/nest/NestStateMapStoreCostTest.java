package io.tapstate.runtime.engine.nest;

import io.tapstate.runtime.engine.StateStoreCostStats;
import io.tapstate.runtime.engine.StateStoreCostProbe;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Cold-layer costs follow the bytes a stateful nest actually writes and reads. */
class NestStateMapStoreCostTest {

    @Test
    void writeReadAndDeleteMeasureTheColdLayerWithoutEncodingASecondTime() {
        HeapKeyedStateStore store = new HeapKeyedStateStore();
        StateStoreCostStats costs = new StateStoreCostStats();
        NestStateMapStore bridge = new NestStateMapStore("nest.p.step.root", store, costs);
        List<String> key = List.of("order-1");

        bridge.store(key, "payload");
        int encodedBytes = store.load("nest.p.step.root", NestStateKeys.nameOf(key))
                .orElseThrow().length;
        assertThat(bridge.load(key)).isEqualTo("payload");
        bridge.delete(key);
        assertThat(bridge.load(key)).isNull();

        StateStoreCostStats.Reading measured = costs
                .reading("nest.p.step.root").orElseThrow();
        assertThat(measured.operations().get(StateStoreCostProbe.Operation.SAVE).completed()).isOne();
        assertThat(measured.operations().get(StateStoreCostProbe.Operation.SAVE).payloadBytes())
                .isEqualTo(encodedBytes);
        assertThat(measured.operations().get(StateStoreCostProbe.Operation.LOAD).completed()).isEqualTo(2);
        assertThat(measured.operations().get(StateStoreCostProbe.Operation.LOAD).payloadBytes())
                .isEqualTo(encodedBytes);
        assertThat(measured.operations().get(StateStoreCostProbe.Operation.DELETE).completed()).isOne();
        assertThat(measured.codecs().get(StateStoreCostProbe.Codec.ENCODE).completed()).isOne();
        assertThat(measured.codecs().get(StateStoreCostProbe.Codec.ENCODE).bytes()).isEqualTo(encodedBytes);
        assertThat(measured.codecs().get(StateStoreCostProbe.Codec.DECODE).completed()).isOne();
        assertThat(measured.codecs().get(StateStoreCostProbe.Codec.DECODE).bytes()).isEqualTo(encodedBytes);

        costs.forget("nest.p.step.root");
        assertThat(costs.reading("nest.p.step.root")).isEmpty();
    }
}
