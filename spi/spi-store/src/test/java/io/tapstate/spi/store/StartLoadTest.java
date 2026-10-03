package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StartLoadTest {

    private static final String PIPE = "pipe";

    @Test
    void aPipelineWithNoRecordAnywhereRunsANewFullLoad() {
        assertThat(StartLoad.of(null, PIPE, List.of())).isEqualTo(StartLoad.FULL_LOAD);
        assertThat(StartLoad.of(ReadMode.SNAPSHOT_AND_CDC, PIPE, List.of(chain()))).isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void anEntryWithNoProgressIsStillAFullLoad() {
        ConsumerOffset attached = new ConsumerOffset(PIPE, Map.of(), null, List.of(), null, 0L);
        assertThat(StartLoad.of(null, PIPE, List.of(chain(attached)))).isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void eachKindOfDurableProgressMakesTheStartAResume() {
        ConsumerOffset cursor = new ConsumerOffset(PIPE, Map.of("orders", 3L), null, List.of(), null, 0L);
        ConsumerOffset acked = new ConsumerOffset(PIPE, Map.of(),
                new ChainPosition(new SourceOrder(1, 7), "pos-7"), List.of(), null, 0L);
        ConsumerOffset loaded = new ConsumerOffset(PIPE, Map.of(), null, List.of("orders"), null, 0L);
        ConsumerOffset seam = new ConsumerOffset(PIPE, Map.of(), null, List.of(), "seam-0", 1L);

        for (ConsumerOffset progressed : List.of(cursor, acked, loaded, seam)) {
            assertThat(StartLoad.of(ReadMode.SNAPSHOT_AND_CDC, PIPE, List.of(chain(progressed))))
                    .as("progress %s", progressed)
                    .isEqualTo(StartLoad.RESUME);
        }
    }

    @Test
    void progressOnAnyOneOfTheChainsReadIsEnough() {
        ConsumerOffset loaded = new ConsumerOffset(PIPE, Map.of(), null, List.of("orders"), null, 0L);
        assertThat(StartLoad.of(null, PIPE, List.of(chain(), chain(loaded)))).isEqualTo(StartLoad.RESUME);
    }

    @Test
    void anotherPipelinesProgressOnASharedChainSaysNothingAboutThisOne() {
        ConsumerOffset other = new ConsumerOffset("other", Map.of("orders", 3L), null, List.of("orders"), "seam", 1L);
        assertThat(StartLoad.of(null, PIPE, List.of(chain(other)))).isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void aChangesOnlyReadNeverLoadsWhateverIsRecorded() {
        ConsumerOffset loaded = new ConsumerOffset(PIPE, Map.of(), null, List.of("orders"), null, 0L);
        assertThat(StartLoad.of(ReadMode.CDC_ONLY, PIPE, List.of())).isEqualTo(StartLoad.CDC_ONLY);
        assertThat(StartLoad.of(ReadMode.CDC_ONLY, PIPE, List.of(chain(loaded)))).isEqualTo(StartLoad.CDC_ONLY);
    }

    @Test
    void aSnapshotOnlyReadIsJudgedByItsRecordsLikeTheDefault() {
        ConsumerOffset loaded = new ConsumerOffset(PIPE, Map.of(), null, List.of("orders"), null, 0L);
        assertThat(StartLoad.of(ReadMode.SNAPSHOT_ONLY, PIPE, List.of())).isEqualTo(StartLoad.FULL_LOAD);
        assertThat(StartLoad.of(ReadMode.SNAPSHOT_ONLY, PIPE, List.of(chain(loaded)))).isEqualTo(StartLoad.RESUME);
    }

    private static SrsMeta chain(ConsumerOffset... consumers) {
        return new SrsMeta("chain-a", null, List.of(consumers), List.of(), null);
    }
}
