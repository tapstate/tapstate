package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SnapshotRecoveryFrontierTest {
    @Test void everyTableAndWriterMustReleaseItsBoundIncludingWritersWithNoRows() {
        var frontier=new SnapshotRecoveryFrontier(Map.of("source.orders",7L,"other.orders",7L));
        assertThat(frontier.complete()).isFalse();
        var roster=Map.of("source.orders",List.of("sink-a#0","sink-a#1","sink-b#0"),"other.orders",List.of("sink-a#0","sink-a#1"));
        frontier.begin(roster);
        frontier.bounded("source.orders","sink-a#0",SourceOrder.snapshotRow(7));
        frontier.bounded("source.orders","sink-a#1",SourceOrder.snapshotRow(7));
        frontier.bounded("other.orders","sink-a#0",SourceOrder.snapshotRow(7));
        frontier.bounded("other.orders","sink-a#1",SourceOrder.snapshotRow(7));
        assertThat(frontier.complete()).isFalse();
        // This writer received no rows, but its real bound still closes its part of the output.
        frontier.bounded("source.orders","sink-b#0",SourceOrder.snapshotRow(7));
        assertThat(frontier.complete()).isTrue();
        frontier.begin(roster);frontier.bounded("source.orders","sink-b#0",SourceOrder.snapshotRow(7));
        assertThat(frontier.complete()).isTrue();
    }
    @Test void missingRosterWrongEpochAndForeignReportsNeverStandInForCompletion() {
        var frontier=new SnapshotRecoveryFrontier(Map.of("orders",7L));
        assertThatThrownBy(() -> frontier.bounded("orders","sink#0",SourceOrder.snapshotRow(7))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> frontier.begin(Map.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> frontier.begin(Map.of("orders",List.of()))).isInstanceOf(TapstateException.class);
        frontier.begin(Map.of("orders",List.of("sink#0","sink#1")));
        for(var order:List.of(SourceOrder.snapshotRow(6),SourceOrder.snapshotRow(8),new SourceOrder(7,0)))
            assertThatThrownBy(() -> frontier.bounded("orders","sink#0",order)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> frontier.bounded("foreign","sink#0",SourceOrder.snapshotRow(7))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> frontier.bounded("orders","unknown#0",SourceOrder.snapshotRow(7))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> frontier.begin(Map.of("orders",List.of("sink#0")))).isInstanceOf(TapstateException.class);
        frontier.bounded("orders","sink#0",SourceOrder.snapshotRow(7));assertThat(frontier.complete()).isFalse();
    }
    @Test void aReplacementProcessHasNoCompletionEvidenceFromThePreviousTracker() {
        var original=new SnapshotRecoveryFrontier(Map.of("orders",7L));original.begin(Map.of("orders",List.of("sink#0")));
        original.bounded("orders","sink#0",SourceOrder.snapshotRow(7));assertThat(original.complete()).isTrue();
        var restarted=new SnapshotRecoveryFrontier(Map.of("orders",7L));restarted.begin(Map.of("orders",List.of("sink#0")));
        assertThat(restarted.complete()).isFalse();
    }
}
