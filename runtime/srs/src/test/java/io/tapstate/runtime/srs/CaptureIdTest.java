package io.tapstate.runtime.srs;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.capture.CaptureConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureIdTest {

    @Test
    void pipelineIdentityDoesNotSplitOneNormalizedSourceReadContract() {
        CaptureConfig base = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("orders", "customers"));

        CaptureId first = CaptureId.of(base.at(new PipelineNode("pipeline-a", "source-a")), null);
        CaptureId second = CaptureId.of(base.at(new PipelineNode("pipeline-b", "source-b")), null);

        assertThat(first).isEqualTo(second);
    }

    @Test
    void streamOrderIsNormalizedButADifferentReadSetIsADifferentCapture() {
        CaptureConfig one = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("orders", "customers"));
        CaptureConfig reordered = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("customers", "orders"));
        CaptureConfig narrower = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("orders"));

        assertThat(CaptureId.of(one, null)).isEqualTo(CaptureId.of(reordered, null));
        assertThat(CaptureId.of(one, null)).isNotEqualTo(CaptureId.of(narrower, null));
    }

    @Test
    void anExplicitMiningKeyStillKeepsDistinctReadContractsApart() {
        CaptureConfig orders = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders"));
        CaptureConfig customers = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("customers"));

        assertThat(CaptureId.of(orders, "shared-db"))
                .isNotEqualTo(CaptureId.of(customers, "shared-db"));
    }

    @Test
    void aSnapshotOnlyReadCannotBecomeTheOwnerOfAnIncrementalTail() {
        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders"));
        CaptureRunSpec snapshot = new CaptureRunSpec(
                config, ReadMode.SNAPSHOT_ONLY, null, true, "source", "pipeline-a",
                StartFrom.earliest(), null, 0);
        CaptureRunSpec cdc = new CaptureRunSpec(
                config, ReadMode.CDC_ONLY, null, true, "source", "pipeline-b",
                StartFrom.earliest(), null, 0);
        CaptureRunSpec snapshotAndCdc = new CaptureRunSpec(
                config, ReadMode.SNAPSHOT_AND_CDC, null, true, "source", "pipeline-c",
                StartFrom.latest(), null, 0);

        assertThat(CaptureId.of(snapshot)).isNotEqualTo(CaptureId.of(cdc));
        assertThat(CaptureId.of(cdc)).isEqualTo(CaptureId.of(snapshotAndCdc));
    }
}
