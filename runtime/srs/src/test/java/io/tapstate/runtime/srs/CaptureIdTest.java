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
    void tableSubsetsOfOnePhysicalChainShareOneCaptureClaim() {
        CaptureConfig one = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("orders", "customers"));
        CaptureConfig reordered = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("customers", "orders"));
        CaptureConfig narrower = new CaptureConfig("mysql", Map.of("host", "db.internal"),
                List.of("orders"));

        assertThat(CaptureId.of(one, null)).isEqualTo(CaptureId.of(reordered, null));
        assertThat(CaptureId.of(one, null)).isEqualTo(CaptureId.of(narrower, null));
    }

    @Test
    void anExplicitMiningKeyAlsoSharesOnePhysicalCaptureAcrossTableSubsets() {
        CaptureConfig orders = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders"));
        CaptureConfig customers = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("customers"));

        assertThat(CaptureId.of(orders, "shared-db"))
                .isEqualTo(CaptureId.of(customers, "shared-db"));
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

    /**
     * Two pipelines reading one source directly are two captures, however alike their reads.
     *
     * <p>A tail with the shared ring switched off streams to the one pipeline that opened it and writes no
     * ring anybody else could read. Filed under one identity, the second pipeline over the same source was
     * held to the first one's claim, attached to a tail it could not read from, and ran on its initial
     * load alone -- every change after it missing, with the pipeline running.
     */
    @Test
    void twoPipelinesReadingOneSourceDirectlyAreTwoCaptures() {
        CaptureConfig config = new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders"));
        CaptureRunSpec first = new CaptureRunSpec(
                config, ReadMode.SNAPSHOT_AND_CDC, null, false, "source", "pipeline-a",
                StartFrom.latest(), null, 0);
        CaptureRunSpec second = new CaptureRunSpec(
                config, ReadMode.SNAPSHOT_AND_CDC, null, false, "source", "pipeline-b",
                StartFrom.latest(), null, 0);
        CaptureRunSpec firstAgain = new CaptureRunSpec(
                config, ReadMode.CDC_ONLY, null, false, "source", "pipeline-a",
                StartFrom.latest(), null, 0);

        assertThat(CaptureId.of(first)).isNotEqualTo(CaptureId.of(second));
        assertThat(CaptureId.of(first))
                .as("while one pipeline's direct read stays one capture, run after run")
                .isEqualTo(CaptureId.of(firstAgain));
    }
}
