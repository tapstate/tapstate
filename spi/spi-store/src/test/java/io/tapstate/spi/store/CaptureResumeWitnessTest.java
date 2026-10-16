package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CaptureResumeWitnessTest {
    @Test
    void aStampedExplicitMoveOutranksTheCompletedSharedConsumersOlderSeam() {
        ChainPosition moved = new ChainPosition(null, "explicitly-selected");
        var consumer = new ConsumerOffset("qualified-consumer", Map.of(), null, List.of("orders"),
                "old-load-seam", 3, Map.of(), ConsumerProgressKind.SRS);
        var record = new SrsMeta("chain", moved, List.of(consumer), List.of(), null, 4,
                Instant.parse("2026-10-15T01:00:00Z"), false);
        for (ReadMode mode : List.of(ReadMode.CDC_ONLY, ReadMode.SNAPSHOT_AND_CDC)) {
            var witness = CaptureResumeWitness.from("orders", "mongo", "chain", "qualified-consumer",
                    mode, true, List.of("orders"), java.util.Optional.of(record));
            assertThat(witness.requestedPosition("capture").orElseThrow().position())
                    .as("a profiled start freezes the explicit move rather than the old load seam: %s", mode)
                    .isEqualTo(moved);
        }
    }

    @Test
    void aLegacyOrOrderedUnprovenReadCannotMasqueradeAsAnExplicitMove() {
        var consumer = new ConsumerOffset("qualified-consumer", Map.of(), null, List.of("orders"),
                "old-load-seam", 3, Map.of(), ConsumerProgressKind.SRS);
        for (SrsMeta record : List.of(
                new SrsMeta("chain", new ChainPosition(null, "unstamped"), List.of(consumer), List.of(), null, 4),
                new SrsMeta("chain", new ChainPosition(new SourceOrder(4, 20), "ordered-unproven"),
                        List.of(consumer), List.of(), null, 4, Instant.parse("2026-10-15T01:00:00Z"), false))) {
            var witness = CaptureResumeWitness.from("orders", "mongo", "chain", "qualified-consumer",
                    ReadMode.CDC_ONLY, true, List.of("orders"), java.util.Optional.of(record));
            assertThat(witness.requestedPosition("capture").orElseThrow().position().token())
                    .isEqualTo("old-load-seam");
        }
    }

    @Test
    void aFreshFullSnapshotCannotBorrowAnotherReadersCheckpointAsItsOwnSeam() {
        var witness = witness(ReadMode.SNAPSHOT_AND_CDC, true, null, 0, null,
                new ChainPosition(new SourceOrder(4, 200), "physical-reader-200"));
        assertThat(witness.requestedPosition("capture").orElseThrow().kind())
                .isEqualTo(ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED);
    }

    @Test
    void aDirectResumeUsesThisConsumersConfirmedPrefixInsteadOfAnAheadRead() {
        ChainPosition confirmed = new ChainPosition(new SourceOrder(4, 100), "confirmed-100");
        var witness = witness(ReadMode.CDC_ONLY, false, null, 0, confirmed,
                new ChainPosition(new SourceOrder(4, 200), "ahead-200"));
        assertThat(witness.requestedPosition("capture").orElseThrow().position()).isEqualTo(confirmed);
    }

    @Test
    void everyDirectTableMustConfirmTheCommonPrefixBeforeAnAheadReadCanResume() {
        ChainPosition orders = new ChainPosition(new SourceOrder(4, 100), "opaque-orders-100");
        ChainPosition customers = new ChainPosition(new SourceOrder(4, 70), "opaque-customers-70");
        ChainPosition ahead = new ChainPosition(new SourceOrder(4, 200), "opaque-read-200");
        var complete = directTables(List.of("orders", "customers"), Map.of("orders", orders, "customers", customers), ahead);
        var requested = complete.requestedPosition("capture").orElseThrow();
        assertThat(requested.position()).isEqualTo(customers);
        assertThat(requested.provenance()).isEqualTo("mongo-consumer-confirmed-prefix");
        assertThat(requested.captureId()).isEqualTo("capture");
        var incomplete = directTables(List.of("orders", "customers", "products"),
                Map.of("orders", orders, "customers", customers), ahead);
        assertThat(incomplete.requestedPosition("capture")).isEmpty();
    }

    @Test
    void anUnorderedTableAcknowledgementCannotTurnAnAheadReadIntoAResumePoint() {
        var witness = directTables(List.of("orders", "customers"), Map.of(
                "orders", new ChainPosition(new SourceOrder(4, 100), "orders-100"),
                "customers", new ChainPosition(null, "unordered-customer")),
                new ChainPosition(new SourceOrder(4, 200), "ahead-200"));
        assertThat(witness.requestedPosition("capture")).isEmpty();
    }

    @Test
    void aReadBeforeTheConfirmedFloorKeepsItsExactOpaquePoint() {
        ChainPosition read = new ChainPosition(new SourceOrder(4, 60), "opaque-read-60");
        var witness = directTables(List.of("orders", "customers"), Map.of(
                "orders", new ChainPosition(new SourceOrder(4, 100), "orders-100"),
                "customers", new ChainPosition(new SourceOrder(4, 70), "customers-70")), read);
        assertThat(witness.requestedPosition("capture").orElseThrow().position()).isEqualTo(read);
    }

    @Test
    void anUnfinishedSnapshotUsesItsOwnSeamAheadOfTheSharedReadersCheckpoint() {
        var witness = new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer",
                ReadMode.SNAPSHOT_AND_CDC, true, List.of("orders", "customers"), true, 4,
                new ChainPosition(new SourceOrder(4, 200), "another-reader-200"), true, true,
                List.of("orders"), "own-snapshot-seam", 3, ConsumerProgressKind.SRS, null, Map.of());
        var requested = witness.requestedPosition("capture").orElseThrow();
        assertThat(requested.position()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(3), "own-snapshot-seam"));
        assertThat(requested.provenance()).isEqualTo("mongo-consumer-snapshot-seam");
        assertThat(requested.durableStateReference()).isEqualTo("chain/qualified-consumer");
    }

    @Test
    void aWitnessHoldsOnlyFactsThatItsChainAndConsumerActuallyRecorded() {
        assertThat(new Raw().build().consumerPresent()).isTrue();
        ChainPosition read = new ChainPosition(new SourceOrder(4, 200), "read-200");
        Map<String, Consumer<Raw>> invented = new LinkedHashMap<>();
        invented.put("blank source", raw -> raw.sourceId = " ");
        invented.put("blank connector", raw -> raw.connectorId = "");
        invented.put("blank chain", raw -> raw.chain = " ");
        invented.put("blank consumer", raw -> raw.consumerId = "");
        invented.put("no tables", raw -> raw.tables = List.of());
        invented.put("negative chain epoch", raw -> raw.chainEpoch = -1);
        invented.put("negative snapshot epoch", raw -> raw.snapshotEpoch = -1);
        invented.put("consumer without progress kind", raw -> raw.progressKind = null);
        invented.put("absent chain with an epoch", raw -> raw.chainPresent = false);
        invented.put("absent chain with a read", raw -> {
            raw.chainPresent = false;
            raw.chainEpoch = 0;
            raw.sourceRead = read;
        });
        invented.put("absent chain with a durable read", raw -> {
            raw.chainPresent = false;
            raw.chainEpoch = 0;
            raw.durable = true;
        });
        invented.put("absent consumer with completed tables", raw -> absentConsumer(raw).completed = List.of("orders"));
        invented.put("absent consumer with a seam", raw -> absentConsumer(raw).seam = "seam");
        invented.put("absent consumer with a snapshot epoch", raw -> absentConsumer(raw).snapshotEpoch = 1);
        invented.put("absent consumer with an acknowledgement", raw -> absentConsumer(raw).sinkAcked = read);
        invented.put("absent consumer with table acknowledgements", raw ->
                absentConsumer(raw).sinkAckedByTable = Map.of("orders", read));
        assertThat(invented).hasSize(16);
        invented.forEach((name, change) -> assertThatThrownBy(() -> new Raw().with(change).build()).as(name)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("capture resume witness must contain only actual source facts"));
        assertThat(new Raw().with(CaptureResumeWitnessTest::absentConsumer).build().consumerPresent()).isFalse();

        assertThatThrownBy(() -> new Raw().with(raw -> raw.priorReader = reader("other-chain",
                CaptureReadAttempt.Kind.RESUME, "token-9", null)).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("retained reader must name this exact physical source chain");
    }

    @Test
    void eachRequestedPositionNamesTheDurableFactItCameFrom() {
        ChainPosition durableRead = new ChainPosition(new SourceOrder(4, 200), "read-200");
        assertThat(new Raw().with(raw -> raw.mode = ReadMode.SNAPSHOT_ONLY).build().requestedPosition("capture")
                .orElseThrow()).satisfies(position -> {
                    assertThat(position.kind()).isEqualTo(ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED);
                    assertThat(position.provenance()).isEqualTo("snapshot-only");
                });
        assertThat(new Raw().with(raw -> {
            raw.sourceRead = durableRead;
            raw.durable = true;
        }).build().requestedPosition("capture").orElseThrow()).satisfies(position -> {
            assertThat(position.position()).isEqualTo(durableRead);
            assertThat(position.provenance()).isEqualTo("mongo-source-checkpoint");
        });

        CaptureResumeWitness unsampledSeam = new Raw().with(raw -> {
            raw.sourceRead = durableRead;
            raw.seam = "cdc-start";
        }).build();
        assertThat(unsampledSeam.requestedPosition("capture").orElseThrow()).satisfies(position -> {
            assertThat(position.position()).isEqualTo(new ChainPosition(null, "cdc-start"));
            assertThat(position.provenance()).isEqualTo("mongo-consumer-seam");
        });
        assertThat(new Raw().with(raw -> {
            raw.seam = "cdc-start";
            raw.snapshotEpoch = 3;
        }).build().requestedPosition("capture").orElseThrow().position())
                .isEqualTo(new ChainPosition(SourceOrder.snapshotRow(3), "cdc-start"));

        assertThat(new Raw().with(raw -> raw.priorReader = reader("chain", CaptureReadAttempt.Kind.RESUME,
                "requested-9", null)).build().requestedPosition("capture").orElseThrow()).satisfies(position -> {
                    assertThat(position.position()).isEqualTo(new ChainPosition(SourceOrder.snapshotRow(4), "requested-9"));
                    assertThat(position.provenance()).isEqualTo("mongo-retained-reader-request");
                });
        assertThat(new Raw().with(raw -> raw.priorReader = reader("chain", CaptureReadAttempt.Kind.PRESENT,
                null, "anchor-7")).build().requestedPosition("capture").orElseThrow().position())
                .isEqualTo(new ChainPosition(SourceOrder.snapshotRow(4), "anchor-7"));
        assertThat(new Raw().with(raw -> raw.priorReader = reader("chain", CaptureReadAttempt.Kind.PRESENT,
                null, null)).build().requestedPosition("capture")).isEmpty();

        assertThat(new Raw().with(raw -> {
            raw.mode = ReadMode.SNAPSHOT_AND_CDC;
            raw.tables = List.of("orders", "customers");
            raw.completed = List.of("orders");
        }).build().requestedPosition("capture").orElseThrow()).satisfies(position -> {
            assertThat(position.kind()).isEqualTo(ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED);
            assertThat(position.provenance()).isEqualTo("mongo-snapshot-required");
        });
        assertThat(new Raw().with(raw -> {
            raw.mode = ReadMode.SNAPSHOT_AND_CDC;
            raw.completed = List.of("orders");
        }).build().snapshotOwed()).isFalse();
    }

    @Test
    void onlyAnIsolatedChannelsTokenBearingConfirmationCanResumeItsReader() {
        ChainPosition confirmed = new ChainPosition(new SourceOrder(4, 100), "confirmed-100");
        CaptureResumeWitness shared = new Raw().with(raw -> {
            raw.srs = false;
            raw.sinkAcked = confirmed;
        }).build();
        assertThat(shared.confirmedDirectPosition()).isNull();
        assertThat(shared.requestedPosition("capture")).isEmpty();

        CaptureResumeWitness tokenless = new Raw().with(raw -> {
            raw.srs = false;
            raw.progressKind = ConsumerProgressKind.DIRECT_SOURCE;
            raw.sinkAcked = new ChainPosition(new SourceOrder(4, 100), null);
        }).build();
        assertThat(tokenless.confirmedDirectPosition()).isNull();

        CaptureResumeWitness readWithoutToken = new Raw().with(raw -> {
            raw.srs = false;
            raw.progressKind = ConsumerProgressKind.DIRECT_SOURCE;
            raw.sinkAcked = confirmed;
            raw.sourceRead = new ChainPosition(new SourceOrder(4, 50), null);
        }).build();
        assertThat(readWithoutToken.confirmedDirectPosition()).isEqualTo(confirmed);

        CaptureResumeWitness nothingConfirmed = new Raw().with(raw -> {
            raw.srs = false;
            raw.progressKind = ConsumerProgressKind.DIRECT_SOURCE;
        }).build();
        assertThat(nothingConfirmed.confirmedDirectPosition()).isNull();
    }

    /** One SRS-backed consumer of one table, changed one fact at a time by each case. */
    private static final class Raw {
        String sourceId = "orders";
        String connectorId = "mongo";
        String chain = "chain";
        String consumerId = "qualified-consumer";
        ReadMode mode = ReadMode.CDC_ONLY;
        boolean srs = true;
        List<String> tables = List.of("orders");
        boolean chainPresent = true;
        long chainEpoch = 4;
        ChainPosition sourceRead;
        boolean durable;
        boolean consumerPresent = true;
        List<String> completed = List.of();
        String seam;
        long snapshotEpoch;
        ConsumerProgressKind progressKind = ConsumerProgressKind.SRS;
        ChainPosition sinkAcked;
        Map<String, ChainPosition> sinkAckedByTable = Map.of();
        CaptureReadState priorReader;

        Raw with(Consumer<Raw> change) {
            change.accept(this);
            return this;
        }

        CaptureResumeWitness build() {
            return new CaptureResumeWitness(sourceId, connectorId, chain, consumerId, mode, srs, tables, chainPresent,
                    chainEpoch, sourceRead, durable, consumerPresent, completed, seam, snapshotEpoch, progressKind,
                    sinkAcked, sinkAckedByTable, priorReader);
        }
    }

    private static Raw absentConsumer(Raw raw) {
        raw.consumerPresent = false;
        raw.progressKind = null;
        return raw;
    }

    private static CaptureReadState reader(String chain, CaptureReadAttempt.Kind kind, String token, String anchor) {
        WorkloadClaimFence claim = new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, chain),
                new WorkloadOwner("node-a", "boot-a"), 1, 0, 1, 1);
        CaptureReadAttempt attempt = new CaptureReadAttempt(chain, 4, 1, claim, List.of("orders"), kind, token, null,
                Instant.parse("2026-10-11T00:00:00Z"));
        return new CaptureReadState(attempt, anchor, anchor == null ? null : Instant.parse("2026-10-11T00:00:01Z"),
                null, false, null);
    }

    private static CaptureResumeWitness directTables(List<String> tables, Map<String, ChainPosition> confirmed,
            ChainPosition read) {
        return new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer", ReadMode.CDC_ONLY,
                false, tables, true, 4, read, false, true, List.of(), null, 0,
                ConsumerProgressKind.DIRECT_SOURCE, null, confirmed);
    }

    private static CaptureResumeWitness witness(ReadMode mode, boolean shared, String seam, long snapshotEpoch,
            ChainPosition ack, ChainPosition read) {
        return new CaptureResumeWitness("orders", "mongo", "chain", "qualified-consumer", mode, shared,
                List.of("orders"), true, 4, read, shared, true, List.of(), seam, snapshotEpoch,
                shared ? ConsumerProgressKind.SRS : ConsumerProgressKind.DIRECT_SOURCE, ack, Map.of());
    }
}
