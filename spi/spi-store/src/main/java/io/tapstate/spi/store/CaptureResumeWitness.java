package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Exact bounded root/consumer facts read before a source is opened; no connector token is generated. */
public record CaptureResumeWitness(
        String sourceId, String connectorId, String miningChainId, String consumerId,
        ReadMode readMode, boolean srsEnabled, List<String> tables,
        boolean chainPresent, long chainEpoch, ChainPosition sourceRead, boolean sourceReadDurable,
        boolean consumerPresent, List<String> snapshotCompletedTables, String cdcStartPosition,
        long snapshotEpoch, ConsumerProgressKind progressKind,
        ChainPosition sinkAcked, Map<String, ChainPosition> sinkAckedByTable) {
    public CaptureResumeWitness {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(connectorId, "connectorId");
        Objects.requireNonNull(miningChainId, "miningChainId");
        Objects.requireNonNull(consumerId, "consumerId");
        Objects.requireNonNull(readMode, "readMode");
        tables = List.copyOf(tables);
        snapshotCompletedTables = List.copyOf(snapshotCompletedTables);
        sinkAckedByTable = Map.copyOf(sinkAckedByTable);
        if (sourceId.isBlank() || connectorId.isBlank() || miningChainId.isBlank() || consumerId.isBlank()
                || tables.isEmpty() || chainEpoch < 0 || snapshotEpoch < 0
                || (consumerPresent && progressKind == null)
                || (!chainPresent && (chainEpoch != 0 || sourceRead != null || sourceReadDurable))
                || (!consumerPresent && (!snapshotCompletedTables.isEmpty() || cdcStartPosition != null
                        || snapshotEpoch != 0 || sinkAcked != null || !sinkAckedByTable.isEmpty()))) {
            throw new IllegalArgumentException("capture resume witness must contain only actual source facts");
        }
    }

    public static CaptureResumeWitness from(String sourceId, String connectorId, String chain, String consumerId,
            ReadMode mode, boolean shared, List<String> tables, Optional<SrsMeta> record) {
        ConsumerOffset consumer = record.flatMap(meta -> meta.consumerOffset(consumerId)).orElse(null);
        return new CaptureResumeWitness(sourceId, connectorId, chain, consumerId, mode, shared, tables,
                record.isPresent(), record.map(SrsMeta::epoch).orElse(0L), record.map(SrsMeta::sourceRead).orElse(null),
                record.map(SrsMeta::sourceReadDurable).orElse(false), consumer != null,
                consumer == null ? List.of() : consumer.snapshotCompletedTables(),
                consumer == null ? null : consumer.cdcStartPosition(), consumer == null ? 0L : consumer.snapshotEpoch(),
                consumer == null ? null : consumer.progressKind(), consumer == null ? null : consumer.sinkAcked(),
                consumer == null ? Map.of() : consumer.sinkAckedByTable());
    }

    public boolean snapshotOwed() {
        return readMode != ReadMode.CDC_ONLY
                && (readMode == ReadMode.SNAPSHOT_ONLY || tables.stream().anyMatch(table -> !snapshotCompletedTables.contains(table)));
    }

    /** A pending snapshot's own recorded seam outranks a later shared capture checkpoint. */
    public Optional<ClusterRecoveryPosition> requestedPosition(String captureId) {
        String reference = miningChainId + "/" + consumerId;
        if (readMode == ReadMode.SNAPSHOT_ONLY) {
            return Optional.of(new ClusterRecoveryPosition(sourceId, connectorId, captureId,
                    ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "snapshot-only", reference));
        }
        if (snapshotOwed() && cdcStartPosition != null && snapshotEpoch > 0) {
            return Optional.of(new ClusterRecoveryPosition(sourceId, connectorId, captureId,
                    ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(SourceOrder.snapshotRow(snapshotEpoch), cdcStartPosition),
                    "mongo-consumer-snapshot-seam", reference));
        }
        if (sourceRead != null && sourceRead.token() != null) {
            return Optional.of(new ClusterRecoveryPosition(sourceId, connectorId, captureId,
                    ClusterRecoveryPosition.Kind.DURABLE_POSITION, sourceRead, "mongo-source-checkpoint", reference));
        }
        if (cdcStartPosition != null) {
            return Optional.of(new ClusterRecoveryPosition(sourceId, connectorId, captureId,
                    ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                    new ChainPosition(snapshotEpoch > 0 ? SourceOrder.snapshotRow(snapshotEpoch) : null, cdcStartPosition),
                    "mongo-consumer-seam", reference));
        }
        return snapshotOwed() ? Optional.of(new ClusterRecoveryPosition(sourceId, connectorId, captureId,
                ClusterRecoveryPosition.Kind.SNAPSHOT_REQUIRED, null, "mongo-snapshot-required", reference))
                : Optional.empty();
    }
}
