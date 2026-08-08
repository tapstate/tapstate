package io.tapstate.app;

import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.SourceModel;
import java.util.List;

/**
 * How a pipeline-referenced source resolves into its capture/read ring identity: the connector config, the
 * selected tables, the srs key, and the mining-chain id that keys each per-table change ring. The
 * side that fills the ring (capture) and the side that reads it (the source vertex) each resolve the source
 * independently, so deriving this identity in one place is what guarantees they land on the same ring rather
 * than drifting apart.
 *
 * <p>An explicit {@code srs.key} keys the chain directly; otherwise it derives from the connector-and-settings
 * content hash.
 *
 * @param sourceId the resource id of the source
 * @param config   the connector, its settings, and the selected streams to read
 * @param tables   the selected tables in deterministic selector/discovery order
 * @param srsKey   the explicit mining-chain key, or null to derive from the config hash
 * @param chainId  the mining-chain id both the writer and the reader key the ring under
 */
record SourceCaptureResolution(
        String sourceId, CaptureConfig config, List<String> tables, String srsKey, MiningChainId chainId) {

    static SourceCaptureResolution of(SourceResource source) {
        return of(source, null);
    }

    static SourceCaptureResolution of(SourceResource source, SourceModel discovered) {
        List<String> tables = SourceTableSelection.resolve(source, discovered);
        CaptureConfig config = new CaptureConfig(source.connector(), source.config(), tables);
        String srsKey = source.srs() != null ? source.srs().key() : null;
        return new SourceCaptureResolution(source.id(), config, tables, srsKey, MiningChainId.resolve(config, srsKey));
    }

    String table() {
        return tables.getFirst();
    }

    /** The per-table change ring name both the capture writer and the source-vertex reader look the ring up by. */
    String ringName() {
        return ringName(table());
    }

    String ringName(String table) {
        return SrsRingbuffer.ringName(chainId.value(), table);
    }
}
