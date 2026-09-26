package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkPort;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapstate.spi.sink.TargetTable;
import java.util.Map;

/**
 * The PDK implementation of the write-side sink port: it provisions a connector, refuses it with a
 * code if its declared API level is incompatible, opens the target once, and yields a writer that
 * encodes tapstate envelopes back to PDK record events. Asking a connector for a write capability it
 * does not provide is a caller invariant violation (the DSL validated the connector's capabilities
 * upstream) and crashes bare rather than being laundered into a code.
 */
public final class PdkSinkPort implements SinkPort {

    private final ConnectorProvisioner provisioner;
    private final KeyedStateStore stateStore;
    private final SharedSinkConnectors sharing;

    /** For the drives that keep nothing: no store, so nothing a connector writes is filed anywhere. */
    public PdkSinkPort(ConnectorProvisioner provisioner) {
        this(provisioner, null);
    }

    public PdkSinkPort(ConnectorProvisioner provisioner, KeyedStateStore stateStore) {
        this(provisioner, stateStore, null);
    }

    /**
     * As above, sharing one connector among the writers of a sink where its artifact is certified for that, as
     * {@code sharing} keeps them on this member; null opens one per writer whatever the artifact.
     */
    public PdkSinkPort(ConnectorProvisioner provisioner, KeyedStateStore stateStore, SharedSinkConnectors sharing) {
        this.provisioner = provisioner;
        this.stateStore = stateStore;
        this.sharing = sharing;
    }

    @Override
    public SinkWriter open(SinkConfig config) {
        Map<String, TargetTable> targets = config.target() == null ? Map.of() : Map.of(config.target().name(), config.target());
        return open(config, targets);
    }

    public SinkWriter open(SinkConfig config, Map<String, TargetTable> targets) {
        return open(config, provisioner.resolve(config.connectorId()), targets, false);
    }

    /**
     * A writer for tables prepared before it opened, by {@link #prepare}: refused, before any connector is opened,
     * unless each one was; then it only writes rows. Several writers of one sink open this way, and none of them
     * clears a table another has started writing.
     *
     * <p>Each opens a connector of its own, unless the artifact is certified to be shared: then the writers of a
     * sink on this member share one, started by the first of them and stopped by the last.
     */
    public SinkWriter openPrepared(SinkConfig config, Map<String, TargetTable> targets) {
        PdkTargetPreparation.requirePrepared(config.node(), stateStore, targets.values());
        ConnectorRef ref = provisioner.resolve(config.connectorId());
        if (sharing == null || config.node() == null || !ref.shareSafe()) {
            return open(config, ref, targets, true);
        }
        PdkConnector connector = sharing.acquire(config.node(), config.connectorId(), ref,
                () -> started(config, ref));
        Runnable letGo = () -> sharing.release(config.node(), config.connectorId(), ref);
        WriteRecordFunction write;
        try {
            write = requireWriteFunction(connector.functions().getWriteRecordFunction());
        } catch (RuntimeException e) {
            letGo.run();
            throw e;
        }
        try {
            PdkSinkWriter writer = new PdkSinkWriter(connector, write, config, targets, stateStore, true, letGo);
            writer.prepareTargets();
            return writer;
        } catch (TapstateException e) {
            letGo.run();
            throw e;
        } catch (Throwable t) {
            letGo.run();
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }
    }

    /** A connector opened and started for the writers that will share it, stopped again if it fails to start. */
    private PdkConnector started(SinkConfig config, ConnectorRef ref) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), ref, config.settings(), config.node(), stateStore);
        try {
            connector.underLoader(() -> {
                connector.connector().init(connector.context());
                return null;
            });
            return connector;
        } catch (TapstateException e) {
            connector.stopQuietly();
            connector.close();
            throw e;
        } catch (Throwable t) {
            connector.stopQuietly();
            connector.close();
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }
    }

    /**
     * Prepares each of {@code targets} for the writers that will write it - created where it is missing, cleared
     * or checked as the full-load policy says, indexed - on a connector opened for that alone and closed again.
     * A table prepared before, as its receipt says, is not cleared again. A failure is coded as a write failure,
     * as it was when each writer prepared its own tables.
     */
    public void prepare(SinkConfig config, Map<String, TargetTable> targets) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), provisioner.resolve(config.connectorId()), config.settings(),
                config.node(), stateStore);
        try {
            connector.underLoader(() -> {
                connector.connector().init(connector.context());
                PdkTargetPreparation preparation = new PdkTargetPreparation(connector.context(),
                        connector.functions(), config.onFullLoad(), config.fullLoad(), config.node(), stateStore);
                for (TargetTable target : targets.values()) {
                    TapTable table = TargetTapTable.build(target);
                    connector.resolveTargetTypes(table);
                    preparation.prepare(target, table);
                }
                return null;
            });
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        } finally {
            connector.stopQuietly();
            connector.close();
        }
    }

    private SinkWriter open(SinkConfig config, ConnectorRef ref, Map<String, TargetTable> targets,
            boolean preparedAhead) {
        PdkConnector connector = PdkConnector.open(config.connectorId(), ref, config.settings(), config.node(),
                stateStore);
        WriteRecordFunction write;
        try {
            write = requireWriteFunction(connector.functions().getWriteRecordFunction());
        } catch (RuntimeException e) {
            connector.close();
            throw e;
        }
        try {
            connector.underLoader(() -> {
                connector.connector().init(connector.context());
                return null;
            });
            PdkSinkWriter writer = new PdkSinkWriter(connector, write, config, targets, stateStore, preparedAhead);
            writer.prepareTargets();
            return writer;
        } catch (TapstateException e) {
            connector.stopQuietly();
            connector.close();
            throw e;
        } catch (Throwable t) {
            connector.stopQuietly();
            connector.close();
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }
    }

    private static WriteRecordFunction requireWriteFunction(WriteRecordFunction function) {
        if (function == null) {
            throw new IllegalStateException("connector does not provide a write capability");
        }
        return function;
    }
}
