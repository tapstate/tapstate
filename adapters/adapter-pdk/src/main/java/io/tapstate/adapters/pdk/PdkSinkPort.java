package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkPort;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.Objects;
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
    private final PipelineNode.Purpose purpose;
    private final ConnectorNoteScopes noteScopes;
    private final WorkloadClaimFence execution;
    private final PdkSinkPreparations preparations;
    private final Map<String, TargetIdentityPlan> plannedIdentities;

    private io.tapstate.spi.capture.SourceOperationObserver operationObserver=io.tapstate.spi.capture.SourceOperationObserver.NONE;
    private java.util.function.Supplier<io.tapstate.spi.capture.SourceOperationObserver> sharedInitialization=()->operationObserver;
    public PdkSinkPort withSharedInitialization(java.util.function.Supplier<io.tapstate.spi.capture.SourceOperationObserver> observer){sharedInitialization=Objects.requireNonNull(observer);return this;}
    public PdkSinkPort withOperationObserver(io.tapstate.spi.capture.SourceOperationObserver observer){operationObserver=Objects.requireNonNull(observer);return this;}
    private void operation(io.tapstate.spi.capture.SourceOperationObserver.Capability capability,Throwable failure){report(operationObserver,capability,failure);}
    private void report(io.tapstate.spi.capture.SourceOperationObserver observer,io.tapstate.spi.capture.SourceOperationObserver.Capability capability,Throwable failure){
        try{observer.completed(capability,failure);}catch(RuntimeException unavailable){org.slf4j.LoggerFactory.getLogger(getClass()).warn("Target operation collection is degraded");}
    }
    private void initialize(PdkConnector connector) throws Throwable {
        try {connector.connector().init(connector.context());}
        catch(Throwable failure){operation(io.tapstate.spi.capture.SourceOperationObserver.Capability.INITIALIZE,failure);throw failure;}
        operation(io.tapstate.spi.capture.SourceOperationObserver.Capability.INITIALIZE,null);
    }
    /** The exact assessed models frozen into this run's preparation roster, separate from input lineage. */
    public PdkSinkPort withTargetIdentities(Map<String, TargetIdentityPlan> identities) {
        return new PdkSinkPort(provisioner, stateStore, sharing, purpose, noteScopes, execution, preparations,
                Objects.requireNonNull(identities, "identities")).withOperationObserver(operationObserver).withSharedInitialization(sharedInitialization);
    }

    /** Configured writes use already prepared CONFIG notes and the caller's fixed run authority. */
    public static PdkSinkPort withRequiredNotes(ConnectorProvisioner provisioner, KeyedStateStore stateStore,
            SharedSinkConnectors sharing, PipelineNode.Purpose purpose, ConnectorNoteScopes noteScopes,
            WorkloadClaimFence execution) {
        return new PdkSinkPort(provisioner, stateStore, sharing, purpose,
                Objects.requireNonNull(noteScopes, "noteScopes"), Objects.requireNonNull(execution, "execution"));
    }

    /** Native configured writes keep target receipts in CACHE and necessary connector notes in CONFIG. */
    public static PdkSinkPort withScopedState(ConnectorProvisioner provisioner,SharedSinkConnectors sharing,
            PipelineNode.Purpose purpose,ConnectorNoteScopes noteScopes,WorkloadClaimFence execution,
            PdkSinkPreparations preparations) {
        return new PdkSinkPort(provisioner,null,sharing,purpose,Objects.requireNonNull(noteScopes,"noteScopes"),
                Objects.requireNonNull(execution,"execution"),Objects.requireNonNull(preparations,"preparations"));
    }

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
        this(provisioner, stateStore, sharing, PipelineNode.Purpose.SYNC);
    }

    public PdkSinkPort(ConnectorProvisioner provisioner, KeyedStateStore stateStore, SharedSinkConnectors sharing,
            PipelineNode.Purpose purpose) {
        this(provisioner, stateStore, sharing, purpose, null, null);
    }

    private PdkSinkPort(ConnectorProvisioner provisioner, KeyedStateStore stateStore, SharedSinkConnectors sharing,
            PipelineNode.Purpose purpose, ConnectorNoteScopes noteScopes, WorkloadClaimFence execution) {
        this(provisioner,stateStore,sharing,purpose,noteScopes,execution,null);
    }

    private PdkSinkPort(ConnectorProvisioner provisioner,KeyedStateStore stateStore,SharedSinkConnectors sharing,
            PipelineNode.Purpose purpose,ConnectorNoteScopes noteScopes,WorkloadClaimFence execution,PdkSinkPreparations preparations) {
        this(provisioner,stateStore,sharing,purpose,noteScopes,execution,preparations,null);
    }

    private PdkSinkPort(ConnectorProvisioner provisioner,KeyedStateStore stateStore,SharedSinkConnectors sharing,
            PipelineNode.Purpose purpose,ConnectorNoteScopes noteScopes,WorkloadClaimFence execution,
            PdkSinkPreparations preparations,Map<String,TargetIdentityPlan> plannedIdentities) {
        if(preparations!=null)preparations.requireClaim(execution);
        this.plannedIdentities=plannedIdentities==null?null:Map.copyOf(plannedIdentities);
        this.preparations=preparations;
        if (purpose != PipelineNode.Purpose.VIEW && purpose != PipelineNode.Purpose.SYNC) {
            throw new IllegalArgumentException("sink purpose must be view or sync");
        }
        this.purpose = purpose;
        this.noteScopes = noteScopes;
        this.execution = execution;
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
        requireInputs(config);requireActive();
        ConnectorRef ref = provisioner.resolve(config.connectorId());
        return open(config, ref, targets, false, configuredDrive(config, ref));
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
        requireInputs(config);requireActive();
        if(preparations!=null) {
            if(plannedIdentities!=null && !plannedIdentities.keySet().equals(targets.keySet()))
                throw new IllegalArgumentException("frozen identities must cover every input stream");
            var physical=plannedIdentities==null?targets.values():plannedIdentities.values().stream().map(TargetIdentityPlan::target).toList();
            for(var target:physical)preparations.requireReady(target);
        }
        ConnectorRef ref = provisioner.resolve(config.connectorId());
        ConfiguredDrive drive = configuredDrive(config, ref);
        SharedSinkConnectors.NoteIdentity identity = drive.identity();
        if(preparations==null)PdkTargetPreparation.requirePrepared(config.node(), stateStore, targets.values(), purpose);
        if (sharing == null || config.node() == null || !ref.shareSafe()) {
            return open(config, ref, targets, true, drive);
        }
        PdkConnector connector = sharing.acquire(config.node(), purpose, config.connectorId(), ref, identity,
                () -> started(config, ref, drive));
        Runnable letGo = () -> sharing.release(config.node(), purpose, config.connectorId(), ref, identity);
        try {
            return writerOver(connector, config, targets, letGo);
        } catch (Throwable t) {
            // However the writer failed to open, it lets go of the hold it took: the last hold stops the connector.
            letGo.run();
            throw t;
        }
    }

    /** A writer over {@code connector}, which the writers of its sink share; closing it runs {@code letGo}. */
    private PdkSinkWriter writerOver(PdkConnector connector, SinkConfig config, Map<String, TargetTable> targets,
            Runnable letGo) {
        WriteRecordFunction write = requireWriteFunction(connector.functions().getWriteRecordFunction());
        try {
            PdkSinkWriter writer = new PdkSinkWriter(connector, write, config, targets, stateStore, true, letGo, purpose,preparations,plannedIdentities,operationObserver).withOperationObserver(operationObserver);
            writer.prepareTargets();
            return writer;
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }
    }

    /** A connector opened and started for the writers that will share it, stopped again if it fails to start. */
    private PdkConnector started(SinkConfig config, ConnectorRef ref, ConfiguredDrive drive) {
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), ref, config.settings(), config.node(), stateStore, null, drive.scopes(), purpose);
        var initializing=sharedInitialization.get();
        try {
            connector.underLoader(() -> {
                try {connector.connector().init(connector.context());}
                catch(Throwable failure){report(initializing,io.tapstate.spi.capture.SourceOperationObserver.Capability.INITIALIZE,failure);throw failure;}
                report(initializing,io.tapstate.spi.capture.SourceOperationObserver.Capability.INITIALIZE,null);
                return null;
            });
            return connector;
        } catch (Throwable t) {
            connector.stopQuietly();
            connector.close();
            throw t instanceof TapstateException coded
                    ? coded : PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }finally{if(initializing!=operationObserver)initializing.close();}
    }

    /**
     * Prepares each of {@code targets} for the writers that will write it - created where it is missing, cleared
     * or checked as the full-load policy says, indexed - on a connector opened for that alone and closed again.
     * A table prepared before, as its receipt says, is not cleared again. A failure is coded as a write failure,
     * as it was when each writer prepared its own tables.
     */
    public void prepare(SinkConfig config, Map<String, TargetTable> targets) {
        requireInputs(config);requireActive();
        ConnectorRef ref = provisioner.resolve(config.connectorId());
        PdkWritePolicies.select(config.connectorId(),ref.spec(),config.writeMode());
        ConfiguredDrive drive = configuredDrive(config, ref);
        PdkConnector connector = PdkConnector.open(
                config.connectorId(), ref, config.settings(),
                config.node(), stateStore, null, drive.scopes(), purpose);
        try {
            connector.underLoader(() -> {
                initialize(connector);
                PdkTargetPreparation preparation = new PdkTargetPreparation(connector.connectorId(),
                        connector.context(), connector.functions(), config.onFullLoad(), config.fullLoad(),
                        config.node(), stateStore, false, purpose, connector::requireActive,preparations).withOperationObserver(operationObserver);
                Map<String, TargetIdentityPlan> identities = PdkTargetIdentityPlanner.assess(connector, config, targets,operationObserver);
                identities.values().forEach(TargetIdentityProjection::requireUsable);
                PdkTargetIdentityPlanner.requireSame(config, plannedIdentities, identities);
                for (TargetIdentityPlan identity : identities.values()) {
                    TargetTable target = identity.target();
                    TapTable table = TargetTapTable.build(target);
                    connector.resolveTargetTypes(table);
                    preparation.prepare(target, table);
                }
                PdkTargetIdentityPlanner.verifyPrepared(connector, config, targets, identities,operationObserver);
                return null;
            });
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        } finally {
            try {connector.stopQuietly();connector.close();}finally{operationObserver.close();}
        }
    }

    /** Read-only assessment under the configured note scopes and exact artifact, before the roster is frozen. */
    public Map<String, TargetIdentityPlan> assessTargets(SinkConfig config, Map<String, TargetTable> targets) {
        requireInputs(config);requireActive();
        if(targets.values().stream().noneMatch(target -> target.unwindIdentity()!=null)) {
            var unchanged=new java.util.LinkedHashMap<String,TargetIdentityPlan>();
            targets.forEach((stream,target) -> unchanged.put(stream,PdkTargetIdentityPlanner.unchanged(target)));
            return Map.copyOf(unchanged);
        }
        var ref=provisioner.resolve(config.connectorId());
        var drive=configuredDrive(config,ref);
        var connector=PdkConnector.open(config.connectorId(),ref,config.settings(),config.node(),stateStore,null,drive.scopes(),purpose);
        try {
            return connector.underLoader(() -> {
                initialize(connector);
                var identities=PdkTargetIdentityPlanner.assess(connector,config,targets,operationObserver);
                identities.values().forEach(TargetIdentityProjection::requireUsable);
                connector.requireActive();
                return Map.copyOf(identities);
            });
        } catch(TapstateException coded) {
            throw coded;
        } catch(Throwable failure) {
            throw PdkSinkWriter.writeFailed(config.connectorId(),failure);
        } finally {
            try {connector.stopQuietly();connector.close();}finally{operationObserver.close();}
        }
    }

    private SinkWriter open(SinkConfig config, ConnectorRef ref, Map<String, TargetTable> targets,
            boolean preparedAhead, ConfiguredDrive drive) {
        PdkConnector connector = PdkConnector.open(config.connectorId(), ref, config.settings(), config.node(),
                stateStore, null, drive.scopes(), purpose);
        WriteRecordFunction write;
        try {
            write = requireWriteFunction(connector.functions().getWriteRecordFunction());
        } catch (RuntimeException e) {
            connector.close();
            throw e;
        }
        try {
            connector.underLoader(() -> {
                initialize(connector);
                return null;
            });
            PdkSinkWriter writer = new PdkSinkWriter(connector, write, config, targets, stateStore, preparedAhead, null, purpose,preparations,plannedIdentities,operationObserver).withOperationObserver(operationObserver);
            writer.prepareTargets();
            return writer;
        } catch (TapstateException e) {
            operationObserver.close();connector.stopQuietly();
            connector.close();
            throw e;
        } catch (Throwable t) {
            operationObserver.close();connector.stopQuietly();
            connector.close();
            throw PdkSinkWriter.writeFailed(connector.connectorId(), t);
        }
    }

    private void requireInputs(SinkConfig config) {
        if(preparations!=null)preparations.requireInputs(config.node(),purpose,config.onFullLoad(),config.fullLoad());
    }

    private void requireActive() {
        if (noteScopes != null) noteScopes.requireActive();
    }

    private record ConfiguredDrive(SharedSinkConnectors.NoteIdentity identity, ConnectorNoteScopes scopes) { }

    private ConfiguredDrive configuredDrive(SinkConfig config, ConnectorRef ref) {
        if (noteScopes == null) return new ConfiguredDrive(null, null);
        requireActive();
        if (config.node() == null || execution.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || execution.executionGeneration() < 1 || !config.node().pipelineId().equals(execution.key().resourceId())) {
            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
        }
        var primary = noteScopes.open(ConnectorStateNamespace.of(config.node(), purpose));
        var global = noteScopes.open(ConnectorStateNamespace.GLOBAL);
        if (primary.owner().kind() != RequiredNoteOwner.Kind.PIPELINE
                || !primary.owner().ownerId().equals(config.node().pipelineId()) || !primary.owner().hasPurpose(purpose)
                || global.owner().kind() != RequiredNoteOwner.Kind.GLOBAL
                || !global.owner().ownerId().equals(execution.key().clusterId())
                || primary.owner().bindingVersion() != global.owner().bindingVersion()) {
            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
        }
        var origin = noteScopes.store().origin(primary);
        if (!origin.equals(ConnectorNoteOrigin.capture(config.connectorId(), ref.contentHash(), config.settings()))) {
            throw new TapstateException(IoError.NOTE_ORIGIN_MISMATCH, Map.of(), null);
        }
        requireActive();
        var identity = new SharedSinkConnectors.NoteIdentity(execution, primary, global, origin);
        ConnectorNoteScopes fixed = ConnectorNoteScopes.of(noteScopes.store(), namespace -> {
            if (namespace.equals(primary.owner().namespace())) return primary;
            if (namespace.equals(global.owner().namespace())) return global;
            throw new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
        }, noteScopes::requireActive);
        return new ConfiguredDrive(identity, fixed);
    }

    private static WriteRecordFunction requireWriteFunction(WriteRecordFunction function) {
        if (function == null) {
            throw new IllegalStateException("connector does not provide a write capability");
        }
        return function;
    }
}
