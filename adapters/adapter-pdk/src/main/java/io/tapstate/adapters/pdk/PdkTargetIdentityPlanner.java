package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.TargetTable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reads actual target constraints; the resolver and projection are shared by checks, previews and writes. */
public final class PdkTargetIdentityPlanner {
    private static final Set<String> DOCUMENT_IDENTITIES = Set.of(
            "mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");
    private final ConnectorProvisioner provisioner;

    public PdkTargetIdentityPlanner(ConnectorProvisioner provisioner) {
        this.provisioner = provisioner;
    }

    public Map<String, TargetIdentityPlan> assess(SinkConfig config, Map<String, TargetTable> targets) {
        if (targets.values().stream().noneMatch(table -> table.unwindIdentity() != null)) {
            Map<String, TargetIdentityPlan> unchanged = new LinkedHashMap<>();
            targets.forEach((stream, table) -> unchanged.put(stream, unchanged(table)));
            return unchanged;
        }
        PdkConnector connector = PdkConnector.open(config.connectorId(),
                provisioner.resolve(config.connectorId()), config.settings());
        try {
            return connector.underLoader(() -> {
                connector.connector().init(connector.context());
                return assess(connector, config, targets);
            });
        } catch (TapstateException coded) {
            throw coded;
        } catch (Throwable failure) {
            throw failed(config.connectorId(), "", failure);
        } finally {
            connector.stopQuietly();
            connector.close();
        }
    }

    static Map<String, TargetIdentityPlan> assess(PdkConnector connector, SinkConfig config,
            Map<String, TargetTable> targets) {
        return assess(connector,config,targets,io.tapstate.spi.capture.SourceOperationObserver.NONE);
    }
    static Map<String,TargetIdentityPlan> assess(PdkConnector connector,SinkConfig config,Map<String,TargetTable> targets,
            io.tapstate.spi.capture.SourceOperationObserver observer) {
        Map<String, TargetIdentityPlan> plans = new LinkedHashMap<>();
        targets.forEach((stream,target)->{
            try {
                var plan=assess(connector,config,target,true);
                if(target.unwindIdentity()!=null)report(observer,target.name(),null);
                plans.put(stream,plan);
            }catch(RuntimeException | Error failure){if(target.unwindIdentity()!=null)report(observer,target.name(),failure);throw failure;}
        });
        return TargetIdentityResolver.reconcile(plans);
    }
    private static void report(io.tapstate.spi.capture.SourceOperationObserver observer,String table,Throwable failure) {
        try {observer.completed(table,io.tapstate.spi.capture.SourceOperationObserver.Capability.TARGET_READ,failure);}
        catch(RuntimeException unavailable){org.slf4j.LoggerFactory.getLogger(PdkTargetIdentityPlanner.class).warn("Target identity observation is degraded");}
    }

    private static TargetIdentityPlan assess(PdkConnector connector, SinkConfig config, TargetTable target, boolean allowPreparation) {
        if (target.unwindIdentity() == null) return unchanged(target);
        try {
            return connector.underLoader(() -> resolveTarget(connector, config, target, allowPreparation));
        } catch (TapstateException coded) {
            throw coded;
        } catch (Throwable failure) {
            throw failed(connector.connectorId(), target.name(), failure);
        }
    }

    private record TargetShape(List<String> primary, Set<String> generated,
            Set<String> columns, Set<String> nullable) { }

    private static TargetIdentityPlan resolveTarget(PdkConnector connector, SinkConfig config,
            TargetTable target, boolean allowPreparation) throws Throwable {
        boolean documentIdentity = DOCUMENT_IDENTITIES.contains(connector.connectorId());
        TapTable existing = existingTable(connector, target.name(), documentIdentity);
        TargetShape shape = targetShape(existing, documentIdentity);
        var evidence = indexEvidence(connector, target.name(), existing, documentIdentity, shape.primary());
        boolean exactRelational = PdkIdentityIndexes.supports(connector.connectorId());
        boolean mayPrepare = mayPrepareIndex(connector, config, existing, documentIdentity, allowPreparation);
        return TargetIdentityResolver.resolve(target, new TargetIdentityFacts(existing != null,
                documentIdentity, shape.primary(), shape.generated(), shape.columns(), evidence.keys(),
                shape.nullable(), documentIdentity, connector.functions().getWriteRecordFunction() != null
                        && (documentIdentity || exactRelational), mayPrepare, evidence.opaqueUniqueConstraints(),
                evidence.signedInt64Columns()));
    }

    private static TapTable existingTable(PdkConnector connector, String table, boolean documentIdentity)
            throws Throwable {
        if (documentIdentity) {
            var names = connector.functions().getGetTableNamesFunction();
            if (names == null) throw new IllegalStateException("target collection metadata is unavailable");
            List<String> tables = new ArrayList<>();
            connector.requireActive();
            names.tableNames(connector.context(), 1000, tables::addAll);
            // Flexible target columns do not require sampling user documents to infer a schema.
            return tables.contains(table) ? new TapTable(table) : null;
        }
        List<TapTable> discovered = new ArrayList<>();
        connector.requireActive();
        connector.connector().discoverSchema(connector.context(), List.of(table), 1, discovered::addAll);
        return discovered.stream().filter(candidate -> table.equals(candidate.getId())).findFirst().orElse(null);
    }

    private static TargetShape targetShape(TapTable existing, boolean documentIdentity) {
        List<String> primary = List.of();
        if (documentIdentity) primary = List.of("_id");
        else if (existing != null) primary = new ArrayList<>(existing.primaryKeys());
        Set<String> generated = new LinkedHashSet<>();
        Set<String> columns = new LinkedHashSet<>();
        Set<String> nullable = new LinkedHashSet<>();
        if (documentIdentity) generated.add("_id");
        if (existing != null && existing.getNameFieldMap() != null) {
            for (var entry : existing.getNameFieldMap().entrySet()) {
                String name = entry.getKey();
                var field = entry.getValue();
                columns.add(name);
                if (Boolean.TRUE.equals(field.getAutoInc())) generated.add(name);
                if (!Boolean.FALSE.equals(field.getNullable()) && !primary.contains(name)) nullable.add(name);
            }
        }
        return new TargetShape(primary, generated, columns, nullable);
    }

    private static PdkIdentityIndexes.Evidence indexEvidence(PdkConnector connector, String table,
            TapTable existing, boolean documentIdentity, List<String> primary) throws Throwable {
        List<List<String>> unique = new ArrayList<>();
        if (!primary.isEmpty()) unique.add(primary);
        if (existing == null) return new PdkIdentityIndexes.Evidence(unique, false, Set.of());
        if (documentIdentity) return PdkIdentityIndexes.mongo(connector, table);
        unique.addAll(genericUniqueKeys(connector, existing));
        if (PdkIdentityIndexes.supports(connector.connectorId())) {
            // Partial, expression and prefix indexes are not proof of full-key uniqueness.
            return PdkIdentityIndexes.read(connector, table);
        }
        return new PdkIdentityIndexes.Evidence(unique, true, Set.of());
    }

    private static List<List<String>> genericUniqueKeys(PdkConnector connector, TapTable table) throws Throwable {
        List<TapIndex> indexes = new ArrayList<>();
        if (connector.functions().getQueryIndexesFunction() != null) {
            connector.requireActive();
            connector.functions().getQueryIndexesFunction().query(connector.context(), table, indexes::addAll);
        } else if (table.getIndexList() != null) {
            indexes.addAll(table.getIndexList());
        } else {
            throw new IllegalStateException("target index metadata is unavailable");
        }
        List<List<String>> unique = new ArrayList<>();
        for (TapIndex index : indexes) {
            if (!Boolean.TRUE.equals(index.getUnique()) && !Boolean.TRUE.equals(index.getPrimary())) continue;
            if (index.getIndexFields() == null || index.getIndexFields().isEmpty()
                    || index.getIndexFields().stream().anyMatch(field -> field.getName() == null)) {
                throw new IllegalStateException("a target unique index cannot be represented by field names");
            }
            unique.add(index.getIndexFields().stream().map(io.tapdata.entity.schema.TapIndexField::getName).toList());
        }
        return unique;
    }

    private static boolean mayPrepareIndex(PdkConnector connector, SinkConfig config, TapTable existing,
            boolean documentIdentity, boolean allowPreparation) throws Throwable {
        boolean mayPrepare = existing == null;
        if (documentIdentity && existing != null) {
            boolean clearing = config.fullLoad() && config.onFullLoad() == OnFullLoad.CLEAR;
            if (clearing) {
                mayPrepare = true;
            } else if (connector.functions().getCountByPartitionFilterFunction() != null) {
                connector.requireActive();
                long count = connector.functions().getCountByPartitionFilterFunction()
                        .countByPartitionFilter(connector.context(), existing, TapAdvanceFilter.create());
                mayPrepare = count == 0;
            }
        }
        return mayPrepare && allowPreparation && connector.functions().getCreateIndexFunction() != null;
    }

    static void verifyPrepared(PdkConnector connector, SinkConfig config, Map<String, TargetTable> targets,
            Map<String, TargetIdentityPlan> expected) {verifyPrepared(connector,config,targets,expected,io.tapstate.spi.capture.SourceOperationObserver.NONE);}
    static void verifyPrepared(PdkConnector connector,SinkConfig config,Map<String,TargetTable> targets,
            Map<String,TargetIdentityPlan> expected,io.tapstate.spi.capture.SourceOperationObserver observer) {
        targets.forEach((stream, target) -> {
            if (!TargetIdentityPlan.AUTOMATIC.equals(expected.get(stream).status())) return;
            TargetIdentityPlan checked;
            try {checked=assess(connector,config,target,false);report(observer,target.name(),null);}
            catch(RuntimeException | Error failure){report(observer,target.name(),failure);throw failure;}
            TargetIdentityProjection.requireUsable(checked);
            if (!TargetIdentityPlan.AUTOMATIC.equals(checked.status()) || !checked.writeKey().equals(expected.get(stream).writeKey())) {
                throw failed(config.connectorId(), target.name(),
                        new IllegalStateException("target identity changed during preparation"));
            }
        });
    }

    static TargetIdentityPlan unchanged(TargetTable target) {
        return new TargetIdentityPlan(TargetIdentityPlan.NOT_REQUIRED, "compatible-key", target, null, null,
                target.fields().stream().filter(io.tapstate.spi.sink.TargetField::primaryKey)
                        .map(io.tapstate.spi.sink.TargetField::name).toList());
    }

    static void requireSame(SinkConfig config, Map<String, TargetIdentityPlan> expected,
            Map<String, TargetIdentityPlan> actual) {
        if (expected != null && !expected.equals(actual)) {
            throw failed(config.connectorId(), "",
                    new IllegalStateException("target identity differs from the frozen preparation model"));
        }
    }

    private static TapstateException failed(String connector, String table, Throwable failure) {
        return new TapstateException(ConnectorError.TARGET_IDENTITY_CHECK_FAILED,
                Map.of("connector", connector, "table", table,
                        "detail", failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage()), failure);
    }
}
