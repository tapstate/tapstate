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
        Map<String, TargetIdentityPlan> plans = new LinkedHashMap<>();
        targets.forEach((stream, target) -> plans.put(stream, assess(connector, config, target, true)));
        return plans;
    }

    private static TargetIdentityPlan assess(PdkConnector connector, SinkConfig config, TargetTable target, boolean allowPreparation) {
        if (target.unwindIdentity() == null) return unchanged(target);
        try {
            return connector.underLoader(() -> {
                boolean documentIdentity = DOCUMENT_IDENTITIES.contains(connector.connectorId());
                TapTable existing;
                if (documentIdentity) {
                    var names = connector.functions().getGetTableNamesFunction();
                    if (names == null) throw new IllegalStateException("target collection metadata is unavailable");
                    List<String> tables = new ArrayList<>();
                    names.tableNames(connector.context(), 1000, tables::addAll);
                    // Flexible target columns do not require sampling user documents to infer a schema.
                    existing = tables.contains(target.name()) ? new TapTable(target.name()) : null;
                } else {
                    List<TapTable> discovered = new ArrayList<>();
                    connector.connector().discoverSchema(connector.context(), List.of(target.name()), 1, discovered::addAll);
                    existing = discovered.stream().filter(table -> target.name().equals(table.getId()))
                            .findFirst().orElse(null);
                }
                List<String> primary = documentIdentity ? List.of("_id")
                        : existing == null ? List.of() : new ArrayList<>(existing.primaryKeys());
                Set<String> generated = new LinkedHashSet<>();
                Set<String> columns = new LinkedHashSet<>();
                Set<String> nullable = new LinkedHashSet<>();
                if (documentIdentity) generated.add("_id");
                if (existing != null && existing.getNameFieldMap() != null) {
                    existing.getNameFieldMap().forEach((name, field) -> {
                        columns.add(name);
                        if (Boolean.TRUE.equals(field.getAutoInc())) generated.add(name);
                        if (!Boolean.FALSE.equals(field.getNullable()) && !primary.contains(name)) nullable.add(name);
                    });
                }
                List<List<String>> unique = new ArrayList<>();
                if (!primary.isEmpty()) unique.add(primary);
                if (existing != null && !documentIdentity) {
                    List<TapIndex> indexes = new ArrayList<>();
                    if (connector.functions().getQueryIndexesFunction() != null) {
                        connector.functions().getQueryIndexesFunction().query(connector.context(), existing, indexes::addAll);
                    } else if (existing.getIndexList() != null) {
                        indexes.addAll(existing.getIndexList());
                    } else {
                        throw new IllegalStateException("target index metadata is unavailable");
                    }
                    for (TapIndex index : indexes) {
                        if (!Boolean.TRUE.equals(index.getUnique()) && !Boolean.TRUE.equals(index.getPrimary())) continue;
                        if (index.getIndexFields() == null || index.getIndexFields().isEmpty()
                                || index.getIndexFields().stream().anyMatch(field -> field.getName() == null)) {
                            throw new IllegalStateException("a target unique index cannot be represented by field names");
                        }
                        unique.add(index.getIndexFields().stream().map(field -> field.getName()).toList());
                    }
                }
                boolean opaqueUniqueConstraints = existing != null && !documentIdentity
                        && !PdkIdentityIndexes.supports(connector.connectorId());
                if (documentIdentity && existing != null) {
                    var evidence = PdkIdentityIndexes.mongo(connector, target.name());
                    unique = evidence.keys();
                    opaqueUniqueConstraints = evidence.opaqueUniqueConstraints();
                }
                boolean exactRelational = PdkIdentityIndexes.supports(connector.connectorId());
                if (existing != null && !documentIdentity && exactRelational) {
                    // Partial, expression and prefix indexes are not proof of full-key uniqueness.
                    var evidence = PdkIdentityIndexes.read(connector, target.name());
                    unique = evidence.keys();
                    opaqueUniqueConstraints = evidence.opaqueUniqueConstraints();
                }
                boolean mayPrepare = existing == null;
                if (documentIdentity && existing != null) {
                    boolean clearing = config.fullLoad() && config.onFullLoad() == OnFullLoad.CLEAR;
                    if (clearing) {
                        mayPrepare = true;
                    } else if (connector.functions().getCountByPartitionFilterFunction() != null) {
                        long count = connector.functions().getCountByPartitionFilterFunction()
                                .countByPartitionFilter(connector.context(), existing, TapAdvanceFilter.create());
                        mayPrepare = count == 0;
                    }
                }
                mayPrepare &= allowPreparation && connector.functions().getCreateIndexFunction() != null;
                return TargetIdentityResolver.resolve(target, new TargetIdentityFacts(existing != null,
                        documentIdentity, primary, generated, columns, unique, nullable, documentIdentity,
                        connector.functions().getWriteRecordFunction() != null
                                && (documentIdentity || exactRelational), mayPrepare, opaqueUniqueConstraints));
            });
        } catch (TapstateException coded) {
            throw coded;
        } catch (Throwable failure) {
            throw failed(connector.connectorId(), target.name(), failure);
        }
    }

    static void verifyPrepared(PdkConnector connector, SinkConfig config, Map<String, TargetTable> targets,
            Map<String, TargetIdentityPlan> expected) {
        targets.forEach((stream, target) -> {
            if (!TargetIdentityPlan.AUTOMATIC.equals(expected.get(stream).status())) return;
            TargetIdentityPlan checked = assess(connector, config, target, false);
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

    private static TapstateException failed(String connector, String table, Throwable failure) {
        return new TapstateException(ConnectorError.TARGET_IDENTITY_CHECK_FAILED,
                Map.of("connector", connector, "table", table,
                        "detail", failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage()), failure);
    }
}
