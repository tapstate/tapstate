package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.SrsMetaStore;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * A start with everything around it held in memory: applied artifacts, the desired intent, the audit
 * log, and the targets -- what each holds is set per test, keyed by {@code <connection>/<table>}.
 *
 * <p>The start plan is predicted with no chain records, so every start here is a new full load; the
 * judgement itself is held against the run's own decision elsewhere, where both sides exist.
 */
final class StartChecksBench {

    static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-02T08:00:00Z"), ZoneOffset.UTC);

    static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src
            connector: mysql
            config: { host: 10.0.0.1, database: crm, username: u, password: p }
            mode: cdc
            tables: [ orders ]
            """;

    static final String WAREHOUSE = """
            version: tapstate/v1
            kind: source
            id: warehouse
            connector: mongodb
            config: { uri: "mongodb://warehouse:27017/dw" }
            """;

    static String pipeline(String onFullLoad) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: pl
                source: src
                serve:
                  from: orders
                  sync: [ { id: out, source: warehouse%s } ]
                """.formatted(onFullLoad == null ? "" : ", on_full_load: " + onFullLoad);
    }

    final Artifacts artifacts = new Artifacts();
    final Map<String, DesiredState> desiredById = new HashMap<>();
    final List<AuditRecord> audit = new ArrayList<>();
    final Map<String, Function<String, TargetProbe.TargetRows>> targets = new HashMap<>();
    final List<StartCheck> checks = new ArrayList<>(List.of(new TargetNotEmptyCheck()));
    final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    Duration budget = StartCheckEvaluator.BUDGET;
    Duration perTarget = StartCheckEvaluator.PER_TARGET;

    final DesiredStore desired = new DesiredStore() {
        @Override
        public void save(DesiredState state) {
            desiredById.put(state.pipelineId(), state);
        }

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            return Optional.ofNullable(desiredById.get(pipelineId));
        }

        @Override
        public List<String> pipelineIds() {
            return List.copyOf(desiredById.keySet());
        }

        @Override
        public void delete(String pipelineId) {
            desiredById.remove(pipelineId);
        }
    };

    final AuditGate auditGate = new AuditGate(audit::add, CLOCK);
    final PipelineLifecycleService lifecycle = new PipelineLifecycleService(
            new ArtifactQueryService(artifacts), desired, auditGate, pipelineId -> Optional.empty());
    final ApplyService apply = new ApplyService(TapstateCatalog::load, artifacts, auditGate,
            new EmptySchemaStore(), PlanAdvisories.none(), SchemaDerivation.none());

    StartChecksBench() {
        artifacts.put(SOURCE);
        artifacts.put(WAREHOUSE);
    }

    /** The target {@code coordinate} holds {@code rows}. */
    StartChecksBench holding(String coordinate, TargetProbe.TargetRows rows) {
        targets.put(coordinate, ignored -> rows);
        return this;
    }

    StartCheckEvaluator evaluator() {
        return new StartCheckEvaluator(checks, new StartPlanner(pipelineId -> List.of(), noChainRecords(),
                this::writeTargets), (connection, table) -> {
                    Function<String, TargetProbe.TargetRows> answer = targets.get(connection + "/" + table);
                    return answer == null ? TargetProbe.TargetRows.EMPTY : answer.apply(table);
                },
                (code, params) -> code.code() + " " + new java.util.TreeMap<>(params), executor, CLOCK,
                budget, perTarget);
    }

    PipelineStartService service() {
        return new PipelineStartService(lifecycle, evaluator(), apply);
    }

    String hash(String id) {
        return CanonicalHash.of(artifacts.get(id).orElseThrow());
    }

    PipelineResource stored(String id) {
        return (PipelineResource) artifacts.get(id).orElseThrow();
    }

    /** A plain sync of the one source table, and the view's collection named after the view. */
    List<PipelineWriteTargets.WriteTarget> writeTargets(PipelineResource definition) {
        List<PipelineWriteTargets.WriteTarget> written = new ArrayList<>();
        List<SyncElement> sync = null;
        String definedIn = null;
        if (definition.serve() instanceof ServeBlock.Inline inline) {
            sync = inline.sync();
        } else if (definition.serve() instanceof ServeBlock.Use use) {
            sync = ((ServeResource) artifacts.get(use.use()).orElseThrow()).sync();
            definedIn = use.use();
        }
        if (sync != null) {
            for (SyncElement element : sync) {
                written.add(new PipelineWriteTargets.WriteTarget(element.id(),
                        PipelineWriteTargets.WriteTarget.Kind.SYNC, element.source(), "orders",
                        element.onFullLoad() == null ? OnFullLoad.APPEND : element.onFullLoad(), definedIn));
            }
        }
        if (definition.view() instanceof ViewBlock.Inline view) {
            written.add(new PipelineWriteTargets.WriteTarget(view.id(), PipelineWriteTargets.WriteTarget.Kind.VIEW,
                    "views", view.id(), view.onFullLoad() == null ? OnFullLoad.APPEND : view.onFullLoad(), null));
        }
        return written;
    }

    /** No chain is read here, so nothing may ask for a chain's record. */
    private static SrsMetaStore noChainRecords() {
        return (SrsMetaStore) Proxy.newProxyInstance(StartChecksBench.class.getClassLoader(),
                new Class<?>[] {SrsMetaStore.class}, (proxy, method, args) -> {
                    throw new AssertionError("no chain is read in this bench: " + method.getName());
                });
    }

    /** Applied artifacts, written conditionally the way the store writes them. */
    static final class Artifacts implements ArtifactStore {
        final Map<String, Resource> byId = new LinkedHashMap<>();

        void put(String document) {
            Resource resource = new DslParser().parse(document);
            byId.put(resource.id(), resource);
        }

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(resource -> byId.put(resource.id(), resource));
        }

        @Override
        public Optional<String> saveAll(List<Resource> artifacts, Map<String, String> expectedContentHashes) {
            for (Map.Entry<String, String> precondition : expectedContentHashes.entrySet()) {
                Resource stored = byId.get(precondition.getKey());
                if (stored == null || !CanonicalHash.of(stored).equals(precondition.getValue())) {
                    return Optional.of(precondition.getKey());
                }
            }
            saveAll(artifacts);
            return Optional.empty();
        }

        @Override
        public ArtifactBatchWrite writeAll(List<ArtifactWrite> writes) {
            Map<String, String> preconditions = new LinkedHashMap<>();
            writes.forEach(write -> preconditions.putAll(write.readPreconditions()));
            Optional<String> refused = saveAll(writes.stream().map(ArtifactWrite::resource).toList(), preconditions);
            return refused.map(id -> ArtifactBatchWrite.refused(id,
                            io.tapstate.spi.store.ArtifactMutation.VERSION_CONFLICT))
                    .orElseGet(ArtifactBatchWrite::applied);
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(byId.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(byId.values());
        }
    }
}
