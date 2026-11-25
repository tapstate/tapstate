package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineStartService;
import io.tapstate.control.core.PipelineWriteTargets;
import io.tapstate.control.core.StartCheckEvaluator;
import io.tapstate.control.core.StartChecks;
import io.tapstate.control.core.StartPlanner;
import io.tapstate.control.core.TargetProbe;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SyncElement;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.SrsMetaStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * The start checks every bundle that serves the start verb needs, with targets a test can set: a sync
 * element writes the table named for it in {@link StartCheckTargets#tables}, and a table holds what
 * {@link StartCheckTargets#rows} says, nothing when it says nothing. A bundle that sets neither starts
 * every pipeline with no start check findings at all, as it did before start checks existed.
 *
 * <p>The start plan is predicted with no chain records, so every start here is a new full load.
 */
@Configuration
class StartChecksTestConfiguration {

    @Bean
    StartCheckTargets startCheckTargets() {
        return new StartCheckTargets();
    }

    @Bean
    PipelineStartService pipelineStartService(
            PipelineLifecycleService lifecycle, ApplyService applyService, StartCheckTargets targets) {
        MessageCatalog catalog = MessageCatalog.bundled();
        StartCheckEvaluator evaluator = new StartCheckEvaluator(StartChecks.registered(),
                new StartPlanner(pipelineId -> List.of(), noChainRecords(), targets::writeTargets),
                targets::rows, (code, params) -> catalog.render(code, params).message(),
                Executors.newVirtualThreadPerTaskExecutor(), Clock.systemUTC());
        return new PipelineStartService(lifecycle, evaluator, applyService);
    }

    private static SrsMetaStore noChainRecords() {
        return (SrsMetaStore) Proxy.newProxyInstance(StartChecksTestConfiguration.class.getClassLoader(),
                new Class<?>[] {SrsMetaStore.class}, (proxy, method, args) -> {
                    throw new AssertionError("no chain is read in this bundle: " + method.getName());
                });
    }

    /** What a test says its pipelines write and its targets hold. */
    static final class StartCheckTargets {

        /** The table each sync element writes, by element id; an element not named here writes nothing. */
        final Map<String, String> tables = new ConcurrentHashMap<>();
        /** What each {@code <connection>/<table>} holds; a table not named here holds nothing. */
        final Map<String, TargetProbe.TargetRows> rows = new ConcurrentHashMap<>();

        List<PipelineWriteTargets.WriteTarget> writeTargets(PipelineResource definition) {
            List<PipelineWriteTargets.WriteTarget> written = new ArrayList<>();
            if (definition.serve() instanceof ServeBlock.Inline inline && inline.sync() != null) {
                for (SyncElement element : inline.sync()) {
                    String table = tables.get(element.id());
                    if (table != null) {
                        written.add(new PipelineWriteTargets.WriteTarget(element.id(),
                                PipelineWriteTargets.WriteTarget.Kind.SYNC, element.source(), table,
                                element.onFullLoad() == null ? OnFullLoad.APPEND : element.onFullLoad(), null));
                    }
                }
            }
            return written;
        }

        TargetProbe.TargetRows rows(String connection, String table) {
            return rows.getOrDefault(connection + "/" + table, TargetProbe.TargetRows.EMPTY);
        }

        void clear() {
            tables.clear();
            rows.clear();
        }
    }
}
