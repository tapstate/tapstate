package io.tapstate.app;

import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import io.tapstate.spi.store.StorePort;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Binds the cold rollup worker to current artifact identity and local actuation ownership. */
@Configuration
@ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
class HistoryRollupConfiguration {

    @Bean(destroyMethod = "close")
    HistoryRollupWorker historyRollupWorker(StorePort storePort, MetricsHistoryProperties history,
            ClusterMembershipGate membership, PipelineActuationOwnership actuation, Clock clock) {
        ArtifactStore artifacts = storePort.artifacts();
        return new HistoryRollupWorker(storePort.rateHistory(), storePort.historyRollups(), clock,
                history.getSampleInterval(), HistoryRollupWorker.DEFAULT_BATCH_SIZE,
                HistoryRollupWorker.DEFAULT_INTERVAL,
                () -> currentWork(storePort.desired().pipelineIds(), artifacts),
                work -> membership.businessEligible() && currentOwner(artifacts, work)
                        && actuation.permit(work.pipelineId()).granted(), true);
    }

    static List<HistoryRollupWorker.Work> currentWork(List<String> pipelineIds, ArtifactStore artifacts) {
        List<HistoryRollupWorker.Work> work = new ArrayList<>();
        for (String pipelineId : pipelineIds) {
            Optional<ArtifactStore.HistoryOwner> owner = artifacts.pipelineHistoryOwner(pipelineId);
            owner.ifPresent(found -> {
                Visibility visibility = found.visibility();
                if (visibility.includeLegacy()) {
                    work.add(new HistoryRollupWorker.Work(pipelineId, Scope.legacy()));
                }
                if (visibility.incarnationId() != null) {
                    work.add(new HistoryRollupWorker.Work(pipelineId,
                            Scope.incarnation(visibility.incarnationId())));
                }
            });
        }
        return List.copyOf(work);
    }

    static boolean currentOwner(ArtifactStore artifacts, HistoryRollupWorker.Work work) {
        return artifacts.pipelineHistoryOwner(work.pipelineId()).map(owner -> {
            Visibility visibility = owner.visibility();
            return work.scope().incarnationId().map(id -> id.equals(visibility.incarnationId()))
                    .orElseGet(visibility::includeLegacy);
        }).orElse(false);
    }
}
