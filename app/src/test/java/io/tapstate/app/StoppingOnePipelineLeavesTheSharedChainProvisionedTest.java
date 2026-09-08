package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.StorePort;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/**
 * Stopping one pipeline of a shared mining chain gives back that pipeline's hold on the chain and nothing
 * else. The chain is torn down when the last consumer leaves it, and not one stop earlier.
 *
 * <p>The teardown says of itself that it is "a separate explicit act a pipeline stop never reaches", and
 * every stop reached it: it removed the chain outright, with no regard for who else was on it. What that
 * cost the pipelines still running was measured before any of this was changed -- the survivor's own stop
 * then threw {@code mining chain not provisioned}, and a restart of the stopped one took a generation of
 * its own, which is the shape the chain's own comment warns about ("two sources ordering their changes by
 * which arrived first").
 *
 * <p>Both halves are asserted, because each passes on its own under a wrong fix: never tearing down at all
 * satisfies the first and leaks every chain, and the old unconditional teardown satisfies the second.
 */
class StoppingOnePipelineLeavesTheSharedChainProvisionedTest {

    @Test
    void stoppingOneOfTwoLeavesTheChainStandingForTheOtherOnItsOwnGeneration() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p1");
        fixture.coordinator.startCapture("p2");
        long generationBothStartedOn = fixture.generationOf("p2");

        fixture.coordinator.stopCapture("p1", true);

        assertThat(fixture.srsCoordinator.isProvisioned(fixture.chain()))
                .as("the chain the survivor still reads is still open")
                .isTrue();
        assertThat(fixture.srsCoordinator.affectedConsumers(fixture.chain()))
                .as("and only the leaving pipeline's own membership was given back")
                .containsExactly("p2");

        fixture.coordinator.startCapture("p1");

        assertThat(fixture.generationOf("p1"))
                .as("a pipeline rejoining a chain that never closed reads under the generation in flight")
                .isEqualTo(generationBothStartedOn);
    }

    @Test
    void theSurvivorsOwnStopStillWorksAndCarriesTheChainAwayWithIt() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p1");
        fixture.coordinator.startCapture("p2");

        fixture.coordinator.stopCapture("p1", true);
        fixture.coordinator.stopCapture("p2", true);

        assertThat(fixture.srsCoordinator.isProvisioned(fixture.chain()))
                .as("the last consumer to leave closes the chain")
                .isFalse();
        assertThat(fixture.closedSubscriptions())
                .as("both captures were stopped, whatever happened to the chain")
                .isEqualTo(2);
    }

    @Test
    void aLonePipelineStillClosesTheChainOnItsOwnStop() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p1");

        fixture.coordinator.stopCapture("p1", true);

        assertThat(fixture.srsCoordinator.isProvisioned(fixture.chain()))
                .as("the only consumer is also the last one")
                .isFalse();
    }

    /** Two pipelines over one source, so both resolve to the very same mining chain. */
    private static final class Fixture {

        private final SrsCoordinator srsCoordinator = new SrsCoordinator(new InMemorySrsMetaStore());
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private final Map<String, Long> generationByPipeline = new ConcurrentHashMap<>();
        private final Map<String, Boolean> closes = new ConcurrentHashMap<>();
        private final SourceResource source = new SourceResource("orders_src", null, "mysql",
                Map.of("host", "h"), SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null);

        Fixture() {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(source);
            artifacts.save(pipeline("p1"));
            artifacts.save(pipeline("p2"));
            StorePort store = new InMemoryStorePort(artifacts);
            coordinator = new StoreBackedPipelineCaptureCoordinator(store, this::start, srsCoordinator,
                    new SnapshotBuffer());
        }

        /** Mirrors what the real run unit does: provision the chain, attach the consumer, keep the generation. */
        private CaptureRun start(CaptureRunSpec spec, java.util.function.Consumer<Envelope> passthrough) {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            long generation = srsCoordinator
                    .provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention())
                    .epoch();
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            generationByPipeline.put(spec.pipelineId(), generation);
            String pipelineId = spec.pipelineId();
            Subscription subscription = () -> closes.put(pipelineId, true);
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(),
                    Optional.of(subscription), new CaptureHealth());
        }

        MiningChainId chain() {
            return MiningChainId.resolve(SourceCaptureResolution.of(source).config(), null);
        }

        long generationOf(String pipelineId) {
            return generationByPipeline.get(pipelineId);
        }

        int closedSubscriptions() {
            return closes.size();
        }

        private static PipelineResource pipeline(String id) {
            return new PipelineResource(id, null, List.of(SourceRef.spec("orders_src", true)), null, null,
                    new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                            List.of(new SyncElement("sync_1", "orders_src", null, null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
        }
    }
}
