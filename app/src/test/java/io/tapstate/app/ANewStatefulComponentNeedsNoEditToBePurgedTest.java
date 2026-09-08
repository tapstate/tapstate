package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.lifecycle.PipelineStateInventory;
import io.tapstate.runtime.engine.Engine;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * That a component which starts keeping state is cleared, and spoken about, by declaring itself and
 * doing nothing else.
 *
 * <p>The component here is invented for the test and the product has never heard of it. It supplies one
 * declaration -- a name, whose the state is, and the namespace it keeps it under -- and that is the
 * whole of what it does. Nothing in the clearing path is taught about it and no sentence anywhere is
 * edited; if either were needed, this would fail.
 *
 * <p>This case is the design constraint's only evidence. Every implementation that hard-codes the kinds
 * it knows -- a branch per component in the clearing, a sentence listing them by hand -- passes every
 * other test in this repository and fails exactly these two assertions. Without it, "adding a component
 * needs one declaration" is a sentence in a document rather than a property of the code.
 */
class ANewStatefulComponentNeedsNoEditToBePurgedTest {

    private static final String PIPELINE = "orders_pipe";

    /** What the invented component keeps, and what it calls it. Both are its own to choose. */
    private static final String ITS_NAMESPACE = "somethingnew." + PIPELINE + ".whatever_it_keeps";
    private static final String ITS_LABEL = "the ledger a brand new component keeps";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("new-stateful-component-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aStopClearsWhatTheNewComponentDeclaredWithoutBeingTaughtAboutIt() {
        InMemoryStorePort store = new InMemoryStorePort();
        store.keyedState().save(ITS_NAMESPACE, "k", "held".getBytes(StandardCharsets.UTF_8));
        assertThat(store.keyedState().load(ITS_NAMESPACE, "k"))
                .as("the seeding took, so what is asserted after the stop is a difference the stop made")
                .isPresent();

        actuator(store).stop(PIPELINE, true);

        assertThat(store.keyedState().load(ITS_NAMESPACE, "k"))
                .as("a component that declared where it keeps state is let go of by that declaration alone")
                .isEmpty();
    }

    @Test
    void aStopSaysTheNewComponentsNameWithoutAnySentenceBeingEdited() {
        String said = PipelineStateInventory.describe(true, new NewComponentDagSource().stateHeldBy(PIPELINE));

        assertThat(said)
                .as("the sentence a stop says is rendered from the same declarations it clears by")
                .contains(ITS_LABEL);
    }

    @Test
    void aStopAskedToKeepSaysTheSameNameOnTheOtherPath() {
        // The pair matters. A rendering that walked the declarations for the destructive path and a
        // hand-written sentence for the other would pass the case above and leave the new component
        // unmentioned in the one a cautious user reads.
        assertThat(PipelineStateInventory.describe(false, new NewComponentDagSource().stateHeldBy(PIPELINE)))
                .contains(ITS_LABEL);
    }

    private EngineLifecycleActuator actuator(InMemoryStorePort store) {
        return new EngineLifecycleActuator(
                new Engine(member),
                new NewComponentDagSource(),
                new NoOpCaptureCoordinator(),
                new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
    }

    /**
     * A topology whose only stateful component is one the product has never heard of. It declares the
     * namespace it keeps state under and what to call it; nothing else about it exists anywhere.
     */
    private static final class NewComponentDagSource implements DagSource {

        @Override
        public DAG dagFor(String pipelineId) {
            return new IdleDagSource().dagFor(pipelineId);
        }

        @Override
        public List<PipelineStateHolding> stateHeldBy(String pipelineId) {
            return List.of(new PipelineStateHolding(
                    ITS_LABEL, PipelineStateHolding.Scope.PIPELINE, Set.of(ITS_NAMESPACE)));
        }

        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }
    }
}
