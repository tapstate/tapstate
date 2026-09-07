package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.runtime.engine.nest.NestMemoryBudget;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.KeyedStateStore;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A budget a pipeline asks for bounds what its state maps actually hold.
 *
 * <p>Every other reading of a budget is of the configuration rather than of the map: the number is
 * written, it is broadcast, it reads back as the number that was asked for, and none of that says a
 * single entry was ever put out of memory. This one writes more entries than the budget allows and
 * counts what is left resident, because that is the only question a budget is asked.
 *
 * <p><b>Why it is written against the way the process actually starts.</b> A namespace belongs to a
 * pipeline, and pipelines do not exist when the member does, so a per-namespace budget can only be
 * added once the member is running. What decides whether it then applies is where the process-wide
 * pattern for these maps was put: the substrate resolves a map's configuration by looking through the
 * static configuration by pattern first and only then at what was added while it ran, so a pattern
 * placed in the static configuration answers for every namespace and the exact configuration behind it
 * is never reached. Measured, that is not a difference the substrate reports and not one the
 * configuration reads back: both halves of the number look right and the map runs on the other one.
 *
 * <p>So the member here is built the way the product builds it, rather than by hand. A test that placed
 * the pattern itself would be asserting over its own arrangement and would pass whichever way the
 * product had it.
 */
class ABudgetAPipelineAsksForBoundsWhatIsHeldTest {

    /** One namespace, named the way a compiled topology names one. */
    private static final String NAMESPACE = "nest.a-pipeline.order_doc.$root";

    /** The smallest budget the maps take, which is one entry per partition. */
    private static final long BUDGET = 271L;

    /** Comfortably more than the budget, so what is held is a decision rather than a coincidence. */
    private static final int WRITTEN = 700;

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("more entries than the budget leaves the budget resident, not all of them")
    void aBudgetAppliedWhileTheMemberRunsPutsWhatIsPastItOutOfMemory() {
        member = memberBuiltTheWayTheProductBuildsOne();
        NestMemoryBudget.applyTo(member, Set.of(NAMESPACE),
                NestSettings.defaults().withEntriesHeldInMemory(BUDGET));

        IMap<Object, Object> map = member.getMap(NAMESPACE);
        for (int i = 0; i < WRITTEN; i++) {
            map.set("key-" + i, "state-" + i);
        }

        long resident = map.getLocalMapStats().getOwnedEntryCount();
        // Both directions in one claim, because each rules out a different way of passing. Above the
        // write count and the budget bounded nothing: the configuration reads back as the number asked
        // for, the substrate reports nothing, and the only trace is this count. Far below it and
        // something other than the budget removed the entries, or nothing was measured at all - a
        // reading of zero would satisfy an upper bound on its own.
        assertThat(resident)
                .describedAs("%d entries were written under a budget of %d and %d are resident. All of "
                        + "them means the budget bounds nothing, which is not a slower run but a member "
                        + "that fills up with every reading healthy; almost none of them means they went "
                        + "somewhere this case is not about. The budget is spent per partition, so what "
                        + "should be left is near it", WRITTEN, BUDGET, resident)
                .isBetween(BUDGET / 2, (long) WRITTEN - 1);
    }

    /**
     * The member the product starts, through the product's own assembly rather than a hand-built config.
     * What the state maps are is part of that assembly, and where it puts them is the whole subject here.
     */
    private static HazelcastInstance memberBuiltTheWayTheProductBuildsOne() {
        KeyedStateStore cold = new InMemoryKeyedStateStore();
        Config config = HazelcastConfiguration.memberConfig(
                loopbackProperties(), cold, NestSettings.defaults());
        HazelcastInstance started = HazelcastConfiguration.startMember(
                () -> Hazelcast.newHazelcastInstance(config));
        NestStateMapStoreFactory.bindTo(started, cold);
        HazelcastConfiguration.makeNestCapable(started, cold, NestSettings.defaults());
        return started;
    }

    private static HazelcastProperties loopbackProperties() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("budget-bounds-" + System.nanoTime());
        return properties;
    }
}
