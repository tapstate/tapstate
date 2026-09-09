package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The same carrying as the case beside this one, on the shape a pipeline actually runs: what travels between
 * the two identities sits on a map with a store behind it rather than on a heap.
 *
 * <p><b>The difference is not a detail of the fixture.</b> A heap store files entries by the key object and
 * never asks what that key is called; a map with a layer behind it files under a name. So every case in this
 * tree that carried anything between one identity and another exercised the one arrangement where a key is
 * never named - and the whole family of defects in the naming of these keys was, structurally, not reachable
 * from any of them. It was reached by a running pipeline instead, which is a place a defect costs a great
 * deal more to find.
 *
 * <p>Both cases here are about that reachability rather than about the carrying, which is why the first of
 * them renames nothing: a row of a vertex's own embed asks what may have been left for the identity it now
 * has, every time, and asking is where the name is needed.
 */
class WhatWaitedUnderARenamedIdentityTravelsThroughTheStateLayerTest {

    private static final TransformBody.Nest TRACKED = nest("customer", List.of("customer_id"),
            tracking(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TRACKED, tables());
    private static final NestVertex POLICIES = TOPOLOGY.vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;
    private static final int CLAIMS = 1;

    private final HeapKeyedStateStore cold = new HeapKeyedStateStore();
    private final TestOutbox outbox = new TestOutbox(256);

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    /**
     * The plain row, which is the one that matters for how this defect stayed hidden. Nothing is renamed and
     * nothing is waiting, so there is nothing to carry - but the row still asks whether anything was left for
     * the identity it now has, and that question is answered by the layer behind the map, under a name.
     */
    @Test
    void anOwnRowAsksWhatWasLeftForItWithoutFailingOnTheName() throws Exception {
        ResolverProcessor policies = resolver();

        feed(policies, OWN_ROWS, policyInserted(1, "P1", "C1"));

        assertThat(drain())
                .describedAs("the row travels as an element of the document, as it always did")
                .hasSize(1);
    }

    /**
     * The carrying itself, with every entry involved on a map: the claim waits under the value it was told to
     * point at, the policy renames that value, and what was waiting is answered under the new one. The count
     * in the layer between the two halves is what says it really travelled through it.
     */
    @Test
    void aClaimWaitingUnderTheOldIdentityIsAnsweredUnderTheNewOne() throws Exception {
        ResolverProcessor policies = resolver();
        feed(policies, CLAIMS, claim(1, "K1", "P1"));
        assertThat(drain()).describedAs("nothing can travel while the parent is unknown").isEmpty();

        feed(policies, twinOf(POLICIES.pathId()), policyRenamed(2, "P1", "P2", "C1"));

        assertThat(cold.count(POLICIES.parkingMapName()))
                .describedAs("what the vacated identity gave up is in the layer both halves reach")
                .isEqualTo(1L);

        feed(policies, OWN_ROWS, policyRenamed(2, "P1", "P2", "C1"));

        assertThat(drain())
                .describedAs("the policy itself, and the claim that had been waiting under the old value")
                .hasSize(2);
        assertThat(cold.count(POLICIES.parkingMapName()))
                .describedAs("taken in rather than left behind, in the layer as well as in memory")
                .isZero();
    }

    /**
     * The same carrying with the parking area evicted out of memory in between, which is the state that
     * decides what an ask of this area actually risks. In an ordinary round every one of those asks answers
     * null, and a null from a map with a layer behind it has two readings that are identical from the
     * outside: nothing was ever left here, or what was left was dropped on its way out of memory. Only the
     * second one is data loss, and no count of trips can tell them apart.
     *
     * <p>Evicting on purpose separates them. What is evicted is gone from memory and not from the layer, so
     * if the claim still lands the ask is fetching it back rather than finding it missing - and an ask that
     * answers null is then costing a trip and never a row.
     */
    @Test
    void aClaimWaitingIsStillAnsweredAfterTheParkingAreaIsEvicted() throws Exception {
        ResolverProcessor policies = resolver();
        feed(policies, CLAIMS, claim(1, "K1", "P1"));
        assertThat(drain()).describedAs("nothing can travel while the parent is unknown").isEmpty();

        feed(policies, twinOf(POLICIES.pathId()), policyRenamed(2, "P1", "P2", "C1"));
        assertThat(cold.count(POLICIES.parkingMapName()))
                .describedAs("what the vacated identity gave up reached the layer behind the map")
                .isEqualTo(1L);

        // Out of memory, still in the layer - which is the one arrangement where the two readings of a
        // null differ, and the only one in which the answer can be got wrong without anything saying so.
        member.getMap(POLICIES.parkingMapName()).evictAll();

        feed(policies, OWN_ROWS, policyRenamed(2, "P1", "P2", "C1"));

        assertThat(drain())
                .describedAs("the policy, and the claim that was waiting - fetched back rather than lost")
                .hasSize(2);
        assertThat(cold.count(POLICIES.parkingMapName()))
                .describedAs("taken in rather than left behind, eviction or not")
                .isZero();
    }

    private ResolverProcessor resolver() throws Exception {
        member = startMember(cold);
        NestBinding.NestStores stores = NestBinding.onMap().bind(member);
        ResolverProcessor processor = new ResolverProcessor(POLICIES, stores.forResolver(POLICIES),
                (from, released) -> { }, null, null, ReplayFloor.NONE, NestClock.SYSTEM,
                NestSettings.defaults(), stores.forParking(POLICIES));
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static int twinOf(List<String> pathId) {
        for (NestInbound edge : POLICIES.inbound()) {
            if (edge.carriesDepartures() && edge.pathId().equals(pathId)) {
                return edge.ordinal();
            }
        }
        throw new AssertionError("no departure edge carries " + pathId);
    }

    private void feed(ResolverProcessor processor, int ordinal, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(event);
        processor.process(ordinal, inbox);
    }

    private List<Object> drain() {
        List<Object> out = new ArrayList<>();
        outbox.drainQueueAndReset(0, out, false);
        return out;
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return Envelope.insert(seq, "claim", row("claim_id", claimId, "policy_id", policyId), null)
                .withOrder(at(seq));
    }

    private static Envelope policyInserted(long seq, String policyId, String customerId) {
        return Envelope.insert(seq, "policy",
                row("policy_id", policyId, "customer_id", customerId, "policy_no", "PN-1"), null)
                .withOrder(at(seq));
    }

    /** The policy row with the column its claims point at renamed from {@code was} to {@code is}. */
    private static Envelope policyRenamed(long seq, String was, String is, String customerId) {
        return Envelope.update(seq, "policy",
                row("policy_id", was, "customer_id", customerId, "policy_no", "PN-1"),
                row("policy_id", is, "customer_id", customerId, "policy_no", "PN-1"), null)
                .withOrder(at(seq));
    }

    private static HazelcastInstance startMember(KeyedStateStore store) {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        HazelcastInstance started = Hazelcast.newHazelcastInstance(config);
        // The configuration names the store and the member is what holds it, so it is bound after the
        // member exists and before any map on it is used.
        NestStateMapStoreFactory.bindTo(started, store);
        return started;
    }
}
