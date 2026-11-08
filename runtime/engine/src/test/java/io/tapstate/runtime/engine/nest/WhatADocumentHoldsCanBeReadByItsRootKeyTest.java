package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.NestInspection;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reading what a nest step holds for one root key: the root's order, each part beneath it with its own, parts
 * waiting for a parent that has not arrived, and the document as it stands - found from the key's text even
 * though the key was filed typed, and read from the state rather than from any target.
 */
class WhatADocumentHoldsCanBeReadByItsRootKeyTest {

    private static final String PIPELINE = "p";
    private static final String STEP = "doc";

    private HazelcastInstance member;
    private NestTopology topology;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("nest-inspect-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);

        Embed claims = new Embed("cl", Map.of("policy_id", "policy_id"), EmbedAs.ARRAY, "claims",
                List.of("claim_id"), null, null, null);
        Embed policies = new Embed("p", Map.of("customer_id", "customer_id"), EmbedAs.ARRAY, "policies",
                List.of("policy_id"), null, null, List.of(claims));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("c", List.of("customer_id"), null, null, List.of(policies)));
        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("c", new NestTable("customers", List.of("customer_id")));
        tables.put("p", new NestTable("policies", List.of("policy_id")));
        tables.put("cl", new NestTable("claims", List.of("claim_id")));
        topology = NestTopology.compile(PIPELINE, STEP, body, tables::get);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aWholeDocumentListsItsPartsAndRendersAsItWentOut() {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(Map.of("customer_id", 7L, "name", "Ann"), at(3));
        assembly.applyElement(new ElementRef(List.of("policies"), null, List.of("P1"), "P1"),
                Map.of("policy_id", "P1", "customer_id", 7L), at(4), Map.of());
        assembly.applyElement(new ElementRef(List.of("policies", "claims"), "P1", List.of("CL1"), null),
                Map.of("claim_id", "CL1", "policy_id", "P1"), at(5), Map.of());
        assembly.documentSent();
        file(7L, assembly);

        NestInspection seen = held(NestInspector.inspect(member, PIPELINE, STEP, topology, List.of("7")));

        assertThat(seen.rootKey()).containsExactly(7L);
        assertThat(seen.rootKeyColumns()).containsExactly("customer_id");
        assertThat(seen.rootPresent()).isTrue();
        assertThat(seen.rootOrder()).isEqualTo("1:3");
        assertThat(seen.parts()).extracting(NestInspection.Part::path, NestInspection.Part::order)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("policies", "1:4"),
                        org.assertj.core.groups.Tuple.tuple("policies.claims", "1:5"));
        assertThat(seen.unsent()).isZero();
        assertThat(seen.document()).containsEntry("name", "Ann").containsKey("policies");
        assertThat(seen.inTransit()).isFalse();
    }

    @Test
    void partsUnderARootThatNeverArrivedAreHeldAndNoDocumentExists() {
        RootAssembly orphaned = new RootAssembly();
        orphaned.applyElement(new ElementRef(List.of("policies"), null, List.of("P9"), "P9"),
                Map.of("policy_id", "P9", "customer_id", 9L), at(6), Map.of());
        orphaned.applyElement(new ElementRef(List.of("policies", "claims"), "P8", List.of("CL8"), null),
                Map.of("claim_id", "CL8", "policy_id", "P8"), at(7), Map.of());
        file(9L, orphaned);

        NestInspection seen = held(NestInspector.inspect(member, PIPELINE, STEP, topology, List.of("9")));

        assertThat(seen.rootPresent()).isFalse();
        assertThat(seen.rootOrder()).isNull();
        assertThat(seen.document()).isNull();
        assertThat(seen.parts()).extracting(NestInspection.Part::key).containsExactly(List.of("P9"));
        assertThat(seen.waiting()).singleElement().satisfies(waiting -> {
            assertThat(waiting.path()).isEqualTo("policies");
            assertThat(waiting.parentKey()).isEqualTo("P8");
            assertThat(waiting.held()).isOne();
        });
        assertThat(seen.unsent()).isEqualTo(2L);
    }

    @Test
    void aKeyNothingIsFiledUnderAndAKeyOfTheWrongWidthAreToldApart() {
        assertThat(NestInspector.inspect(member, PIPELINE, STEP, topology, List.of("404")))
                .isInstanceOf(NestInspector.Outcome.NotHeld.class);
        assertThat(NestInspector.inspect(member, PIPELINE, STEP, topology, List.of("1", "2")))
                .isEqualTo(new NestInspector.Outcome.WrongArity(List.of("customer_id")));
    }

    @Test
    void textIsReadAsEveryTypeAKeyMayHaveBeenFiledAs() {
        assertThat(NestInspector.readings(List.of("17"))).containsExactly(
                List.of(17L), List.of(17), List.of(new java.math.BigDecimal("17")), List.of("17"));
        assertThat(NestInspector.readings(List.of("A-1"))).containsExactly(List.of("A-1"));
    }

    private void file(long customer, RootAssembly assembly) {
        member.getMap(topology.assembler().mapName())
                .put(NestKeys.valuesOf(Map.of("customer_id", customer), List.of("customer_id"),
                        topology.assembler().mapName()), assembly);
    }

    private static NestInspection held(NestInspector.Outcome outcome) {
        assertThat(outcome).isInstanceOf(NestInspector.Outcome.Held.class);
        return ((NestInspector.Outcome.Held) outcome).inspection();
    }

    private static SourceOrder at(long seq) {
        return new SourceOrder(1L, seq);
    }
}
