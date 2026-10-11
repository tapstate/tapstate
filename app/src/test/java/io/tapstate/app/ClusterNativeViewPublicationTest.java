package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.ClusterNativeObservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ClusterNativeViewPublicationTest {
    private static final String CLUSTER = "native-view-cluster";
    private static final Duration INTERVAL = Duration.ofHours(1);
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile(CLUSTER, 3,
            new ExecutionProfile(1, Map.of("profile-protocol", "2", "runtime", "test")));
    private static final WorkloadClaim PUBLISHER = new WorkloadClaim(
            new WorkloadClaimKey(CLUSTER, WorkloadClaimType.NODE_SESSION, "a"), new WorkloadOwner("a", "boot-a"),
            2, 0, 0, Instant.parse("2026-10-11T10:00:30Z"), 0, 0, Set.of(), 0, false, PROFILE.generation());

    @Test void everyMemberOfOneActualSampleIsPublishedWithItsRuntimeIdentity() {
        Fixture fixture = new Fixture();
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            int before = fixture.samples.get();

            controller.reconcile();

            assertThat(fixture.samples.get()).isEqualTo(before + 1);
            ClusterNativeObservation actual = fixture.last.get();
            assertThat(actual.profile()).isEqualTo(PROFILE);
            assertThat(actual.membersByNode()).containsOnlyKeys("a", "b", "c");
            actual.membersByNode().forEach((node, nativeMember) -> {
                assertThat(nativeMember.identity().nodeId()).isEqualTo(node);
                assertThat(nativeMember.identity().bootId()).isEqualTo("boot-" + node);
                assertThat(nativeMember.identity().memberUuid()).isEqualTo(uuid(node).toString());
                assertThat(nativeMember.memberAddress()).isEqualTo(address(node).toString());
            });
            verify(fixture.profiles, atLeastOnce()).recordNativeView(same(PUBLISHER), eq(7L), any(), eq(INTERVAL.multipliedBy(3)));
            assertThat(fixture.gate.committed().activeNodeIds()).containsExactlyInAnyOrder("a", "b", "c");
        }
    }

    @Test void aSingletonCanReportItsWholeViewWithoutShrinkingTheCommittedMajority() {
        Fixture fixture = new Fixture();
        fixture.visible.set(Set.of(fixture.a));
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            controller.reconcile();

            assertThat(fixture.last.get().membersByNode()).containsOnlyKeys("a");
            assertThat(fixture.gate.businessEligible()).isFalse();
            assertThat(fixture.gate.committed().activeNodeIds()).containsExactlyInAnyOrder("a", "b", "c");
            verify(fixture.membership, never()).compareAndSetProfile(any(), anyLong(), anySet());
            verify(fixture.membership, never()).initializeProfile(any(), anyLong(), anySet());
        }
    }

    @Test void bootstrapPublicationUsesTheRevisionActuallyCommittedBeforeIt() {
        Fixture fixture = new Fixture();
        when(fixture.membership.read(CLUSTER)).thenReturn(Optional.empty());
        var initialized = new ClusterMembership(CLUSTER, 8, Set.of("a", "b", "c"), PROFILE.generation());
        when(fixture.membership.initializeProfile(PUBLISHER, 0, Set.of("a", "b", "c")))
                .thenReturn(Optional.of(initialized));
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();

            var order = inOrder(fixture.membership, fixture.profiles);
            order.verify(fixture.membership).initializeProfile(PUBLISHER, 0, Set.of("a", "b", "c"));
            order.verify(fixture.profiles).recordNativeView(same(PUBLISHER), eq(8L), any(), eq(INTERVAL.multipliedBy(3)));
            assertThat(fixture.gate.committed()).isEqualTo(initialized);
        }
    }

    @Test void aRefusedPublicationDoesNotCreateAnotherLocalRunningBusinessRevocation() {
        Fixture fixture = new Fixture();
        fixture.publicationAccepted = false;
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            controller.reconcile();

            assertThat(fixture.last.get().membersByNode()).containsOnlyKeys("a", "b", "c");
            assertThat(fixture.gate.businessEligible()).isTrue();
            assertThat(fixture.gate.committed().revision()).isEqualTo(7);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"lite", "missing-node", "blank-boot", "missing-uuid", "missing-address",
            "foreign-generation", "foreign-hash", "duplicate-node", "duplicate-uuid", "duplicate-address",
            "missing-local", "foreign-local-boot", "two-local-members", "empty"})
    void anInvalidMemberInvalidatesTheWholeSampleRatherThanPublishingAFilteredPool(String defect) {
        Fixture fixture = new Fixture();
        Set<Member> peers = new HashSet<>(fixture.visible.get());
        switch (defect) {
            case "lite" -> when(fixture.b.isLiteMember()).thenReturn(true);
            case "missing-node" -> when(fixture.b.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(null);
            case "blank-boot" -> when(fixture.b.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(" ");
            case "missing-uuid" -> when(fixture.b.getUuid()).thenReturn(null);
            case "missing-address" -> when(fixture.b.getAddress()).thenReturn(null);
            case "foreign-generation" -> when(fixture.b.getAttribute(ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE)).thenReturn("2");
            case "foreign-hash" -> when(fixture.b.getAttribute(ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE)).thenReturn("other");
            case "duplicate-node" -> peers.add(nativeMember("b", "boot-other", UUID.fromString("00000000-0000-4000-8000-000000000099"), address("d"), false));
            case "duplicate-uuid" -> {
                UUID existing = fixture.a.getUuid();
                when(fixture.b.getUuid()).thenReturn(existing);
            }
            case "duplicate-address" -> {
                var existing = fixture.a.getAddress();
                when(fixture.b.getAddress()).thenReturn(existing);
            }
            case "missing-local" -> when(fixture.a.localMember()).thenReturn(false);
            case "foreign-local-boot" -> when(fixture.a.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("other-boot");
            case "two-local-members" -> when(fixture.b.localMember()).thenReturn(true);
            case "empty" -> peers.clear();
            default -> throw new AssertionError(defect);
        }
        fixture.visible.set(Set.copyOf(peers));
        try (ClusterMembershipController controller = fixture.controller()) {
            controller.reconcile();

            assertThat(fixture.last.get()).isNull();
            assertThat(fixture.gate.businessEligible()).isFalse();
            verify(fixture.profiles, never()).recordNativeView(any(), anyLong(), any(), any());
            verify(fixture.membership, never()).compareAndSetProfile(any(), anyLong(), anySet());
            verify(fixture.membership, never()).initializeProfile(any(), anyLong(), anySet());
        }
    }

    @Test void aMissingOrDifferentAcceptedProfileCannotBorrowTodaysNativeIdentities() {
        for (Optional<ClusterExecutionProfile> current : java.util.List.of(Optional.<ClusterExecutionProfile>empty(),
                Optional.of(new ClusterExecutionProfile(CLUSTER, 4, PROFILE.profile())))) {
            Fixture fixture = new Fixture();
            when(fixture.profiles.profile(CLUSTER)).thenReturn(current);
            try (ClusterMembershipController controller = fixture.controller()) {
                controller.reconcile();

                assertThat(fixture.last.get()).isNull();
                assertThat(fixture.gate.businessEligible()).isFalse();
                verify(fixture.profiles, never()).recordNativeView(any(), anyLong(), any(), any());
            }
        }
    }

    @Test void aRefusedJoinKeepsTheActuallyRereadCompatibleMembershipRenewable() {
        Fixture fixture = new Fixture();
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            fixture.showJoiningMember();
            ClusterMembership admitted = new ClusterMembership(CLUSTER, 7,
                    Set.of("a", "b", "c"), PROFILE.generation());
            when(fixture.membership.read(CLUSTER)).thenReturn(Optional.of(admitted));
            when(fixture.membership.compareAndSetProfile(PUBLISHER, 7, Set.of("a", "b", "c", "d")))
                    .thenReturn(Optional.empty());
            clearInvocations(fixture.membership, fixture.profiles);

            controller.reconcile();

            assertThat(fixture.gate.committed()).isEqualTo(admitted);
            assertThat(fixture.gate.businessEligible()).isTrue();
            assertThat(fixture.gate.submissionEligible(fixture.visible.get()))
                    .as("a still uncommitted joining member cannot enter a new submission")
                    .isFalse();
            assertThat(fixture.claims.renew(fixture.capture, fixture.captureTtl)).contains(fixture.capture);
            verify(fixture.captureStore).renew(same(fixture.capture), eq(fixture.captureTtl));
            verify(fixture.membership).compareAndSetProfile(PUBLISHER, 7, Set.of("a", "b", "c", "d"));
            verify(fixture.profiles).recordNativeView(same(PUBLISHER), eq(7L), any(), eq(INTERVAL.multipliedBy(3)));
            assertThat(fixture.last.get().membersByNode()).containsOnlyKeys("a", "b", "c", "d");
        }
    }

    @Test void aCompetingJoinUsesTheActuallyRereadRevisionForBusinessAndNativePublication() {
        Fixture fixture = new Fixture();
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            fixture.showJoiningMember();
            ClusterMembership admitted = new ClusterMembership(CLUSTER, 7,
                    Set.of("a", "b", "c"), PROFILE.generation());
            ClusterMembership latest = new ClusterMembership(CLUSTER, 8,
                    Set.of("a", "b", "c", "d"), PROFILE.generation());
            when(fixture.membership.read(CLUSTER)).thenReturn(Optional.of(admitted), Optional.of(latest));
            when(fixture.membership.compareAndSetProfile(PUBLISHER, 7, Set.of("a", "b", "c", "d")))
                    .thenReturn(Optional.empty());
            clearInvocations(fixture.membership, fixture.profiles);

            controller.reconcile();

            assertThat(fixture.gate.committed()).isEqualTo(latest);
            assertThat(fixture.gate.businessEligible()).isTrue();
            assertThat(fixture.claims.renew(fixture.capture, fixture.captureTtl)).contains(fixture.capture);
            verify(fixture.captureStore).renew(same(fixture.capture), eq(fixture.captureTtl));
            verify(fixture.profiles).recordNativeView(same(PUBLISHER), eq(8L), any(), eq(INTERVAL.multipliedBy(3)));
            verify(fixture.profiles, never()).recordNativeView(any(), eq(7L), any(), any());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "different-profile", "unavailable"})
    void aRefusedJoinWithNoCompatibleRereadRemainsFailClosed(String outcome) {
        Fixture fixture = new Fixture();
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            fixture.showJoiningMember();
            ClusterMembership admitted = new ClusterMembership(CLUSTER, 7,
                    Set.of("a", "b", "c"), PROFILE.generation());
            switch (outcome) {
                case "missing" -> when(fixture.membership.read(CLUSTER))
                        .thenReturn(Optional.of(admitted), Optional.empty());
                case "different-profile" -> when(fixture.membership.read(CLUSTER))
                        .thenReturn(Optional.of(admitted), Optional.of(new ClusterMembership(CLUSTER, 8,
                                Set.of("a", "b", "c", "d"), PROFILE.generation() + 1)));
                case "unavailable" -> when(fixture.membership.read(CLUSTER))
                        .thenReturn(Optional.of(admitted))
                        .thenThrow(new TapstateException(IoError.STORE_UNAVAILABLE,
                                Map.of("detail", "membership-reread-unavailable"), null));
                default -> throw new AssertionError(outcome);
            }
            when(fixture.membership.compareAndSetProfile(PUBLISHER, 7, Set.of("a", "b", "c", "d")))
                    .thenReturn(Optional.empty());
            fixture.last.set(null);
            clearInvocations(fixture.membership, fixture.profiles);

            controller.reconcile();

            assertThat(fixture.gate.committed()).isNull();
            assertThat(fixture.gate.businessEligible()).isFalse();
            assertThat(fixture.claims.renew(fixture.capture, fixture.captureTtl)).isEmpty();
            verify(fixture.captureStore, never()).renew(any(), any());
            assertThat(fixture.last.get()).isNull();
            verify(fixture.profiles, never()).recordNativeView(any(), anyLong(), any(), any());
        }
    }

    @Test void aRefusedJoinCannotKeepTheSmallerDenominatorWhenTheRereadAlreadyHasEightMembers() {
        Fixture fixture = new Fixture();
        try (ClusterMembershipController controller = fixture.controller()) {
            fixture.awaitFirstAttempt();
            fixture.showJoiningMember();
            ClusterMembership admitted = new ClusterMembership(CLUSTER, 7,
                    Set.of("a", "b", "c"), PROFILE.generation());
            ClusterMembership latest = new ClusterMembership(CLUSTER, 9,
                    Set.of("a", "b", "c", "d", "e", "f", "g", "h"), PROFILE.generation());
            when(fixture.membership.read(CLUSTER)).thenReturn(Optional.of(admitted), Optional.of(latest));
            when(fixture.membership.compareAndSetProfile(PUBLISHER, 7, Set.of("a", "b", "c", "d")))
                    .thenReturn(Optional.empty());
            clearInvocations(fixture.membership, fixture.profiles);

            controller.reconcile();

            assertThat(fixture.gate.committed()).isEqualTo(latest);
            assertThat(fixture.gate.visibleNodeIds()).containsExactlyInAnyOrder("a", "b", "c", "d");
            assertThat(fixture.gate.businessEligible()).isFalse();
            assertThat(fixture.claims.renew(fixture.capture, fixture.captureTtl)).isEmpty();
            verify(fixture.captureStore, never()).renew(any(), any());
            verify(fixture.profiles).recordNativeView(same(PUBLISHER), eq(9L), any(), eq(INTERVAL.multipliedBy(3)));
            assertThat(fixture.last.get().membersByNode()).containsOnlyKeys("a", "b", "c", "d");
        }
    }

    private static final class Fixture {
        final Member a = nativeMember("a", "boot-a", uuid("a"), address("a"), true);
        final Member b = nativeMember("b", "boot-b", uuid("b"), address("b"), false);
        final Member c = nativeMember("c", "boot-c", uuid("c"), address("c"), false);
        final AtomicReference<Set<Member>> visible = new AtomicReference<>(Set.of(a, b, c));
        final ClusterMembershipStore membership = mock(ClusterMembershipStore.class);
        final ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        final HazelcastInstance member = mock(HazelcastInstance.class);
        final ClusterMembershipGate gate;
        final Duration captureTtl = Duration.ofSeconds(30);
        final WorkloadClaim capture = new WorkloadClaim(
                new WorkloadClaimKey(CLUSTER, WorkloadClaimType.CAPTURE, "capture-a"), PUBLISHER.owner(),
                5, 0, 7, Instant.now().plus(captureTtl), 0, 0, Set.of(), 0, false, PROFILE.generation());
        final WorkloadClaimStore captureStore = mock(WorkloadClaimStore.class);
        final ClusterWorkloadClaims claims;
        final AtomicInteger samples = new AtomicInteger();
        final AtomicReference<ClusterNativeObservation> last = new AtomicReference<>();
        volatile boolean publicationAccepted = true;

        Fixture() {
            Cluster cluster = mock(Cluster.class);
            when(member.getCluster()).thenReturn(cluster);
            when(cluster.getMembers()).thenAnswer(call -> {
                samples.incrementAndGet();
                return visible.get();
            });
            when(membership.read(CLUSTER)).thenReturn(Optional.of(
                    new ClusterMembership(CLUSTER, 7, Set.of("a", "b", "c"), PROFILE.generation())));
            when(profiles.profile(CLUSTER)).thenReturn(Optional.of(PROFILE));
            when(profiles.recordNativeView(any(), anyLong(), any(), any())).thenAnswer(call -> {
                last.set(call.getArgument(2));
                return publicationAccepted;
            });
            ClusterProperties properties = new ClusterProperties();
            properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            properties.setBootstrapMinMembers(3);
            gate = new ClusterMembershipGate(properties);
            gate.bindProfile(PROFILE.generation(), PROFILE.profile().hash());
            claims = new ClusterWorkloadClaims(captureStore, gate);
            when(captureStore.renew(same(capture), eq(captureTtl))).thenReturn(Optional.of(capture));
        }

        void showJoiningMember() {
            Member d = nativeMember("d", "boot-d", uuid("d"), address("d"), false);
            visible.set(Set.of(a, b, c, d));
        }

        ClusterMembershipController controller() {
            return new ClusterMembershipController(CLUSTER, member, membership, gate, INTERVAL, PUBLISHER, profiles);
        }

        void awaitFirstAttempt() {
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (last.get() == null) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new AssertionError("the first native-view publication was never attempted");
                }
                Thread.onSpinWait();
            }
        }
    }

    private static Member nativeMember(String node, String boot, UUID uuid,
            com.hazelcast.cluster.Address address, boolean local) {
        Member peer = mock(Member.class);
        when(peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(node);
        when(peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(boot);
        when(peer.getAttribute(ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE)).thenReturn(Long.toString(PROFILE.generation()));
        when(peer.getAttribute(ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE)).thenReturn(PROFILE.profile().hash());
        when(peer.getUuid()).thenReturn(uuid);
        when(peer.getAddress()).thenReturn(address);
        when(peer.localMember()).thenReturn(local);
        return peer;
    }

    private static UUID uuid(String node) {
        return UUID.fromString("00000000-0000-4000-8000-00000000000" + (node.charAt(0) - 'a' + 1));
    }

    private static com.hazelcast.cluster.Address address(String node) {
        return new com.hazelcast.cluster.Address(InetAddress.getLoopbackAddress(), 5701 + node.charAt(0) - 'a');
    }
}
