package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The REPL's mutable connection state. It starts offline; {@code connect} records the ordered seed
 * list plus the landing node and seeds the member set; {@code authenticate} records an opaque
 * credential (a signed session token today, a machine token later — the shape is mechanism-agnostic)
 * plus the principal to show and any discovered members; {@code logout} clears the credential but
 * keeps the connection; {@code disconnect} clears everything. Connecting is decoupled from
 * authenticating, so a connected session may still be unauthenticated.
 */
class SessionTest {

    private static final URI NODE1 = URI.create("http://node1:7900");
    private static final URI NODE2 = URI.create("http://node2:7900");

    @Test
    void aFreshSessionIsOfflineAndUnauthenticated() {
        Session session = new Session();
        assertThat(session.isConnected()).isFalse();
        assertThat(session.isAuthenticated()).isFalse();
        assertThat(session.landingNode()).isNull();
        assertThat(session.credential()).isNull();
        assertThat(session.principal()).isNull();
        assertThat(session.clusterName()).isNull();
        assertThat(session.sessionIdleExpiresAt()).isNull();
        assertThat(session.sessionAbsoluteExpiresAt()).isNull();
        assertThat(session.seeds()).isEmpty();
        assertThat(session.members()).isEmpty();
    }

    @Test
    void connectRecordsTheSeedsAndLandingNodeAndFlipsConnected() {
        Session session = new Session();
        List<URI> seeds = List.of(NODE1, NODE2);
        session.connect(seeds, NODE2);
        assertThat(session.isConnected()).isTrue();
        assertThat(session.landingNode()).isEqualTo(NODE2);
        assertThat(session.seeds()).containsExactlyElementsOf(seeds);
    }

    @Test
    void connectSeedsTheMemberSetWithTheSeedsAndStaysUnauthenticated() {
        // before any discovery the known members are exactly the seeds; connecting never authenticates
        Session session = new Session();
        List<URI> seeds = List.of(NODE1, NODE2);
        session.connect(seeds, NODE1);
        assertThat(session.members()).containsExactlyElementsOf(seeds);
        assertThat(session.isAuthenticated()).isFalse();
    }

    @Test
    void authenticateRecordsTheCredentialAndPrincipalAndFlipsAuthenticated() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("jwt-abc", "alice", null, List.of(NODE1));
        assertThat(session.isAuthenticated()).isTrue();
        assertThat(session.credential()).isEqualTo("jwt-abc");
        assertThat(session.principal()).isEqualTo("alice");
        assertThat(session.clusterName()).isNull();
        assertThat(session.members()).containsExactly(NODE1);
    }

    @Test
    void authenticateStoresExpiryMetadataAndLogoutClearsIt() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        Instant idle = Instant.parse("2026-09-30T10:00:00Z");
        Instant absolute = Instant.parse("2026-11-30T10:00:00Z");

        session.authenticate("jwt", "alice", null, List.of(NODE1), idle, absolute);

        assertThat(session.sessionIdleExpiresAt()).isEqualTo(idle);
        assertThat(session.sessionAbsoluteExpiresAt()).isEqualTo(absolute);
        session.logout();
        assertThat(session.sessionIdleExpiresAt()).isNull();
        assertThat(session.sessionAbsoluteExpiresAt()).isNull();
    }

    @Test
    void reconnectAndMachineAuthenticationDoNotRetainHumanExpiryMetadata() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("jwt", "alice", null, List.of(NODE1),
                Instant.parse("2026-09-30T10:00:00Z"), Instant.parse("2026-11-30T10:00:00Z"));

        session.connect(List.of(NODE2), NODE2);
        assertThat(session.sessionIdleExpiresAt()).isNull();
        assertThat(session.sessionAbsoluteExpiresAt()).isNull();

        session.authenticateMachine("machine-token", List.of(NODE2));
        assertThat(session.sessionIdleExpiresAt()).isNull();
        assertThat(session.sessionAbsoluteExpiresAt()).isNull();
    }

    @Test
    void authenticateStoresAClusterNameAndDiscoveredMembersWhenGiven() {
        Session session = new Session();
        session.connect(List.of(URI.create("http://seed:7900")), URI.create("http://seed:7900"));
        List<URI> discovered = List.of(NODE1, NODE2);
        session.authenticate("tok", "svc", "prod", discovered);
        assertThat(session.clusterName()).isEqualTo("prod");
        assertThat(session.members()).containsExactlyElementsOf(discovered);
    }

    @Test
    void logoutClearsAuthenticationButKeepsTheConnection() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("jwt", "alice", null, List.of(NODE1));
        session.logout();
        assertThat(session.isAuthenticated()).isFalse();
        assertThat(session.credential()).isNull();
        assertThat(session.principal()).isNull();
        assertThat(session.clusterName()).isNull();
        // the transport connection survives a logout; only the credential is dropped
        assertThat(session.isConnected()).isTrue();
        assertThat(session.landingNode()).isEqualTo(NODE1);
        assertThat(session.members()).containsExactly(NODE1);
    }

    @Test
    void disconnectClearsAuthenticationAndConnection() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("jwt", "alice", null, List.of(NODE1));
        session.disconnect();
        assertThat(session.isConnected()).isFalse();
        assertThat(session.isAuthenticated()).isFalse();
        assertThat(session.landingNode()).isNull();
        assertThat(session.credential()).isNull();
        assertThat(session.principal()).isNull();
        assertThat(session.seeds()).isEmpty();
        assertThat(session.members()).isEmpty();
    }

    @Test
    void reconnectingDropsAStaleAuthentication() {
        // a fresh connect is a new transport target; a credential issued by the old node must not carry over
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("jwt-from-node1", "alice", "prod", List.of(NODE1));
        session.connect(List.of(NODE2), NODE2);
        assertThat(session.isConnected()).isTrue();
        assertThat(session.landingNode()).isEqualTo(NODE2);
        assertThat(session.isAuthenticated()).isFalse();
        assertThat(session.credential()).isNull();
        assertThat(session.principal()).isNull();
        assertThat(session.clusterName()).isNull();
    }

    @Test
    void logoutResetsMembersToTheSeedsNotTheDiscoveredSet() {
        // discovered members are session-scoped; after logout the failover set is the seeds again
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        session.authenticate("t", "u", "c", List.of(NODE1, NODE2));   // discovered set wider than the seeds
        session.logout();
        assertThat(session.members()).containsExactly(NODE1);
    }

    @Test
    void relandMovesTheLandingNodeKeepingAuthAndMembers() {
        // failover re-lands on another member without dropping the (cluster-wide) credential
        Session session = new Session();
        List<URI> seeds = List.of(NODE1, NODE2);
        session.connect(seeds, NODE1);
        session.authenticate("jwt", "alice", null, seeds);
        session.reland(NODE2);
        assertThat(session.landingNode()).isEqualTo(NODE2);
        assertThat(session.isAuthenticated()).isTrue();
        assertThat(session.credential()).isEqualTo("jwt");
        assertThat(session.members()).containsExactlyElementsOf(seeds);
    }

    @Test
    void theSeedListIsCopiedDefensivelyOnConnect() {
        Session session = new Session();
        List<URI> seeds = new ArrayList<>(List.of(NODE1));
        session.connect(seeds, NODE1);
        seeds.add(URI.create("http://intruder:9999"));
        assertThat(session.seeds()).containsExactly(NODE1);
    }

    @Test
    void theSeedsAccessorIsUnmodifiable() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        assertThatThrownBy(() -> session.seeds().add(URI.create("http://x:1")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void theMemberListIsCopiedDefensivelyAndUnmodifiable() {
        Session session = new Session();
        session.connect(List.of(NODE1), NODE1);
        List<URI> discovered = new ArrayList<>(List.of(NODE1));
        session.authenticate("t", "u", null, discovered);
        discovered.add(URI.create("http://intruder:9999"));
        assertThat(session.members()).containsExactly(NODE1);
        assertThatThrownBy(() -> session.members().add(URI.create("http://x:1")))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
