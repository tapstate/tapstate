package io.tapstate.cli;

import java.net.URI;
import java.util.List;

/**
 * The REPL's mutable connection state, carried across read-loop iterations. A session moves through
 * three stages: offline, connected (a reachable transport target, no credential yet), and
 * authenticated. Connecting records the ordered seed list that was tried and the landing node that
 * answered the reachability probe, and seeds the member set with those seeds. Authenticating records
 * an opaque credential — a signed session token today, a machine token later; the shape is
 * mechanism-agnostic on purpose — plus the principal to show and any members discovered from the
 * server, so a later request that loses its landing node can fail over across them.
 *
 * <p>Connecting is decoupled from authenticating: a connected session may still be unauthenticated, and
 * {@code logout} drops the credential while keeping the transport connection.
 */
final class Session {

    enum CredentialKind {
        HUMAN_ACCESS,
        MACHINE
    }

    private List<URI> seeds = List.of();
    private URI landingNode;
    private boolean connected;

    /** The bearer credential presented on authenticated requests (opaque; {@code null} while unauthenticated). */
    private String credential;

    /** The provenance of the bearer credential, or {@code null} while unauthenticated. */
    private CredentialKind credentialKind;

    /** The principal to display in the prompt (a username; {@code null} while unauthenticated). */
    private String principal;

    /** The cluster name once known; {@code null} until membership discovery yields one (none in L1). */
    private String clusterName;

    /** The member base URLs to fail over across; the seeds until discovery refines them. */
    private List<URI> members = List.of();

    /** Whether the session currently targets a reachable server. */
    boolean isConnected() {
        return connected;
    }

    /** Whether the session currently carries a credential. */
    boolean isAuthenticated() {
        return credential != null;
    }

    /** Whether the process currently carries a machine bearer rather than a human access token. */
    boolean hasMachineCredential() {
        return credentialKind == CredentialKind.MACHINE;
    }

    /** The base URL that answered the probe, or {@code null} while offline. */
    URI landingNode() {
        return landingNode;
    }

    /** The ordered seed list used to connect (unmodifiable); empty while offline. */
    List<URI> seeds() {
        return seeds;
    }

    /** The member base URLs to fail over across (unmodifiable); empty while offline. */
    List<URI> members() {
        return members;
    }

    /** The bearer credential, or {@code null} while unauthenticated. */
    String credential() {
        return credential;
    }

    /** The principal to display, or {@code null} while unauthenticated. */
    String principal() {
        return principal;
    }

    /** The cluster name, or {@code null} until known. */
    String clusterName() {
        return clusterName;
    }

    /** Records a successful connection: the seeds tried, the landing node, and the seeds as the member set. */
    void connect(List<URI> seeds, URI landingNode) {
        this.seeds = List.copyOf(seeds);
        this.landingNode = landingNode;
        this.connected = true;
        this.members = this.seeds;   // members = seeds until discovery refines them
        // a fresh connect is a new transport target: never carry a credential the previous node issued
        this.credential = null;
        this.credentialKind = null;
        this.principal = null;
        this.clusterName = null;
    }

    /** Records an authenticated session: the credential, the principal, an optional cluster name, and members. */
    void authenticate(String credential, String principal, String clusterName, List<URI> members) {
        this.credential = credential;
        this.credentialKind = CredentialKind.HUMAN_ACCESS;
        this.principal = principal;
        this.clusterName = clusterName;
        this.members = List.copyOf(members);
    }

    /** Records a verified machine bearer without associating it with a persisted human principal. */
    void authenticateMachine(String credential, List<URI> members) {
        this.credential = credential;
        this.credentialKind = CredentialKind.MACHINE;
        this.principal = "machine";
        this.clusterName = null;
        this.members = List.copyOf(members);
    }

    /** Moves the landing node to another member (failover), keeping the credential and member set. */
    void reland(URI landingNode) {
        this.landingNode = landingNode;
    }

    /** Drops the credential while keeping the transport connection; members fall back to the seeds. */
    void logout() {
        this.credential = null;
        this.credentialKind = null;
        this.principal = null;
        this.clusterName = null;
        this.members = this.seeds;
    }

    /** Clears the session back to offline, dropping any credential. */
    void disconnect() {
        this.seeds = List.of();
        this.landingNode = null;
        this.connected = false;
        this.credential = null;
        this.credentialKind = null;
        this.principal = null;
        this.clusterName = null;
        this.members = List.of();
    }
}
