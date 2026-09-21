package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

/**
 * Settings for the embedded Hazelcast member ({@code tapstate.hz.*}). Follows the configuration
 * layering: packaged defaults, overridable from the external conf file or the environment.
 */
@ConfigurationProperties(prefix = "tapstate.hz")
class HazelcastProperties {

    private String clusterName = "tapstate";
    private int memberPort = 5701;
    private String bindAddress = "127.0.0.1";
    private String advertisedMemberAddress;
    private List<String> outboundMemberPorts = new ArrayList<>();
    private Duration heartbeatInterval = Duration.ofSeconds(5);
    private Duration maximumNoHeartbeat = Duration.ofSeconds(30);
    private final Discovery discovery = new Discovery();
    private final Jet jet = new Jet();

    String getClusterName() {
        return clusterName;
    }

    void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    int getMemberPort() {
        return memberPort;
    }

    void setMemberPort(int memberPort) {
        this.memberPort = memberPort;
    }

    String getBindAddress() {
        return bindAddress;
    }

    void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    /**
     * The address this member tells the others to reach it at, when that is not the address it binds.
     *
     * <p>Members exchange the address each one reports for itself, and every later connection dials
     * that -- so a member behind a port mapping or a translated address is unreachable unless it can
     * report the outside of that mapping rather than the inside. Absent, a member reports what it
     * binds, which is what every deployment without one in front of it wants.
     *
     * <p>{@code host} or {@code host:port}; the port may differ from the bound one, which is the whole
     * of a port mapping. It never changes what the member binds, and therefore never changes who can
     * reach it.
     */
    String getAdvertisedMemberAddress() {
        return advertisedMemberAddress;
    }

    void setAdvertisedMemberAddress(String advertisedMemberAddress) {
        this.advertisedMemberAddress = advertisedMemberAddress;
    }

    /**
     * The local ports this member is allowed to dial the other members from; empty means any.
     *
     * <p>Outgoing member connections take an arbitrary ephemeral port by default, which is exactly
     * what an egress firewall cannot be written against: the rule would have to allow the whole
     * ephemeral range in both directions. Naming a range here makes member traffic leave from ports
     * an operator can enumerate, so the rule can be as narrow as the member port itself.
     *
     * <p>Each entry is a single port or an inclusive {@code low-high} range. Give the range at least
     * as many ports as this member holds connections, since they are used in turn and a port still
     * in its close-wait is skipped; the size is not enforced here because only the deployment knows
     * how many members it runs. This never changes what the member binds or reports -- it changes
     * only which local port a dial leaves from.
     */
    List<String> getOutboundMemberPorts() {
        return List.copyOf(outboundMemberPorts);
    }

    void setOutboundMemberPorts(List<String> outboundMemberPorts) {
        this.outboundMemberPorts =
                outboundMemberPorts == null ? new ArrayList<>() : new ArrayList<>(outboundMemberPorts);
    }

    Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    void setHeartbeatInterval(Duration heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    Duration getMaximumNoHeartbeat() {
        return maximumNoHeartbeat;
    }

    void setMaximumNoHeartbeat(Duration maximumNoHeartbeat) {
        this.maximumNoHeartbeat = maximumNoHeartbeat;
    }

    Discovery getDiscovery() {
        return discovery;
    }

    Jet getJet() {
        return jet;
    }

    enum DiscoveryMode {
        NONE,
        TCP_IP,
        KUBERNETES
    }

    /** The one explicit join strategy selected for this member. */
    static class Discovery {

        private DiscoveryMode mode = DiscoveryMode.NONE;
        private final TcpIp tcpIp = new TcpIp();
        private final Kubernetes kubernetes = new Kubernetes();

        DiscoveryMode getMode() {
            return mode;
        }

        void setMode(DiscoveryMode mode) {
            this.mode = mode;
        }

        TcpIp getTcpIp() {
            return tcpIp;
        }

        Kubernetes getKubernetes() {
            return kubernetes;
        }
    }

    /** Static seed addresses for deterministic VM or bare-metal discovery. */
    static class TcpIp {

        private List<String> seeds = new ArrayList<>();

        List<String> getSeeds() {
            return List.copyOf(seeds);
        }

        void setSeeds(List<String> seeds) {
            this.seeds = seeds == null ? new ArrayList<>() : new ArrayList<>(seeds);
        }
    }

    /** Kubernetes discovery inputs; headless-service DNS is preferred over API discovery. */
    static class Kubernetes {

        private String serviceDns;
        private String serviceName;
        private String namespace;

        String getServiceDns() {
            return serviceDns;
        }

        void setServiceDns(String serviceDns) {
            this.serviceDns = serviceDns;
        }

        String getServiceName() {
            return serviceName;
        }

        void setServiceName(String serviceName) {
            this.serviceName = serviceName;
        }

        String getNamespace() {
            return namespace;
        }

        void setNamespace(String namespace) {
            this.namespace = namespace;
        }
    }

    /** Jet engine knobs. */
    static class Jet {

        /** Cooperative worker thread count; {@code null} keeps the engine default (CPU cores). */
        private Integer cooperativeThreadCount;

        Integer getCooperativeThreadCount() {
            return cooperativeThreadCount;
        }

        void setCooperativeThreadCount(Integer cooperativeThreadCount) {
            this.cooperativeThreadCount = cooperativeThreadCount;
        }
    }
}
