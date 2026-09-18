package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Settings for the embedded Hazelcast member ({@code tapstate.hz.*}). Follows the configuration
 * layering: packaged defaults, overridable from the external conf file or the environment.
 */
@ConfigurationProperties(prefix = "tapstate.hz")
class HazelcastProperties {

    private String clusterName = "tapstate";
    private int memberPort = 5701;
    private String bindAddress = "127.0.0.1";
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
