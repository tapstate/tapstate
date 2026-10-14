package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.LinkOption;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapstate.e2e.ClusterLifecycleProbe.Phase.*;
import static io.tapstate.e2e.ClusterProcessControlFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A scoped fault ends an actual owned initial JVM; real peer exit, ports and saved logs are inspected. */
class InitialClusterFailureCleanupIT {
    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aSecondInitialMemberKilledBeforeHealthLeavesNoOwnedPeersOrLinksBehind(@TempDir Path scratch)
            throws Exception {
        List<String> nodes = List.of("node-a", "node-b", "node-c");
        List<ClusterLifecycleProbe.Event> events = new ArrayList<>();
        Map<String, ProcessHandle> handles = new LinkedHashMap<>();
        AtomicBoolean injected = new AtomicBoolean();
        Set<Integer> priorReservations = reservedPorts();
        Set<Integer> allocatedReservations = new LinkedHashSet<>();
        Set<Integer> allocatedLinks = new LinkedHashSet<>();
        String store = SharedMongo.replicaSetUrl(
                "e2e_initial_failure_" + UUID.randomUUID().toString().replace("-", ""));
        Set<String> priorLogDirectories = memberLogDirectories().stream()
                .map(path -> path.getFileName().toString()).collect(java.util.stream.Collectors.toSet());
        // Created before installing the fault scope, and closed by this test after inspecting it.
        try (AutoCloseable ownedCleanup = () -> retireCaptured(handles.values());
             RealProcessServer foreign = child(scratch, "outside-batch", "quiet", null, null, null)) {
            try (ClusterLifecycleProbe.Scope ignored = ClusterLifecycleProbe.install(event -> {
                events.add(event);
                if (event.phase() != LAUNCHED) return;
                ProcessHandle owned = ProcessHandle.of(event.pid())
                        .orElseThrow(() -> new AssertionError("the owned initial launch is not observable"));
                handles.put(event.launch(), owned);
                Set<Integer> currentReservations = reservedPorts();
                currentReservations.removeAll(priorReservations);
                allocatedReservations.addAll(currentReservations);
                String prefix = "--tapstate.hz.discovery.tcp-ip.seeds=";
                for (String argument : owned.info().arguments().orElseThrow(
                        () -> new AssertionError("the owned child arguments are unavailable"))) {
                    if (!argument.startsWith(prefix)) continue;
                    for (String seed : argument.substring(prefix.length()).split(",")) {
                        allocatedLinks.add(Integer.parseInt(seed.substring(seed.lastIndexOf(':') + 1)));
                    }
                }
                if (event.node().equals("node-b")) {
                    if (!injected.compareAndSet(false, true)) {
                        throw new AssertionError("the scoped second-member fault was injected twice");
                    }
                    if (!owned.destroyForcibly()) {
                        throw new AssertionError("the second owned initial member could not be ended");
                    }
                }
            })) {
                assertThatThrownBy(() -> PartitionableCluster.start(store, "initial-start-failure", nodes))
                        .isInstanceOf(AssertionError.class)
                        .hasMessageContaining("exited with status")
                        .hasMessageContaining("before answering")
                        .satisfies(failure -> assertThat(failure.getSuppressed())
                                .as("normal cleanup introduced no hidden secondary failure").isEmpty());
            }
            assertThat(injected.get()).isTrue();
            List<ClusterLifecycleProbe.Event> prepared = events.stream()
                    .filter(event -> event.phase() == PREPARED).toList();
            List<ClusterLifecycleProbe.Event> launched = events.stream()
                    .filter(event -> event.phase() == LAUNCHED).toList();
            assertThat(prepared).hasSize(2);
            assertThat(launched).hasSize(2);
            assertThat(prepared.stream().map(ClusterLifecycleProbe.Event::node).toList())
                    .containsExactlyElementsOf(nodes.subList(0, 2));
            assertThat(launched.stream().map(ClusterLifecycleProbe.Event::launch).toList())
                    .containsExactlyElementsOf(prepared.stream().map(ClusterLifecycleProbe.Event::launch).toList());
            assertThat(events.stream().filter(event -> event.phase() == HEALTH_PASSED)
                    .map(ClusterLifecycleProbe.Event::node).toList()).containsExactly("node-a");
            String clusterHash = launched.getFirst().cluster();
            assertThat(launched).allSatisfy(event -> assertThat(event.cluster()).isEqualTo(clusterHash));
            List<Path> ownLogDirectories = memberLogDirectories().stream()
                    .filter(path -> !priorLogDirectories.contains(path.getFileName().toString()))
                    .filter(path -> ClusterLifecycleProbe.hash(path.getFileName().toString()).equals(clusterHash))
                    .toList();
            assertThat(ownLogDirectories).as("only this bring-up's new hashed-identity log directory").hasSize(1);
            for (ClusterLifecycleProbe.Event event : launched) {
                // The OS handle was captured before fault injection; this is independent exit evidence.
                assertThat(handles.get(event.launch()).isAlive()).as("owned initial member %s", event.node()).isFalse();
                assertThat(events.stream().filter(item -> item.phase() == EXIT_CONFIRMED
                        && item.launch().equals(event.launch())).toList()).hasSize(1);
                Path saved = ownLogDirectories.getFirst().resolve(event.node() + ".log");
                assertThat(saved).as("the actual initial member log %s", event.node()).isRegularFile();
                // A child killed before its first log line has a real empty output, not a fabricated account.
                Files.readAllLines(saved);
            }
            for (ClusterLifecycleProbe.Event event : prepared) {
                try (var http = releasedPort(event.httpPort());
                     var member = releasedPort(event.memberPort());
                     var link = releasedPort(event.ownedLinkPort())) {
                    assertThat(http.getLocalPort()).isEqualTo(event.httpPort());
                    assertThat(member.getLocalPort()).isEqualTo(event.memberPort());
                    assertThat(link.getLocalPort()).isEqualTo(event.ownedLinkPort());
                }
            }
            assertThat(allocatedLinks).hasSize(nodes.size());
            assertThat(allocatedReservations).isNotEmpty();
            Set<Integer> allObservedPorts = new LinkedHashSet<>(allocatedReservations);
            allObservedPorts.addAll(allocatedLinks);
            for (int port : allObservedPorts) {
                try (var released = releasedPort(port)) {
                    assertThat(released.getLocalPort()).isEqualTo(port);
                }
            }
            assertThat(reservedPorts()).containsExactlyInAnyOrderElementsOf(priorReservations);
            assertThat(foreign.isAlive()).isTrue();
            assertThat(foreign.stagingDirectory()).isDirectory();
        }
    }

    /** A faulty cleanup control must not leave this test's captured children behind. */
    private static void retireCaptured(java.util.Collection<ProcessHandle> owned) throws Exception {
        for (ProcessHandle process : owned) {
            if (process.isAlive()) process.destroyForcibly();
        }
        for (ProcessHandle process : owned) {
            process.onExit().get(20, TimeUnit.SECONDS);
            assertThat(process.isAlive()).as("the captured test child actually exited").isFalse();
        }
    }

    /** Captures live reservations; closed history prevents reuse but owns no OS port. */
    private static Set<Integer> reservedPorts() {
        synchronized (RealProcessServer.class) {
            try {
                var field = RealProcessServer.class.getDeclaredField("PORT_RESERVATIONS");
                field.setAccessible(true);
                Map<?, ?> reservations = (Map<?, ?>) field.get(null);
                Set<Integer> ports = new LinkedHashSet<>();
                for (var entry : reservations.entrySet()) {
                    if (!((java.net.Socket) entry.getValue()).isClosed()) ports.add((Integer) entry.getKey());
                }
                return ports;
            } catch (ReflectiveOperationException failure) {
                throw new AssertionError("the owned port reservations could not be observed", failure);
            }
        }
    }

    private static ServerSocket releasedPort(int port) throws Exception {
        ServerSocket probe = new ServerSocket();
        try {
            // TIME_WAIT is not a live listener. Reuse permits that normal terminal TCP state while
            // binding the wildcard still fails if an owned process or link listener retained the port.
            probe.setReuseAddress(true);
            probe.bind(new InetSocketAddress("0.0.0.0", port));
            return probe;
        } catch (Exception failure) {
            probe.close();
            throw failure;
        }
    }

    private static List<Path> memberLogDirectories() throws Exception {
        Path directory = Path.of("target", "failure-scenes", "members");
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) return List.of();
        try (var children = Files.list(directory)) {
            return children.filter(path -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)).toList();
        }
    }
}
