package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RealProcessServerPortsTest {

    @Test
    void anUnlaunchedMembersPortCannotBeTakenByAnotherListener() throws IOException {
        int port = RealProcessServer.reservePort();
        try {
            try (ServerSocket competing = new ServerSocket()) {
                competing.setReuseAddress(false);
                assertThatThrownBy(() -> competing.bind(
                        new InetSocketAddress(InetAddress.getLoopbackAddress(), port)))
                        .isInstanceOf(BindException.class);
            }
        } finally {
            RealProcessServer.releasePort(port);
        }
        try (ServerSocket takingOver = new ServerSocket()) {
            takingOver.setReuseAddress(false);
            takingOver.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            assertThat(takingOver.getLocalPort()).isEqualTo(port);
        }
    }

    @Test
    void theMembersAndLinksOfAClusterReceiveDistinctPorts() {
        List<Integer> ports = new ArrayList<>();
        try {
            for (int index = 0; index < 12; index++) {
                ports.add(RealProcessServer.reservePort());
            }
            assertThat(ports).doesNotHaveDuplicates();
            assertThat(ports).allSatisfy(port -> assertThat(port).isBetween(10_000, 19_999));
        } finally {
            ports.forEach(RealProcessServer::releasePort);
        }
    }

    @Test
    void anOccupiedPortIsSkippedWithoutClosingTheExistingListener() throws Exception {
        var cursor = RealProcessServer.class.getDeclaredField("nextFixturePort");
        cursor.setAccessible(true);
        int occupied = cursor.getInt(null);
        try (var listener = new ServerSocket()) {
            listener.setReuseAddress(false);
            listener.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), occupied));
            int reserved = RealProcessServer.reservePort();
            try {
                assertThat(reserved).isNotEqualTo(occupied);
                assertThat(listener.isBound()).isTrue();
                assertThat(listener.isClosed()).isFalse();
            } finally { RealProcessServer.releasePort(reserved); }
        }
    }

    @Test
    void releasedPortsRemainDisjointUntilTheirAllocatorProcessesEnd() throws Exception {
        Process first = allocatorProcess();
        Process second = null;
        try {
            String firstPorts = new java.io.BufferedReader(new java.io.InputStreamReader(first.getInputStream())).readLine();
            second = allocatorProcess();
            String secondPorts = new java.io.BufferedReader(new java.io.InputStreamReader(second.getInputStream())).readLine();
            assertThat(firstPorts).isNotBlank();
            assertThat(secondPorts).isNotBlank();
            assertThat(List.of(firstPorts.split(","))).doesNotContainAnyElementsOf(List.of(secondPorts.split(",")));
        } finally {
            try { stopAllocator(first); }
            finally { if (second != null) stopAllocator(second); }
        }
    }

    private static Process allocatorProcess() throws IOException {
        return new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                PortAllocator.class.getName()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
    }

    private static void stopAllocator(Process process) throws Exception {
        process.getOutputStream().close();
        if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        assertThat(process.exitValue()).isZero();
    }

    public static final class PortAllocator {
        public static void main(String[] args) throws Exception {
            var cursor = RealProcessServer.class.getDeclaredField("nextFixturePort");
            cursor.setAccessible(true);
            cursor.setInt(null, 10_000);
            List<String> ports = new ArrayList<>();
            for (int index = 0; index < 12; index++) {
                int port = RealProcessServer.reservePort();
                RealProcessServer.releasePort(port);
                ports.add(Integer.toString(port));
            }
            System.out.println(String.join(",", ports));
            System.out.flush();
            System.in.read();
        }
    }

    @Test
    void aCuttableLinkTakesOverItsReservation() throws IOException {
        int port = RealProcessServer.reservePort();
        try (CuttableLink link = CuttableLink.open(port, "127.0.0.1", port,
                Map.of("node-a", new int[] {20000, 20119}));
             ServerSocket competing = new ServerSocket()) {
            competing.setReuseAddress(false);
            assertThat(link.address()).isEqualTo("127.0.0.1:" + port);
            assertThatThrownBy(() -> competing.bind(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), port)))
                    .isInstanceOf(BindException.class);
        } finally {
            RealProcessServer.releasePort(port);
        }
        try (ServerSocket takingOver = new ServerSocket()) {
            takingOver.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            assertThat(takingOver.getLocalPort()).isEqualTo(port);
        }
    }
}
