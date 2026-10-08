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
        } finally {
            ports.forEach(RealProcessServer::releasePort);
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
