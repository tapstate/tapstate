package io.tapstate.e2e;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * This machine's address as another machine would reach it.
 *
 * <p>A member with discovery on is refused when it binds the loopback, because two members reaching
 * each other over 127.0.0.1 would be a cluster no third machine could ever join. So every fixture that
 * brings up a cluster of real processes needs the same answer, and asks for it here rather than each
 * carrying its own walk over the interfaces.
 */
final class RoutableAddress {

    private RoutableAddress() {
    }

    /** The first routable IPv4 address of an interface that is up. */
    static String ofThisMachine() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface candidate = interfaces.nextElement();
                if (!candidate.isUp() || candidate.isLoopback()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = candidate.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress()
                            && !address.isLinkLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (SocketException unreachable) {
            throw new AssertionError("could not read this machine's interfaces", unreachable);
        }
        throw new AssertionError("this machine has no address but the loopback, and a member with "
                + "discovery on is refused there - two members cannot be brought up here");
    }
}
