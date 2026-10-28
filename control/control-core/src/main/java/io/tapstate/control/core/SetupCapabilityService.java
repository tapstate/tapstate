package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/** One opaque, purpose-specific installation credential; issuance is never an HTTP operation. */
public final class SetupCapabilityService {
    public static final Duration LIFETIME = Duration.ofMinutes(15);
    private final StorageSetupStateStore states;
    private final Clock clock;
    private final Runnable requireDeploymentOwner;
    private final Runnable requireManagementChannel;
    private final Runnable requireFirstAdminReadiness;
    private final SecureRandom random = new SecureRandom();
    private volatile Current current;
    private volatile long localRevision;

    public record Issued(StorageSetupScope scope, StorageSetupStateStore.Purpose purpose, Instant expiresAt, String token) {
        public Issued {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(purpose, "purpose");
            Objects.requireNonNull(expiresAt, "expiresAt"); Objects.requireNonNull(token, "token");
        }
        @Override public String toString() { return "SetupCapability[redacted]"; }
    }
    private record Current(Issued issued, byte[] digest) {}

    /** Created only after the installation authority and real management channel were verified. */
    public static final class Permission implements Runnable {
        private final StorageSetupScope scope;
        private final StorageSetupStateStore.Purpose purpose;
        private final Runnable active;
        private Permission(StorageSetupScope scope, StorageSetupStateStore.Purpose purpose, Runnable active) {
            this.scope = scope; this.purpose = purpose; this.active = active;
        }
        public StorageSetupScope scope() { return scope; }
        public StorageSetupStateStore.Purpose purpose() { return purpose; }
        @Override public void run() { active.run(); }
        @Override public String toString() { return "SetupPermission[redacted]"; }
    }

    /** Channel checks must use the server listener identity, never forwarded headers or client booleans. */
    public SetupCapabilityService(StorageSetupStateStore states, Clock clock, Runnable requireDeploymentOwner,
            Runnable requireManagementChannel, Runnable requireFirstAdminReadiness) {
        this.states = Objects.requireNonNull(states, "states"); this.clock = Objects.requireNonNull(clock, "clock");
        this.requireDeploymentOwner = Objects.requireNonNull(requireDeploymentOwner, "requireDeploymentOwner");
        this.requireManagementChannel = Objects.requireNonNull(requireManagementChannel, "requireManagementChannel");
        this.requireFirstAdminReadiness = Objects.requireNonNull(requireFirstAdminReadiness, "requireFirstAdminReadiness");
    }

    /** The owner-only file writer receives this value after the authoritative generation has advanced. */
    public synchronized Issued issue(StorageSetupScope expected, StorageSetupStateStore.Purpose purpose) {
        requireDeploymentOwner.run();
        if (purpose == StorageSetupStateStore.Purpose.FIRST_ADMIN) requireFirstAdminReadiness.run();
        var rotated = states.rotate(expected, purpose);
        byte[] bytes = new byte[32]; random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        java.util.Arrays.fill(bytes, (byte) 0);
        Issued issued = new Issued(rotated.scope(), purpose, clock.instant().plus(LIFETIME), token);
        current = new Current(issued, digest(token));
        return issued;
    }

    /** Restore only an owner-protected file for the same current authoritative phase and generation. */
    public synchronized void restore(Issued issued) {
        requireDeploymentOwner.run();
        Objects.requireNonNull(issued, "issued");
        if (issued.token().length() != 43 || !issued.token().matches("[A-Za-z0-9_-]{43}")
                || !issued.expiresAt().isAfter(clock.instant())
                || issued.expiresAt().isAfter(clock.instant().plus(LIFETIME))) throw refused();
        if (issued.purpose() == StorageSetupStateStore.Purpose.FIRST_ADMIN) requireFirstAdminReadiness.run();
        states.requireCurrent(issued.scope(), issued.purpose());
        current = new Current(issued, digest(issued.token()));
    }

    /** Captures the original authorization; repeat this guard inside every final store transaction. */
    public Permission authorize(String token, StorageSetupScope expected, StorageSetupStateStore.Purpose purpose) {
        requireManagementChannel.run();
        Current captured = current;
        if (captured == null || token == null || token.length() != 43
                || !MessageDigest.isEqual(captured.digest(), digest(token))
                || !captured.issued().scope().equals(expected) || captured.issued().purpose() != purpose) throw refused();
        Runnable active = () -> {
            requireManagementChannel.run();
            if (current != captured || !captured.issued().expiresAt().isAfter(clock.instant())) throw refused();
            if (purpose == StorageSetupStateStore.Purpose.FIRST_ADMIN) requireFirstAdminReadiness.run();
            states.requireCurrent(expected, purpose);
            if (current != captured || !captured.issued().expiresAt().isAfter(clock.instant())) throw refused();
        };
        active.run();
        return new Permission(expected, purpose, active);
    }

    /** Local installation mode uses the deployment process and dedicated listener instead of a copied secret. */
    public Permission authorizeLocal(StorageSetupScope expected, StorageSetupStateStore.Purpose purpose) {
        requireDeploymentOwner.run(); requireManagementChannel.run();
        long revision = localRevision;
        Runnable active = () -> {
            requireDeploymentOwner.run(); requireManagementChannel.run();
            if (localRevision != revision) throw refused();
            states.requireCurrent(expected, purpose);
            if (purpose == StorageSetupStateStore.Purpose.FIRST_ADMIN) requireFirstAdminReadiness.run();
            states.requireCurrent(expected, purpose);
            if (localRevision != revision) throw refused();
        };
        active.run(); return new Permission(expected, purpose, active);
    }

    /** Phase completion is durable; clearing local material never reopens its authority. */
    public synchronized void close() { current = null; localRevision++; }

    private static byte[] digest(String token) {
        try { return MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.US_ASCII)); }
        catch (NoSuchAlgorithmException missing) { throw new IllegalStateException(missing); }
    }
    private static TapstateException refused() {
        return new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
    }
}
