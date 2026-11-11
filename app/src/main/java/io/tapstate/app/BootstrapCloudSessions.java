package io.tapstate.app;

import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** One designated setup runtime's temporary sessions, handed to its original CACHE at first publication. */
final class BootstrapCloudSessions implements CloudSessionStore, AutoCloseable {
    private final CloudSessionIdentity identity;
    private final Runnable requireSetup;
    private final Map<String,CloudSessionRecord> sessions = new HashMap<>();
    private final Set<String> revoked = new HashSet<>();
    private boolean closed;

    BootstrapCloudSessions(CloudSessionIdentity identity, Runnable requireSetup) {
        this.identity = Objects.requireNonNull(identity); this.requireSetup = Objects.requireNonNull(requireSetup);
    }
    private void active() {
        if (closed) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
        requireSetup.run();
    }
    @Override public synchronized boolean create(CloudSessionRecord record) {
        active();
        if (!identity.equals(record.identity()) || record.revoked() || revoked.contains(record.jwtId())
                || sessions.containsKey(record.jwtId())) return false;
        sessions.put(record.jwtId(), record); return true;
    }
    @Override public synchronized Optional<CloudSessionRecord> find(CloudSessionIdentity expected, String jwt) {
        active(); return identity.equals(expected) ? Optional.ofNullable(sessions.get(jwt)) : Optional.empty();
    }
    @Override public synchronized Optional<CloudSessionRecord> authenticate(CloudSessionIdentity expected, String jwt,
            String secretHash, Instant now, Instant idleExpiresAt) {
        active(); var original = sessions.get(jwt);
        if (!identity.equals(expected) || original == null || original.revoked() || revoked.contains(jwt)
                || !sameSecret(original.secretHash(), secretHash) || !original.idleExpiresAt().isAfter(now)) return Optional.empty();
        var touched = new CloudSessionRecord(original.identity(), original.jwtId(), original.secretHash(), original.userId(),
                original.scope(), false, original.createdAt(), now.isAfter(original.lastUsedAt()) ? now : original.lastUsedAt(),
                idleExpiresAt.isAfter(original.idleExpiresAt()) ? idleExpiresAt : original.idleExpiresAt(), original.clusterContext());
        sessions.put(jwt, touched); return Optional.of(touched);
    }
    @Override public synchronized boolean logout(CloudSessionIdentity expected, String jwt, String secretHash, Instant now) {
        active(); var original = sessions.get(jwt);
        if (!identity.equals(expected) || original == null || !sameSecret(original.secretHash(), secretHash)) return false;
        revoked.add(jwt); return true;
    }
    @Override public synchronized void invalidate(CloudSessionIdentity expected, String jwt, Instant now) {
        active(); if (identity.equals(expected)) revoked.add(jwt);
    }
    synchronized void handoffTo(CloudSessionStore destination, Instant now) {
        if (closed) return;
        for (String jwt : revoked) destination.invalidate(identity, jwt, now);
        for (CloudSessionRecord record : sessions.values()) {
            if (!revoked.contains(record.jwtId()) && record.idleExpiresAt().isAfter(now)) destination.create(record);
        }
        close();
    }
    @Override public synchronized void close() { closed = true; sessions.clear(); revoked.clear(); }
    private static boolean sameSecret(String expected, String presented) {
        return presented != null && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }
}
