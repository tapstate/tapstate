package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An in-memory {@link ConnectorRegistry} standing in for the Mongo/GridFS distribution store: it
 * hashes bytes content-addressably, registers-if-absent, and counts {@link #artifact} fetches so a
 * test can prove the on-disk cache is reused rather than re-fetched.
 */
final class InMemoryConnectorRegistry implements ConnectorRegistry {

    private final List<ConnectorRegistration> registrations = new ArrayList<>();
    private final Map<String, byte[]> bytesByHash = new LinkedHashMap<>();
    int artifactCalls;
    /** Certifications that one instance may serve several writers, by content hash. */
    final Map<String, String> shareSafe = new java.util.HashMap<>();

    @Override
    public RegistrationOutcome register(
            String connectorId, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
        String hash = ContentHash.of(artifact);
        for (ConnectorRegistration existing : registrations) {
            if (existing.contentHash().equals(hash)) {
                return new RegistrationOutcome(existing, false);
            }
        }
        ConnectorRegistration registration = new ConnectorRegistration(connectorId, hash, pdkApiVersion, source);
        registrations.add(registration);
        bytesByHash.put(hash, artifact.clone());
        return new RegistrationOutcome(registration, true);
    }

    @Override
    public List<ConnectorRegistration> list() {
        return List.copyOf(registrations);
    }

    @Override
    public Optional<byte[]> artifact(String contentHash) {
        artifactCalls++;
        byte[] bytes = bytesByHash.get(contentHash);
        return bytes == null ? Optional.empty() : Optional.of(bytes.clone());
    }

    @Override
    public Optional<String> shareSafeCertification(String connectorId, String contentHash) {
        return Optional.ofNullable(shareSafe.get(contentHash));
    }

    @Override
    public boolean hasArtifact(String contentHash) {
        return artifact(contentHash).isPresent();
    }

    /** A registration whose bytes were never stored — the store inconsistency the provisioner refuses. */
    void addDanglingRegistration(String connectorId, String contentHash, String pdkApiVersion) {
        registrations.add(new ConnectorRegistration(
                connectorId, contentHash, pdkApiVersion, RegistrationSource.REGISTER));
    }
}
