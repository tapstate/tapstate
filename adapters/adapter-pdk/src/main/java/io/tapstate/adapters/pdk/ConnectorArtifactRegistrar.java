package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.CatalogEntryAssembler;
import io.tapstate.core.catalog.ConnectorOverlay;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.NormalizedSpec;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.SpecNormalizer;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistrar;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Registers a connector artifact on disk into the distribution store by what the artifact itself
 * declares: self-scan supplies the entry class and PDK API version, the spec's {@code properties.id}
 * supplies the connector id, and the artifact bytes go through the store's content-hash idempotent
 * register-if-absent. The startup seed sweep and the explicit runtime register operation are both
 * this one call, differing only in the {@link RegistrationSource} they record.
 *
 * <p>An artifact whose spec cannot yield an id — not valid JSON, or no {@code properties.id} — is
 * refused with a coded connector-domain exception: registration would have no identity to file the
 * artifact under. A different artifact under an already-registered id is likewise refused — a single
 * active artifact is kept per id. An unreadable artifact is refused with a coded diagnostic without exposing its path or cause.
 *
 * <p>After the bytes are registered, the connector's normalized catalog row is derived (its declared
 * capabilities merged with its spec) and stored, so the online catalog view can list the connector and
 * validate against it. Derivation is skipped when the bytes were already registered and a row already
 * exists; a re-register whose row is missing backfills it, so a crash between the two never leaves a
 * registered connector without a row.
 */
public final class ConnectorArtifactRegistrar implements ConnectorRegistrar {

    /**
     * The officially supported ids, readable from outside this package.
     *
     * <p>Exists so that a gate standing outside every module can assert what a shipped deployment
     * accepts out of the box. That claim spans two things this package cannot see together — the set
     * itself, and the fact that nothing a release carries widens it — so the assertion cannot live
     * here. The set itself is not held here: it is {@link OfficialConnectors#IDS}, so the authoring
     * surfaces and this register path cannot drift apart.
     */
    public static List<String> officialConnectorIds() {
        return OfficialConnectors.IDS;
    }

    /**
     * The same overlay the checked-in snapshot was assembled against, so a connector registered at
     * runtime lands on the same catalog row the offline path would have produced for it.
     *
     * <p>Read once for the process rather than per registration: it is an immutable object rebuilt
     * from the same bundled resources every time, and registering a directory of artifacts would
     * otherwise re-read and re-parse the whole overlay once per connector.
     */
    private static final ConnectorOverlay OVERLAY = ConnectorOverlay.load();

    private final ConnectorRegistry registry;
    private final ConnectorIntrospector introspector;
    private final CapabilityDeriver capabilityDeriver;
    private final ConnectorCatalogStore catalogStore;
    private final ConnectorSpecStore specStore;
    private final List<String> acceptedConnectorIds;

    /** Registers with the release's own accepted set — what every shipped artifact uses. */
    public ConnectorArtifactRegistrar(ConnectorRegistry registry, ConnectorIntrospector introspector,
                                      CapabilityDeriver capabilityDeriver, ConnectorCatalogStore catalogStore,
                                      ConnectorSpecStore specStore) {
        this(registry, introspector, capabilityDeriver, catalogStore, specStore, List.of());
    }

    /**
     * Registers with further connector ids accepted beyond the official set. Empty in every shipped
     * artifact: this exists for a deployment that stands up its own server and supplies its own
     * connector — the product's end-to-end harness is the case it was built for. Naming ids here does
     * not make them supported; it only stops the register path refusing them.
     */
    public ConnectorArtifactRegistrar(ConnectorRegistry registry, ConnectorIntrospector introspector,
                                      CapabilityDeriver capabilityDeriver, ConnectorCatalogStore catalogStore,
                                      ConnectorSpecStore specStore, List<String> alsoAccept) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.introspector = Objects.requireNonNull(introspector, "introspector");
        this.capabilityDeriver = Objects.requireNonNull(capabilityDeriver, "capabilityDeriver");
        this.catalogStore = Objects.requireNonNull(catalogStore, "catalogStore");
        this.specStore = Objects.requireNonNull(specStore, "specStore");
        Objects.requireNonNull(alsoAccept, "alsoAccept");
        if (alsoAccept.isEmpty()) {
            // The shipped path accepts that list itself rather than a copy of it, so no edit can move
            // one without the other.
            this.acceptedConnectorIds = OfficialConnectors.IDS;
        } else {
            List<String> accepted = new ArrayList<>(OfficialConnectors.IDS);
            accepted.addAll(alsoAccept);
            this.acceptedConnectorIds = List.copyOf(accepted);
        }
    }

    /** The ids this registrar accepts: the official set, widened by whatever the deployment named. */
    List<String> acceptedConnectorIds() {
        return acceptedConnectorIds;
    }

    /** Registers the artifact at {@code artifact} if its content hash is not already registered. */
    public RegistrationOutcome register(Path artifact, RegistrationSource source) {
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(source, "source");
        IntrospectedConnector introspected = introspector.introspect(List.of(artifact));
        Object specTree = parseSpecTree(introspected, artifact);
        String connectorId = declaredId(specTree, introspected, artifact);
        rejectUnofficialConnector(connectorId);
        String hash = hashOf(artifact);
        rejectConflictingArtifact(connectorId, hash);
        // Kept before the artifact is registered, so that a store failure writing it fails a register that
        // has taken nothing: the id is still free and the caller's retry is a clean first attempt. The
        // source is read straight off the artifact and does not depend on the connector loading, so there
        // is nothing about the registration it needs to wait for. An artifact refused after this point
        // leaves the source stored and unreferenced, which costs one small content-addressed document
        // that the next registration of the same spec reuses.
        String specHash = storeSpecSource(introspected);
        RegistrationOutcome outcome;
        try (var bytes = Files.newInputStream(artifact)) {
            outcome = registry.register(connectorId, introspected.pdkApiVersion(), source, hash, Files.size(artifact), bytes);
        } catch (IOException failure) {
            TapstateException diagnostic = artifactUnreadable(artifact);
            for (Throwable suppressed : failure.getSuppressed()) {
                if (suppressed instanceof TapstateException cleanup && cleanup.code() == IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED
                        && cleanup.args().get("hash") instanceof String cleanupHash && cleanupHash.matches("[0-9a-f]{64}")
                        && cleanup.args().get("upload") instanceof String upload && canonicalUploadId(upload)) {
                    diagnostic.addSuppressed(new TapstateException(IoError.CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED,
                            Map.of("hash", cleanupHash, "upload", upload), null));
                }
            }
            throw diagnostic;
        }
        persistCatalogRow(connectorId, introspected, specTree, specHash, outcome);
        return outcome;
    }

    /**
     * Registers the artifact carried by {@code artifact} bytes — the entry the runtime register
     * operation uses, since a remote caller hands over bytes rather than a server path. The bytes are
     * staged to a temporary jar because introspection needs a real file, then registered exactly as the
     * on-disk seed path is; the staged jar is removed afterwards.
     */
    @Override
    public RegistrationOutcome register(byte[] artifact, RegistrationSource source) {
        Objects.requireNonNull(artifact, "artifact");
        Objects.requireNonNull(source, "source");
        Path staged = stage(artifact);
        try {
            return register(staged, source);
        } finally {
            deleteStaged(staged);
        }
    }

    /**
     * Refuses a register naming a connector outside the accepted set, before any byte enters the
     * distribution store — so a refused register leaves no half-registration wedging the id. Staging
     * and self-scan run first, because the id being checked is the one the artifact's own spec
     * declares and reading it means opening the artifact.
     *
     * <p>Every registration source is gated, the seed sweep included. A seed directory is not
     * release-controlled: a deployment mounts its own directory as the seed directory and stages jars
     * there, so an ungated sweep would register anything left in it and the accepted set would bound
     * only the artifacts that happened to arrive over the wire. A refused seed jar is one contained
     * per-jar failure the sweep reports and carries on from, exactly like any other defective artifact.
     */
    private void rejectUnofficialConnector(String connectorId) {
        if (acceptedConnectorIds.contains(connectorId)) {
            return;
        }
        // The message names what would actually be accepted here, not the release default: a caller
        // debugging a refusal needs the set this server is applying, whatever it was started with.
        throw new TapstateException(ConnectorError.NOT_OFFICIAL,
                Map.of("connector", connectorId,
                        "official", String.join(", ", acceptedConnectorIds)),
                null);
    }

    /**
     * Refuses a different artifact under a connector id that already has one — no silent overwrite.
     * Asked of the one id being registered: deciding it from the whole listing would fail this register
     * whenever any other, unrelated stored registration could not be reconstructed.
     *
     * <p>Every artifact under the id is compared, not just one of them. An id carrying two artifacts is
     * a state a connector load refuses outright, so a register that looked at only one could answer
     * "already registered, nothing to do" for bytes that match that one while the id stays unloadable —
     * silencing the last signal that anything is wrong with it.
     */
    private void rejectConflictingArtifact(String connectorId, String incomingHash) {
        for (ConnectorRegistration existing : registry.findAll(connectorId)) {
            if (!existing.contentHash().equals(incomingHash)) {
                throw new TapstateException(ConnectorError.REGISTRATION_CONFLICT,
                        Map.of("connector", connectorId,
                                "existing", existing.contentHash(),
                                "incoming", incomingHash),
                        null);
            }
        }
    }

    private static Path stage(byte[] artifact) {
        Path staged;
        try {
            staged = Files.createTempFile("tapstate-connector-", ".jar");
        } catch (IOException e) {
            throw new UncheckedIOException("staging connector artifact for registration", e);
        }
        try {
            Files.write(staged, artifact);
        } catch (IOException e) {
            // The temp file exists but the write failed: delete it so a write failure does not leak it (the
            // caller's cleanup only runs once register(byte[]) holds the returned path).
            deleteStaged(staged);
            throw new UncheckedIOException("staging connector artifact for registration", e);
        }
        return staged;
    }

    private static void deleteStaged(Path staged) {
        try {
            Files.deleteIfExists(staged);
        } catch (IOException e) {
            // Best-effort: a leaked staging jar is OS-reclaimable and must not mask the register outcome.
        }
    }

    /** Parses the spec text to its object tree, refusing coded when it is not valid JSON. */
    private static Object parseSpecTree(IntrospectedConnector introspected, Path artifact) {
        try {
            return JsonReader.parse(introspected.spec());
        } catch (IllegalArgumentException e) {
            throw specInvalid(introspected, artifact, "the spec is not valid JSON", e);
        }
    }

    /** The connector id the spec declares under {@code properties.id} — the registration identity. */
    private static String declaredId(Object specTree, IntrospectedConnector introspected, Path artifact) {
        if (specTree instanceof Map<?, ?> root
                && root.get("properties") instanceof Map<?, ?> properties
                && properties.get("id") instanceof String id
                && !id.isBlank()) {
            return id;
        }
        throw specInvalid(introspected, artifact, "the spec does not declare properties.id as a non-blank string", null);
    }

    /**
     * Derives the connector's normalized catalog row and stores it, so the online catalog view can see
     * the registered connector. Skipped when the bytes were already registered and a row already exists;
     * otherwise the row is (re)derived, which backfills a row missing after a prior partial register.
     */
    /**
     * Stores the artifact's spec source under its content hash if it is not already stored, and returns
     * that hash. Written at most once per distinct source: a connector registered before sources were
     * kept has none, and that gap is what a re-register backfills.
     */
    private String storeSpecSource(IntrospectedConnector introspected) {
        byte[] specSource = introspected.spec().getBytes(StandardCharsets.UTF_8);
        String specHash = ContentHash.of(specSource);
        if (!specStore.has(specHash)) {
            specStore.put(specHash, specSource);
        }
        return specHash;
    }

    private void persistCatalogRow(
            String connectorId, IntrospectedConnector introspected, Object specTree, String specHash,
            RegistrationOutcome outcome) {
        if (!outcome.newlyRegistered() && catalogStore.get(connectorId).isPresent()) {
            // The bytes were already registered and their row is already derived, so there is nothing to
            // redo. A row missing here is what a re-register backfills.
            return;
        }
        ConnectorCapabilities capabilities;
        try {
            capabilities = capabilityDeriver.derive(connectorId);
        } catch (TapstateException containedDerivationFailure) {
            // The catalog row is best-effort in exactly one respect: whether the connector's capabilities
            // can be derived. A connector that introspects (entry class + spec id) but will not load in
            // this deployment (an incompatible PDK level, or a construct-time failure) fails here, and that
            // coded failure is contained so the register never reports failure over already-stored bytes and
            // wedges the id. The row stays absent — so the connector is out of the online catalog — until a
            // re-register derives it (the missing-row backfill re-runs derivation).
            //
            // The containment is this one call and no more. A store failure writing the spec source or the
            // row is also a coded exception, and swallowing one would report a registration that succeeded
            // while the connector stayed invisible in the online catalog, with nothing reported anywhere. A
            // programmer bug (a bare RuntimeException, not a coded one) still crashes rather than being
            // swallowed.
            return;
        }
        NormalizedSpec normalized = SpecNormalizer.normalize(asSpecObject(specTree));
        ConnectorCatalogEntry row = CatalogEntryAssembler.assemble(
                normalized, capabilities.capabilityIds(), OVERLAY,
                introspected.specPath(), specHash);
        catalogStore.upsert(row);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asSpecObject(Object specTree) {
        // declaredId has already confirmed the spec parses to a JSON object carrying properties.id.
        return (Map<String, Object>) specTree;
    }

    private static TapstateException specInvalid(
            IntrospectedConnector introspected, Path artifact, String detail, Throwable cause) {
        return new TapstateException(ConnectorError.SPEC_INVALID,
                Map.of("artifact", artifact.getFileName().toString(),
                        "spec", introspected.specPath(),
                        "detail", detail),
                cause);
    }

    private static String hashOf(Path artifact) {
        try (var bytes = Files.newInputStream(artifact)) {
            return ContentHash.of(bytes);
        } catch (IOException failure) {
            throw artifactUnreadable(artifact);
        }
    }

    private static TapstateException artifactUnreadable(Path artifact) {
        return new TapstateException(ConnectorError.ARTIFACT_UNREADABLE,
                Map.of("artifact", artifact.getFileName().toString()), null);
    }

    private static boolean canonicalUploadId(String upload) {
        try { return UUID.fromString(upload).toString().equals(upload); }
        catch (IllegalArgumentException invalid) { return false; }
    }
}
