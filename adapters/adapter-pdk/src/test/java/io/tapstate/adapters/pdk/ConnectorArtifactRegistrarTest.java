package io.tapstate.adapters.pdk;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.ModeSource;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceMode;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Registration by introspection: {@link ConnectorArtifactRegistrar} turns a connector artifact on
 * disk into one register-if-absent call — self-scan supplies the entry class and PDK API version,
 * the spec's {@code properties.id} supplies the connector id, and the artifact bytes go to the
 * distribution store. The startup seed sweep and the explicit register operation both stand on this
 * one path, differing only in the {@link RegistrationSource} they record.
 */
class ConnectorArtifactRegistrarTest {

    @Test
    void registersAConnectorArtifactUnderItsSpecDeclaredId(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        RegistrationOutcome outcome = registrarOver(registry).register(jar, RegistrationSource.SEED);

        assertThat(outcome.newlyRegistered()).isTrue();
        ConnectorRegistration registration = outcome.registration();
        assertThat(registration.connectorId()).isEqualTo("mysql");
        assertThat(registration.pdkApiVersion()).isEqualTo("1.3.5");
        assertThat(registration.source()).isEqualTo(RegistrationSource.SEED);
        assertThat(registry.artifact(registration.contentHash())).contains(Files.readAllBytes(jar));
    }

    @Test
    void reRegisteringTheSameArtifactBytesIsANoOp(@TempDir Path dir) {
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorArtifactRegistrar registrar = registrarOver(registry);

        registrar.register(jar, RegistrationSource.SEED);
        RegistrationOutcome again = registrar.register(jar, RegistrationSource.SEED);

        assertThat(again.newlyRegistered()).isFalse();
        assertThat(registry.list()).hasSize(1);
    }

    @Test
    void refusesAnArtifactWhoseSpecIsNotJson(@TempDir Path dir) {
        Path jar = Synthetic.unparsableSpecConnector(dir);
        ConnectorArtifactRegistrar registrar = registrarOver(new InMemoryConnectorRegistry());

        assertThatThrownBy(() -> registrar.register(jar, RegistrationSource.SEED))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException coded = (TapstateException) e;
                    assertThat(coded.code()).isEqualTo(ConnectorError.SPEC_INVALID);
                    assertThat(coded.args()).containsKeys("artifact", "spec", "detail");
                });
    }

    @Test
    void refusesAnArtifactWhoseSpecDeclaresNoPropertiesId(@TempDir Path dir) {
        // This fixture's spec is {"id":"orders"}: valid JSON, but a connector spec carries its
        // identity under properties.id — a top-level id is not one.
        Path jar = Synthetic.annotatedConnector(dir);
        ConnectorArtifactRegistrar registrar = registrarOver(new InMemoryConnectorRegistry());

        assertThatThrownBy(() -> registrar.register(jar, RegistrationSource.SEED))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException coded = (TapstateException) e;
                    assertThat(coded.code()).isEqualTo(ConnectorError.SPEC_INVALID);
                    // The diagnostic must cover an id that is present but unusable (wrong type,
                    // blank), not claim the field is absent when it is visibly there.
                    assertThat(String.valueOf(coded.args().get("detail"))).contains("non-blank string");
                });
    }

    @Test
    void registersFromArtifactBytesJustLikeFromAPath(@TempDir Path dir) throws Exception {
        // The runtime register operation hands the registrar bytes off the wire, not a server path; the
        // bytes entry must land the same registration the on-disk seed path does — same id, same hash.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        byte[] bytes = Files.readAllBytes(jar);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();

        RegistrationOutcome outcome = registrarOver(registry).register(bytes, RegistrationSource.REGISTER);

        assertThat(outcome.newlyRegistered()).isTrue();
        ConnectorRegistration registration = outcome.registration();
        assertThat(registration.connectorId()).isEqualTo("mysql");
        assertThat(registration.pdkApiVersion()).isEqualTo("1.3.5");
        assertThat(registration.source()).isEqualTo(RegistrationSource.REGISTER);
        assertThat(registry.artifact(registration.contentHash())).contains(bytes);
    }

    @Test
    void refusesADifferentArtifactUnderAnAlreadyRegisteredId(@TempDir Path dir) {
        // Same bytes re-registering is a no-op (idempotent by hash); a DIFFERENT artifact claiming an
        // already-registered id is a conflict — selecting among versions is out of scope, so it is
        // refused at register time rather than silently accepted to blow up at load.
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorArtifactRegistrar registrar = registrarOver(registry);
        RegistrationOutcome first = registrar.register(
                Synthetic.seedableMysqlConnector(dir), RegistrationSource.SEED);

        assertThatThrownBy(() -> registrar.register(
                        Synthetic.conflictingMysqlConnector(dir), RegistrationSource.REGISTER))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException coded = (TapstateException) e;
                    assertThat(coded.code()).isEqualTo(ConnectorError.REGISTRATION_CONFLICT);
                    assertThat(coded.args()).containsKeys("connector", "existing", "incoming");
                    assertThat(coded.args().get("connector")).isEqualTo("mysql");
                    assertThat(coded.args().get("existing")).isEqualTo(first.registration().contentHash());
                });
        // The conflicting artifact is not stored: the store still holds exactly the first registration.
        assertThat(registry.list()).hasSize(1);
    }

    @ParameterizedTest
    @EnumSource(RegistrationSource.class)
    void refusesAConnectorOutsideTheOfficialSetWhicheverWayItArrives(
            RegistrationSource source, @TempDir Path dir) {
        // Only officially supported connectors may be registered. One outside that set is refused with a
        // coded error BEFORE any byte reaches the store: a refused register must not leave the id wedged
        // by stored bytes that no catalog row will ever describe.
        //
        // Every source is gated, seed included, and the enum drives this so a source added later is
        // gated by default rather than opening a hole nobody tested. A seed directory is a deployment's
        // own directory, not something a release controls: exempting it would bound the accepted set
        // only for artifacts that happened to arrive over the wire.
        Path jar = Synthetic.seedableOrdersConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, new InMemoryConnectorSpecStore());

        assertThatThrownBy(() -> registrar.register(jar, source))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException coded = (TapstateException) e;
                    assertThat(coded.code()).isEqualTo(ConnectorError.NOT_OFFICIAL);
                    assertThat(coded.args()).containsEntry("connector", "orders");
                    // The set is named in the error itself, so the message reads as a boundary that
                    // moves ("these are supported today") rather than as a defect in the artifact.
                    assertThat(String.valueOf(coded.args().get("official")))
                            .contains("mysql").contains("mongodb");
                });
        assertThat(registry.list()).isEmpty();
        assertThat(rows.get("orders")).isEmpty();
    }

    @Test
    void theDefaultAcceptedSetIsExactlyTheOfficialConnectors() {
        // The pin. Widening is a deployment's explicit act; the default must not drift, because every
        // shipped artifact leaves it alone and a silent addition here is a support promise nobody made.
        //
        // Written out in full rather than counted or matched by prefix. The release verifies the
        // database kinds themselves; each managed variant is accepted by an explicit support decision.
        // Order is the order a refusal message names them in.
        assertThat(OfficialConnectors.IDS).containsExactly(
                "mysql", "aliyun-rds-mysql", "aws-rds-mysql", "polar-db-mysql", "mysql-pxc",
                "postgres", "aliyun-rds-postgres", "aliyun-adb-postgres", "polar-db-postgres",
                "tencent-db-postgres",
                "mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb");
    }

    @Test
    void acceptsAWidenedIdOnlyWhenTheDeploymentNamesIt(@TempDir Path dir) {
        // A deployment that starts its own server — the product's own test harness — can name further
        // ids to accept. Nothing ships with any named, so this changes no released behaviour.
        Path jar = Synthetic.seedableOrdersConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorArtifactRegistrar widened = new ConnectorArtifactRegistrar(
                registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")),
                new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(), List.of("orders"));

        RegistrationOutcome outcome = widened.register(jar, RegistrationSource.REGISTER);

        assertThat(outcome.newlyRegistered()).isTrue();
        assertThat(registry.list()).hasSize(1);
    }

    @Test
    void registerDerivesAndPersistsTheConnectorCatalogRow(@TempDir Path dir) {
        // After a connector's bytes are registered, its normalized catalog row is derived and stored so
        // the online catalog view can see it: batch_read_function derives the snapshot source mode.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, new InMemoryConnectorSpecStore());

        registrar.register(jar, RegistrationSource.REGISTER);

        ConnectorCatalogEntry row = rows.get("mysql").orElseThrow();
        assertThat(row.id()).isEqualTo("mysql");
        assertThat(row.modes()).contains(SourceMode.SNAPSHOT);
    }

    @Test
    void storesTheSpecSourceVerbatimUnderTheHashTheCatalogRowPointsAt(@TempDir Path dir) throws Exception {
        // Normalization is lossy — it keeps the fields a safe form consumes and drops the rest. The
        // source is kept alongside it, keyed by the very hash the row's provenance already carries, so
        // that pointer can be dereferenced without reopening the jar.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();
        registrarOver(new InMemoryConnectorRegistry(), rows, specs)
                .register(jar, RegistrationSource.REGISTER);

        ConnectorCatalogEntry row = rows.get("mysql").orElseThrow();
        // Byte-exact, not "equivalent JSON": a re-serialized copy would silently lose key order,
        // unknown top-level keys and any type table the normalizer has no field for — which is the
        // whole reason the source is kept.
        assertThat(specs.get(row.provenance().specContentHash()))
                .contains(specEntryBytes(jar, "mysql-spec.json"));
        // The normalized row stands unchanged beside it: the source is an addition, not a replacement.
        assertThat(row.id()).isEqualTo("mysql");
        assertThat(row.modes()).contains(SourceMode.SNAPSHOT);
    }

    @Test
    void reRegisteringBackfillsTheSpecSourceWhenOnlyTheRowIsAlreadyStored(@TempDir Path dir) throws Exception {
        // A connector registered before sources were kept has bytes and a row but no source. A
        // re-register must notice the missing source rather than stopping at "the row is there, nothing
        // to do" — otherwise the hash every such row already carries stays permanently dangling.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        registrarOver(registry, rows, new InMemoryConnectorSpecStore())
                .register(jar, RegistrationSource.SEED);
        InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();

        RegistrationOutcome outcome = registrarOver(registry, rows, specs)
                .register(jar, RegistrationSource.SEED);

        assertThat(outcome.newlyRegistered()).isFalse();
        ConnectorCatalogEntry row = rows.get("mysql").orElseThrow();
        assertThat(specs.get(row.provenance().specContentHash()))
                .contains(specEntryBytes(jar, "mysql-spec.json"));
    }

    /** The bytes of one entry inside a jar, read independently of the registrar's own reading of it. */
    private static byte[] specEntryBytes(Path jar, String entryName) throws Exception {
        try (java.util.zip.ZipFile zip = new java.util.zip.ZipFile(jar.toFile())) {
            return zip.getInputStream(zip.getEntry(entryName)).readAllBytes();
        }
    }

    @Test
    void reRegisteringDoesNotReDeriveWhenTheRowIsAlreadyStored(@TempDir Path dir) {
        // An idempotent re-register (same bytes, already registered and already rowed) must not pay the
        // classload-derive cost again — the stored row is reused.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        int[] derivations = {0};
        CapabilityDeriver counting = id -> {
            derivations[0]++;
            return new ConnectorCapabilities(Set.of("batch_read_function"));
        };
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(), counting,
                new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore());

        registrar.register(jar, RegistrationSource.SEED);
        registrar.register(jar, RegistrationSource.SEED);

        assertThat(derivations[0]).isEqualTo(1);
    }

    @Test
    void reRegisteringBackfillsTheRowWhenItIsMissing(@TempDir Path dir) throws Exception {
        // The bytes were registered by a prior run but no catalog row was derived (a crash between the
        // two, or a pre-feature registration): a re-register backfills the missing row even though the
        // bytes are not newly registered.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        byte[] bytes = Files.readAllBytes(jar);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("mysql", "1.3.5", RegistrationSource.SEED, bytes);
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, new InMemoryConnectorSpecStore());

        RegistrationOutcome outcome = registrar.register(jar, RegistrationSource.SEED);

        assertThat(outcome.newlyRegistered()).isFalse();
        assertThat(rows.get("mysql")).isPresent();
    }

    @Test
    void containsACodedDerivationFailureAndStillRegistersTheBytes(@TempDir Path dir) {
        // Derivation is best-effort: a connector that introspects but whose capabilities cannot be derived
        // (it will not load in this deployment) is still registered — its bytes are stored and the op
        // succeeds — rather than reporting failure over already-stored bytes and wedging the id. The catalog
        // row is simply absent until derivation succeeds on a re-register. A coded connector/derive failure
        // is contained; a programmer bug (a bare RuntimeException) still crashes.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        CapabilityDeriver failing = id -> {
            throw new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", id), null);
        };
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(), failing, rows,
                new InMemoryConnectorSpecStore());

        RegistrationOutcome outcome = registrar.register(jar, RegistrationSource.REGISTER);

        assertThat(outcome.newlyRegistered()).isTrue();
        assertThat(rows.get("mysql")).isEmpty();
    }

    @Test
    void refusesAConflictWithoutReadingEveryOtherRegistration(@TempDir Path dir) {
        // The conflict is about one id, so it is asked about one id. Deciding it from the whole listing
        // would fail this register whenever any other, unrelated stored registration could not be
        // reconstructed - an outage across every connector caused by one corrupt entry.
        InMemoryConnectorRegistry backing = new InMemoryConnectorRegistry();
        ConnectorArtifactRegistrar seeding = registrarOver(backing);
        seeding.register(Synthetic.seedableMysqlConnector(dir), RegistrationSource.SEED);
        ConnectorRegistry unlistable = new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(
                    String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                return backing.register(id, pdkApiVersion, source, artifact);
            }

            @Override
            public List<ConnectorRegistration> list() {
                throw new IllegalStateException("one unrelated stored registration cannot be reconstructed");
            }

            @Override
            public List<ConnectorRegistration> findAll(String connectorId) {
                return backing.list().stream()
                        .filter(registration -> registration.connectorId().equals(connectorId))
                        .toList();
            }

            @Override
            public Optional<byte[]> artifact(String contentHash) {
                return backing.artifact(contentHash);
            }

            @Override
            public boolean hasArtifact(String contentHash) {
                return backing.hasArtifact(contentHash);
            }
        };
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                unlistable, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")),
                new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore());

        assertThatThrownBy(() -> registrar.register(
                        Synthetic.conflictingMysqlConnector(dir), RegistrationSource.REGISTER))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.REGISTRATION_CONFLICT));
    }

    @Test
    void refusesEvenBytesThatMatchOneOfTwoArtifactsSharingTheId(@TempDir Path dir) throws Exception {
        // An id carrying two artifacts is a state a connector load refuses outright. A register that
        // compared the incoming bytes against only one of them would answer "already registered, nothing
        // to do" whenever they happened to match that one - reporting success for a connector that cannot
        // be loaded, and silencing the last place the duplicate was visible.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, Files.readAllBytes(jar));
        registry.register("mysql", "1.3.5", RegistrationSource.REGISTER, "a second artifact".getBytes(UTF_8));

        assertThatThrownBy(() -> registrarOver(registry).register(jar, RegistrationSource.REGISTER))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.REGISTRATION_CONFLICT));
    }

    @Test
    void testsForTheStoredSpecSourceWithoutReadingIt(@TempDir Path dir) {
        // A re-register only needs to know whether the source is already filed. A spec source is a whole
        // connector form, so reading one back to compute a boolean costs the whole form - on every jar of
        // every seed sweep, at every startup.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        InMemoryConnectorSpecStore stored = new InMemoryConnectorSpecStore();
        registrarOver(registry, rows, stored).register(jar, RegistrationSource.SEED);
        ConnectorSpecStore presenceOnly = new ConnectorSpecStore() {
            @Override
            public void put(String contentHash, byte[] spec) {
                stored.put(contentHash, spec);
            }

            @Override
            public Optional<byte[]> get(String contentHash) {
                throw new UnsupportedOperationException("presence must be tested without reading the source");
            }

            @Override
            public boolean has(String contentHash) {
                return stored.has(contentHash);
            }
        };

        RegistrationOutcome outcome = new ConnectorArtifactRegistrar(
                registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, presenceOnly)
                .register(jar, RegistrationSource.SEED);

        assertThat(outcome.newlyRegistered()).isFalse();
    }

    @Test
    void doesNotContainAStoreFailureWritingTheSpecSource(@TempDir Path dir) {
        // The containment above is for derivation and nothing else. A store failure is coded too, so a
        // catch wide enough to hold the whole persist step would swallow it: the spec source write would
        // fail, the derivation and the catalog row after it would never run, and the caller would be told
        // the connector registered while it stayed invisible in the online catalog with nothing reported
        // anywhere. It must surface.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows,
                new ConnectorSpecStore() {
                    @Override
                    public void put(String contentHash, byte[] spec) {
                        throw storeUnavailable();
                    }

                    @Override
                    public Optional<byte[]> get(String contentHash) {
                        return Optional.empty();
                    }
                });

        assertThatThrownBy(() -> registrar.register(jar, RegistrationSource.REGISTER))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(StoreFailure.UNAVAILABLE));
        assertThat(rows.get("mysql")).isEmpty();
    }

    @Test
    void doesNotContainAStoreFailureWritingTheCatalogRow(@TempDir Path dir) {
        // The same, one step later: the row write is the whole point of deriving, so losing its failure
        // would report a registration whose connector never reaches the online catalog.
        Path jar = Synthetic.seedableMysqlConnector(dir);
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")),
                new ConnectorCatalogStore() {
                    @Override
                    public void upsert(ConnectorCatalogEntry entry) {
                        throw storeUnavailable();
                    }

                    @Override
                    public Optional<ConnectorCatalogEntry> get(String connectorId) {
                        return Optional.empty();
                    }

                    @Override
                    public List<ConnectorCatalogEntry> list() {
                        return List.of();
                    }
                },
                new InMemoryConnectorSpecStore());

        assertThatThrownBy(() -> registrar.register(jar, RegistrationSource.REGISTER))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(StoreFailure.UNAVAILABLE));
    }

    private static TapstateException storeUnavailable() {
        return new TapstateException(StoreFailure.UNAVAILABLE, Map.of(), null);
    }

    /**
     * Stands in for the coded diagnostic a store adapter raises when its driver fails. The real one
     * belongs to the store adapter and is not on this module's path; what these tests need is only that
     * it is coded, because being coded is exactly what a containment catch would swallow.
     */
    private enum StoreFailure implements TapstateErrorCode {
        UNAVAILABLE;

        @Override
        public String code() {
            return "test.store-unavailable";
        }

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of();
        }
    }

    @Test
    void requiresItsCollaboratorsAndArguments(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorIntrospector introspector = new ConnectorIntrospector();
        CapabilityDeriver deriver = id -> new ConnectorCapabilities(Set.of());
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();

        InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();

        assertThatNullPointerException().isThrownBy(() -> new ConnectorArtifactRegistrar(null, introspector, deriver, rows, specs));
        assertThatNullPointerException().isThrownBy(() -> new ConnectorArtifactRegistrar(registry, null, deriver, rows, specs));
        assertThatNullPointerException().isThrownBy(() -> new ConnectorArtifactRegistrar(registry, introspector, null, rows, specs));
        assertThatNullPointerException().isThrownBy(() -> new ConnectorArtifactRegistrar(registry, introspector, deriver, null, specs));
        assertThatNullPointerException().isThrownBy(() -> new ConnectorArtifactRegistrar(registry, introspector, deriver, rows, null));

        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(registry, introspector, deriver, rows, specs);
        assertThatNullPointerException().isThrownBy(() -> registrar.register((Path) null, RegistrationSource.SEED));
        assertThatNullPointerException().isThrownBy(() -> registrar.register(dir.resolve("x.jar"), null));
        assertThatNullPointerException().isThrownBy(() -> registrar.register((byte[]) null, RegistrationSource.SEED));
        assertThatNullPointerException().isThrownBy(() -> registrar.register(new byte[0], null));
    }

    private static ConnectorArtifactRegistrar registrarOver(InMemoryConnectorRegistry registry) {
        return registrarOver(registry, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore());
    }

    private static ConnectorArtifactRegistrar registrarOver(
            InMemoryConnectorRegistry registry, InMemoryConnectorCatalogStore rows, InMemoryConnectorSpecStore specs) {
        return new ConnectorArtifactRegistrar(registry, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("batch_read_function")), rows, specs);
    }

    @Test
    void aRuntimeRegisteredRowCarriesOurOwnDeclaration(@TempDir Path dir) {
        // The runtime half of the merge. The build-time path has its own twin in the assembler's tests;
        // this is the one the checked-in snapshot cannot speak for, because the snapshot is produced by
        // the other path entirely. Wire the overlay into one entry and not the other and the offline
        // catalog and the registered row describe the same connector differently.
        //
        // Driven against the real bundled overlay rather than a fixture, so it also proves the shipped
        // resource is reachable from this module. kafka is not in the official set, so the deployment
        // has to name it - which is exactly the seam that exists for a deployment supplying its own
        // connector.
        Path jar = Synthetic.seedableConnector(dir, "kafka");
        InMemoryConnectorCatalogStore rows = new InMemoryConnectorCatalogStore();
        ConnectorArtifactRegistrar registrar = new ConnectorArtifactRegistrar(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(Set.of("stream_read_function")), rows,
                new InMemoryConnectorSpecStore(), List.of("kafka"));

        registrar.register(jar, RegistrationSource.REGISTER);

        ConnectorCatalogEntry row = rows.get("kafka").orElseThrow();
        assertThat(row.modes())
                .as("our declaration says stream; without it the stream_read capability derives cdc")
                .containsExactly(SourceMode.STREAM);
        assertThat(row.provenance().modeSource().values()).containsOnly(ModeSource.OVERLAY);
        assertThat(row.modesAreTrustworthy())
                .as("an overlay declaration has to count as a declaration, or validation quietly "
                        + "defers for every connector we declare")
                .isTrue();
    }
}
