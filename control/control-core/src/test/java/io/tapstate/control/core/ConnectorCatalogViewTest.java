package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.TapstateCatalog;

/**
 * The online catalog view keeps the bundled snapshot for capability validation and detail fallback, while
 * the authoring list exposes only rows derived for registered connectors. A registration becomes visible
 * without a restart because the view re-reads the store per call.
 */
class ConnectorCatalogViewTest {

    private static final TapstateCatalog BUNDLED = TapstateCatalog.load();

    private static final String ACME_ROW = """
            {
              "id": "acme", "name": "Acme", "displayName": "Acme", "icon": null,
              "group": "database", "modes": ["snapshot"], "discovery": "catalog",
              "sink": {"capable": false, "writeSemantics": []}, "pushOut": false, "config": [],
              "provenance": {"connectorRepoSha": null, "specPath": "spec.json", "specContentHash": "h",
                "pdkApiVersion": "1.0.0", "requiredLevel": null, "modeSource": {"snapshot": "derived"}}
            }
            """;

    @Test
    void detailCarriesTheStoredSpecSourceUnderTheRowsProvenanceHash() {
        // The normalized row is a lossy projection; the source is kept beside it under the very hash the
        // row's provenance already records. The detail read is where that pointer stops being a pointer.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        InMemoryConnectorSpecStore specs = new InMemoryConnectorSpecStore();
        String source = "{\"properties\":{\"id\":\"acme\"},\"zz\":1,\"a\":2}";
        specs.put("h", source.getBytes(StandardCharsets.UTF_8));
        ConnectorCatalogView view = new ConnectorCatalogView(BUNDLED, store, specs, emptyRegistry());

        ConnectorDetail detail = view.detail("acme");

        assertThat(detail.spec().contentHash()).isEqualTo("h");
        assertThat(detail.spec().text()).isEqualTo(source);
        assertThat(detail.spec().unavailable()).isNull();
        // The projection stands unchanged beside it: the source is an addition, not a replacement, and
        // the fields a connection form already consumes must not shift shape under it.
        assertThat(detail.id()).isEqualTo("acme");
        assertThat(detail.origin()).isEqualTo("registered");
        assertThat(detail.config()).isEmpty();
        assertThat(detail.modes()).containsExactly("snapshot");
    }

    @Test
    void detailSaysWhyTheSpecSourceIsAbsentRatherThanReturningNothing() {
        // A bundled row has no stored source. Answering with a bare null invites a consumer to
        // reconstruct a spec from the normalized config — the one thing the source exists to prevent —
        // so the absence is stated, with the hash still named so a later store can be checked again.
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(), emptyRegistry());

        ConnectorDetail detail = view.detail(BUNDLED.ids().iterator().next());

        assertThat(detail.spec().text()).isNull();
        assertThat(detail.spec().unavailable()).isEqualTo("not-stored");
        assertThat(detail.origin()).isEqualTo("bundled");
    }

    @Test
    void detailStatesTheAbsenceForARowWhoseProvenanceNamesNoSpecHashAtAll() {
        // A row can carry no hash to dereference, not merely a hash nothing is stored under. Both are
        // absences and both must be stated, but they arrive by different routes - and the one with no
        // pointer at all is the route where returning a bare null would be easiest to write.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW.replace("\"specContentHash\": \"h\"", "\"specContentHash\": null")));
        ConnectorCatalogView view =
                new ConnectorCatalogView(BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        ConnectorDetail detail = view.detail("acme");

        assertThat(detail.spec().contentHash()).isNull();
        assertThat(detail.spec().text()).isNull();
        assertThat(detail.spec().unavailable()).isEqualTo("not-stored");
    }

    @Test
    void runtimeAvailableIsTrueOnlyWhenTheArtifactBytesAreActuallyThere() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), registryHolding("acme", "jar-hash", true));

        assertThat(view.detail("acme").runtimeAvailable()).isTrue();
    }

    @Test
    void runtimeAvailableIsFalseForARegisteredRowWhoseArtifactBytesAreGone() {
        // The case that separates runtimeAvailable from origin. Both say "registered" here, yet the jar
        // cannot be loaded — an implementation that reads origin and stops would answer true, and every
        // consumer would be told a connector can run when it cannot.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), registryHolding("acme", "jar-hash", false));

        ConnectorDetail detail = view.detail("acme");

        assertThat(detail.origin()).isEqualTo("registered");
        assertThat(detail.runtimeAvailable()).isFalse();
    }

    @Test
    void runtimeAvailableIsFalseForABundledRowThatWasNeverRegistered() {
        // A bundled row is catalog metadata shipped with the release; nothing says its jar is present.
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(), emptyRegistry());

        ConnectorDetail detail = view.detail(BUNDLED.ids().iterator().next());

        assertThat(detail.origin()).isEqualTo("bundled");
        assertThat(detail.runtimeAvailable()).isFalse();
    }

    @Test
    void mergedUnionsRegisteredRowsOverTheBundledSnapshot() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        TapstateCatalog merged = view.merged();

        assertThat(merged.ids()).containsAll(BUNDLED.ids()).contains("acme");
        assertThat(merged.byId("acme").displayName()).isEqualTo("Acme");
    }

    @Test
    void mergedReflectsRegistrationsMadeAfterTheViewWasConstructed() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());
        assertThat(view.merged().ids()).doesNotContain("acme");

        store.upsert(CatalogEntryReader.read(ACME_ROW));

        // The view re-reads the store per call, so a runtime registration shows up without a restart.
        assertThat(view.merged().ids()).contains("acme");
    }

    @Test
    void summariesExposeOnlyRegisteredConnectorsAsAuthoringCandidates() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        List<ConnectorSummary> summaries = view.summaries();

        assertThat(summaries).extracting(ConnectorSummary::id).containsExactly("acme");
        assertThat(summaries.get(0).origin()).isEqualTo("registered");
        assertThat(summaries.get(0).modes()).contains("snapshot");
    }

    @Test
    void summariesReflectRegistrationsAddedAfterTheViewWasConstructed() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        assertThat(view.summaries()).isEmpty();

        store.upsert(CatalogEntryReader.read(ACME_ROW));

        assertThat(view.summaries()).extracting(ConnectorSummary::id).containsExactly("acme");
    }

    @Test
    void detailProjectsTheNormalizedConfigWithoutRawFormilyExpressions() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        ConnectorDetail detail = view.detail("mysql");

        assertThat(detail.id()).isEqualTo("mysql");
        assertThat(detail.origin()).isEqualTo("bundled");
        assertThat(detail.config()).anySatisfy(field -> {
            assertThat(field.name()).isEqualTo("password");
            assertThat(field.type()).isEqualTo("string");
            assertThat(field.secret()).isTrue();
        });
        assertThat(detail.config()).anySatisfy(field -> {
            assertThat(field.name()).isEqualTo("host");
            assertThat(field.visibleWhen()).isNotNull();
            assertThat(field.visibleWhen().controllingField()).isEqualTo("deploymentMode");
            assertThat(field.visibleWhen().equalsAnyOf()).containsExactly("standalone");
        });
    }

    @Test
    void detailReadsTheLiveRegisteredOverlayAndTagsItsOrigin() {
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), emptyRegistry());

        store.upsert(CatalogEntryReader.read(ACME_ROW));

        assertThat(view.detail("acme").origin()).isEqualTo("registered");
    }

    @Test
    void detailRejectsAnUnknownConnectorWithACodedError() {
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(), emptyRegistry());

        assertThatThrownBy(() -> view.detail("missing"))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        error -> assertThat(error.code().code()).isEqualTo("connector.not-found"));
    }

    @Test
    void readsAConnectorWhileAnotherStoredRegistrationCannotBeReconstructed() {
        // Registry corruption is scoped to the connector it belongs to. A registry that cannot produce a
        // full listing — one entry written by a newer build, one file left behind by a partial restore —
        // must still answer about the connector being read, or a single bad entry takes down every
        // connection form in the product, including bundled connectors that were never registered.
        ConnectorRegistry unlistable = new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException("the detail read never registers");
            }

            @Override
            public List<ConnectorRegistration> list() {
                throw new IllegalStateException("one stored registration cannot be reconstructed");
            }

            @Override
            public List<ConnectorRegistration> findAll(String connectorId) {
                return List.of();
            }

            @Override
            public Optional<byte[]> artifact(String hash) {
                throw new UnsupportedOperationException("a detail read must not pull artifact bytes");
            }

            @Override
            public boolean hasArtifact(String hash) {
                return false;
            }
        };
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(), unlistable);

        ConnectorDetail detail = view.detail(BUNDLED.ids().get(0));

        assertThat(detail.origin()).isEqualTo("bundled");
        assertThat(detail.runtimeAvailable()).isFalse();
    }

    @Test
    void readsAConnectorWhileAnotherStoredRowCannotBeReconstructed() {
        // A detail read asks about one connector. Answering it by listing every derived row makes one row
        // written by a newer build - an unknown enum value, a renamed field - fail the read of every
        // connector, bundled ones included, and with it every connection form in the product.
        ConnectorCatalogStore unlistable = new ConnectorCatalogStore() {
            @Override
            public void upsert(ConnectorCatalogEntry entry) {
                throw new UnsupportedOperationException("the detail read never writes");
            }

            @Override
            public Optional<ConnectorCatalogEntry> get(String connectorId) {
                return Optional.empty();
            }

            @Override
            public List<ConnectorCatalogEntry> list() {
                throw new IllegalStateException("one stored row cannot be reconstructed");
            }
        };
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, unlistable, new InMemoryConnectorSpecStore(), emptyRegistry());

        ConnectorDetail detail = view.detail(BUNDLED.ids().get(0));

        assertThat(detail.origin()).isEqualTo("bundled");
    }

    @Test
    void statesADamagedSpecSourceAsUnreadableRatherThanFailingTheWholeRead() {
        // The stored source is one field of the response. Everything else - the config field list a
        // connection is authored against - is intact, so a damaged spec document must not take the read
        // down with it. It is an absence, and this type exists to state absences, so it says which one.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorSpecStore damaged = new ConnectorSpecStore() {
            @Override
            public void put(String contentHash, byte[] spec) {
                throw new UnsupportedOperationException("the detail read never writes");
            }

            @Override
            public Optional<byte[]> get(String contentHash) {
                throw new TapstateException(
                        IoError.DOCUMENT_UNREADABLE, java.util.Map.of("id", contentHash), null);
            }
        };
        ConnectorCatalogView view = new ConnectorCatalogView(BUNDLED, store, damaged, emptyRegistry());

        ConnectorDetail detail = view.detail("acme");

        assertThat(detail.spec().text()).isNull();
        assertThat(detail.spec().unavailable()).isEqualTo("unreadable");
        // Distinguishable from "nothing was ever stored": a consumer retrying a not-stored hash later is
        // sensible, retrying a damaged document is not.
        assertThat(detail.spec().unavailable()).isNotEqualTo("not-stored");
        assertThat(detail.config()).isEmpty();
        assertThat(detail.modes()).containsExactly("snapshot");
    }

    @Test
    void aStoreThatCannotAnswerAtAllIsRaised() {
        // Degrading is for a document this store holds and cannot read. A store that is unreachable, or
        // that refused the credentials, is not a statement about any document - reporting it as a damaged
        // spec would name the wrong thing as broken and send an operator hunting through stored data for
        // a fault that is not in it.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorSpecStore unreachable = new ConnectorSpecStore() {
            @Override
            public void put(String contentHash, byte[] spec) {
                throw new UnsupportedOperationException("the detail read never writes");
            }

            @Override
            public Optional<byte[]> get(String contentHash) {
                throw new TapstateException(
                        IoError.STORE_UNAVAILABLE, java.util.Map.of("detail", "connection reset"), null);
            }
        };
        ConnectorCatalogView view = new ConnectorCatalogView(BUNDLED, store, unreachable, emptyRegistry());

        assertThatThrownBy(() -> view.detail("acme"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(IoError.STORE_UNAVAILABLE));
    }

    @Test
    void saysTheSourceIsNotDerivedWhenTheConnectorIsRegisteredButHasNoRowHere() {
        // Registered, its derivation never completed, so the only row is the bundled snapshot's - and the
        // hash on that row is the one recorded when the release was built, not the one this deployment's
        // artifact declares. The registered artifact's source IS stored, under a hash nothing here can
        // reach. Answering "not stored" would be answering about a different spec than the one asked for.
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, new InMemoryConnectorCatalogStore(), new InMemoryConnectorSpecStore(),
                registryHolding(BUNDLED.ids().get(0), "jar-hash", true));

        ConnectorDetail detail = view.detail(BUNDLED.ids().get(0));

        assertThat(detail.origin()).isEqualTo("bundled");
        assertThat(detail.spec().text()).isNull();
        assertThat(detail.spec().unavailable()).isEqualTo("not-derived");
    }

    @Test
    void runtimeAvailableIsFalseWhenOneIdCarriesTwoRegistrations() {
        // Loading a connector refuses an id with two artifacts outright. Reporting it available would
        // promise a connector that every operation actually using it - a connection test, a schema
        // discovery, a pipeline start - then refuses, and this boolean exists to answer exactly that.
        InMemoryConnectorCatalogStore store = new InMemoryConnectorCatalogStore();
        store.upsert(CatalogEntryReader.read(ACME_ROW));
        ConnectorCatalogView view = new ConnectorCatalogView(
                BUNDLED, store, new InMemoryConnectorSpecStore(), registryHoldingTwo("acme"));

        ConnectorDetail detail = view.detail("acme");

        assertThat(detail.origin()).isEqualTo("registered");
        assertThat(detail.runtimeAvailable()).isFalse();
    }

    /** A registry carrying two registrations under one id, both with their bytes present. */
    private static ConnectorRegistry registryHoldingTwo(String connectorId) {
        List<ConnectorRegistration> registrations = List.of(
                new ConnectorRegistration(connectorId, "hash-a", "1.0.0", RegistrationSource.REGISTER),
                new ConnectorRegistration(connectorId, "hash-b", "1.0.0", RegistrationSource.REGISTER));
        return new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException("the detail read never registers");
            }

            @Override
            public List<ConnectorRegistration> list() {
                return registrations;
            }

            @Override
            public Optional<byte[]> artifact(String hash) {
                throw new UnsupportedOperationException("a detail read must not pull artifact bytes");
            }

            @Override
            public boolean hasArtifact(String hash) {
                return true;
            }
        };
    }

    private static ConnectorRegistry emptyRegistry() {
        return registryHolding(null, null, false);
    }

    /**
     * A registry carrying at most one registration, and answering independently whether its bytes are
     * still there — the two facts the detail read combines, kept separable so a test can pull them apart.
     */
    private static ConnectorRegistry registryHolding(String connectorId, String contentHash, boolean bytesPresent) {
        List<ConnectorRegistration> registrations = connectorId == null
                ? List.of()
                : List.of(new ConnectorRegistration(connectorId, contentHash, "1.0.0", RegistrationSource.REGISTER));
        return new ConnectorRegistry() {
            @Override
            public RegistrationOutcome register(String id, String pdkApiVersion, RegistrationSource source, byte[] artifact) {
                throw new UnsupportedOperationException("the detail read never registers");
            }

            @Override
            public List<ConnectorRegistration> list() {
                return registrations;
            }

            @Override
            public Optional<byte[]> artifact(String hash) {
                throw new UnsupportedOperationException("a detail read must not pull artifact bytes");
            }

            @Override
            public boolean hasArtifact(String hash) {
                return bytesPresent && hash.equals(contentHash);
            }
        };
    }
}
