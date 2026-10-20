package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StorageSetupServiceTest {
    static final StorageSetupScope SCOPE = new StorageSetupScope("cluster", "installation", "operation", 1);
    private static StorageBinding.Connection connection(String database) {
        return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                "mongodb://user:sentinel-secret@host/" + database, database, null);
    }
    static final StorageSetupService.Candidate CANDIDATE = new StorageSetupService.Candidate(
            connection("config"), connection("cache"), connection("master"));
    static final class Time extends Clock {
        Instant now = Instant.parse("2026-10-20T00:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId ignored) { return this; }
        @Override public Instant instant() { return now; }
    }
    static final class States implements StorageSetupStateStore {
        Status state = new Status(SCOPE, Phase.CONFIG_PREPARED, 0);
        @Override public StorageSetupStateStore guardedBy(Runnable active) { active.run(); return this; }
        @Override public Status load() { return state; }
        @Override public Status rotate(StorageSetupScope expected, Purpose purpose) { throw new UnsupportedOperationException(); }
        @Override public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
            if (!state.scope().equals(expected) || state.phase() == Phase.COMPLETE) throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
        }
    }
    static final class Binding implements StorageBindingStore {
        final States states;
        final Binding root;
        final Runnable active;
        StorageBinding binding;
        int inserts;
        Runnable beforeCommit = () -> {};
        Binding(States states) { this.states = states; this.root = this; this.active = () -> {}; }
        Binding(Binding root, Runnable active) { this.states = root.states; this.root = root; this.active = active; }
        @Override public StorageBindingStore guardedBy(Runnable guard) { return new Binding(root, guard); }
        @Override public Optional<StorageBinding> load() { active.run(); return Optional.ofNullable(root.binding); }
        @Override public Optional<CachePreparation> cachePreparation() { return root.binding == null ? Optional.empty() : Optional.of(preparation()); }
        private CachePreparation preparation() { return new CachePreparation(SCOPE.operationId(), new CacheAuthScope(SCOPE.clusterId(), 1, StorageBinding.Mode.OP, "incarnation", 1)); }
        @Override public StorageBinding commit(StorageSetupScope expected, StorageBinding.Mode mode, StorageBinding.Connection cache, SourceResource source) {
            return commitWithFirstCache(expected, mode, cache, source).binding();
        }
        @Override public StorageBindingCommit commitWithFirstCache(StorageSetupScope expected, StorageBinding.Mode mode,
                StorageBinding.Connection cache, SourceResource source) {
            return commitWithFirstCache(expected, mode, cache, source, () -> {});
        }
        @Override public StorageBindingCommit commitWithFirstCache(StorageSetupScope expected, StorageBinding.Mode mode,
                StorageBinding.Connection cache, SourceResource source, Runnable requireCommit) {
            active.run(); requireCommit.run(); root.beforeCommit.run(); requireCommit.run(); active.run();
            boolean first = root.binding == null;
            if (first) {
                root.inserts++;
                root.binding = new StorageBinding(expected, mode, 1, source.id(), cache);
                states.state = new StorageSetupStateStore.Status(expected, StorageSetupStateStore.Phase.STORAGE_BOUND, 1);
            }
            return new StorageBindingCommit(root.binding, preparation(), first ? Optional.of(new FirstCacheCreation() {
                @Override public CachePreparation preparation() { return Binding.this.preparation(); }
                @Override public void consume() { active.run(); }
                @Override public void requireActive() { active.run(); }
            }) : Optional.empty());
        }
        @Override public StorageBinding complete(StorageSetupScope expected) { throw new UnsupportedOperationException(); }
    }
    static final class Target implements StorageSetupService.DefaultTarget {
        StorageSetupService.ConnectorIdentity identity = new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64));
        int tests;
        @Override public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection master) { return identity; }
        @Override public SourceResource definition(StorageBinding.Connection master, String id) {
            return new SourceResource(id, null, master.kind() == StorageBinding.Connection.Kind.ATLAS ? "mongodb-atlas" : "mongodb", Map.of("isUri", true, "uri", master.uri()), null, null, null, null);
        }
        @Override public void test(StorageBinding.Connection master, SourceResource source, StorageSetupService.ConnectorIdentity original) { tests++; }
    }
    static final class Fixture {
        final Time time = new Time(); final States states = new States(); final Binding binding = new Binding(states); final Target target = new Target();
        final List<AuditRecord> audit = new ArrayList<>();
        Runnable onAudit = () -> {};
        boolean operationAlive = true;
        StorageBinding.Mode mode = StorageBinding.Mode.OP;
        StorageBinding.Connection installedConfiguration = CANDIDATE.configuration();
        StorageConnectionProbe probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection input) { return input; }
            @Override public void test(StorageBinding.Connection input) {}
        };
        StorageSetupService service() {
            return new StorageSetupService(mode, binding, states, probe, target, () -> Optional.ofNullable(installedConfiguration),
                    new AuditGate(record -> { onAudit.run(); audit.add(record); }, time), time, () -> {
                        if (!operationAlive) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
                    });
        }
        Map<StorageSetupService.Layer,String> tested(StorageSetupService service, StorageSetupService.Candidate candidate) {
            var stamps = new EnumMap<StorageSetupService.Layer,String>(StorageSetupService.Layer.class);
            for (var layer : StorageSetupService.Layer.values()) stamps.put(layer, service.test(SCOPE, () -> {}, candidate, layer).testStamp());
            return stamps;
        }
    }
    @Test void cloudConfigurationIsServerOwnedAndNeverRequiredFromTheBrowser() {
        var f = new Fixture(); f.mode = StorageBinding.Mode.CLOUD;
        var service = f.service();
        var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS, "mongodb://user:secret@customer/cache?tls=true", "cache", null);
        var master = new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS, "mongodb://user:secret@customer/master?tls=true", "master", null);
        assertThatCode(() -> service.test(SCOPE, () -> {}, new StorageSetupService.Candidate(null, cache, master), StorageSetupService.Layer.CONFIG))
                .doesNotThrowAnyException();
    }
    @Test void anOpCandidateCanBeTestedBeforeItsConfigurationUriIsInstalled() {
        var f = new Fixture(); f.installedConfiguration = null;
        var service = f.service();
        assertThatCode(() -> service.test(SCOPE, () -> {}, CANDIDATE, StorageSetupService.Layer.CONFIG)).doesNotThrowAnyException();
    }
    @Test void cloudCannotReplaceItsPlatformConfiguration() {
        var f = new Fixture(); f.mode = StorageBinding.Mode.CLOUD;
        assertThatThrownBy(() -> f.service().test(SCOPE, () -> {}, CANDIDATE, StorageSetupService.Layer.CONFIG))
                .isInstanceOf(TapstateException.class).hasMessage(ControlError.STORAGE_SCOPE_CONFLICT.code());
    }
    @Test void opCommitWaitsForTheOriginalConfigurationHandoff() {
        var f = new Fixture(); f.installedConfiguration = null;
        var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        assertThatThrownBy(() -> service.commit("owner", SCOPE, () -> {}, CANDIDATE, stamps))
                .isInstanceOf(TapstateException.class).hasMessage(ControlError.STORAGE_UNAVAILABLE.code());
        assertThat(f.binding.inserts).isZero(); assertThat(f.audit).isEmpty();
        f.installedConfiguration = CANDIDATE.configuration();
        assertThat(service.commit("owner", SCOPE, () -> {}, CANDIDATE, stamps).bindingVersion()).isEqualTo(1);
    }
    @Test void formalLayerTestsPublishOneBindingAndAnExpiredRetryOnlyReadsTheOriginalResult() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        assertThat(f.target.tests).isEqualTo(1);
        assertThat(service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps).setupPhase()).isEqualTo(StorageSetupStateStore.Phase.STORAGE_BOUND);
        f.time.now = f.time.now.plus(Duration.ofDays(1));
        assertThat(service.commit("admin", SCOPE, () -> {}, CANDIDATE, Map.of()).bindingVersion()).isEqualTo(1);
        assertThat(f.binding.inserts).isEqualTo(1);
        assertThat(f.audit).allSatisfy(record -> assertThat(record.operationId()).isEqualTo("storage.setup.commit"));
    }
    @Test void aCommittedFirstCacheInvocationIsIndependentFromExpiringSubmissionStamps() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps);
        var retained = service.pending(SCOPE, () -> {}).orElseThrow().firstCreation().orElseThrow();
        f.time.now = f.time.now.plus(StorageSetupService.TEST_LIFETIME);
        assertThatCode(retained::requireActive).doesNotThrowAnyException();
        f.operationAlive = false;
        assertThatThrownBy(retained::requireActive).isInstanceOf(TapstateException.class);
    }
    @Test void changedMembershipCannotProveThatEqualDatabaseNamesAreIsolated() {
        var f = new Fixture(); var counter = new java.util.concurrent.atomic.AtomicInteger();
        f.probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection input) { return input; }
            @Override public void test(StorageBinding.Connection input) { counter.incrementAndGet(); }
        };
        var service = f.service(); var shared = new StorageSetupService.Candidate(CANDIDATE.configuration(), CANDIDATE.configuration(), CANDIDATE.master());
        assertThatThrownBy(() -> {
            var stamps = f.tested(service, shared);
            service.commit("admin", SCOPE, () -> {}, shared, stamps);
        }).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero();
    }
    @Test void revokedDatabasePermissionsAreCheckedAgainBeforeBindingPublication() {
        var f = new Fixture(); var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
        f.probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection input) { return input; }
            @Override public void test(StorageBinding.Connection input) {
                if (!allowed.get()) throw new TapstateException(ControlError.UNREACHABLE, Map.of(), null);
            }
        };
        var service = f.service(); var stamps = f.tested(service, CANDIDATE); allowed.set(false);
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero(); assertThat(f.audit).isEmpty();
    }
    @Test void changingAnyConnectionInvalidatesAllLayerProofs() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        var changed = new StorageSetupService.Candidate(CANDIDATE.configuration(),
                new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://other:another-secret@host/cache", "cache", null), CANDIDATE.master());
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, changed, stamps)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero(); assertThat(f.audit).isEmpty();
    }
    @Test void changingTheRegisteredArtifactOrSpecInvalidatesAllLayerProofs() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        f.target.identity = new StorageSetupService.ConnectorIdentity("c".repeat(64), "b".repeat(64));
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero();
    }
    @Test void anAuditDelayCannotTurnAnExpiredStampIntoAnAuthorizedWrite() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        f.onAudit = () -> f.time.now = f.time.now.plus(StorageSetupService.TEST_LIFETIME);
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero(); assertThat(f.audit).hasSize(1);
    }
    @Test void theStoreCallbackRechecksTheStampAfterAnInitialAuthorization() {
        var f = new Fixture(); var service = f.service(); var stamps = f.tested(service, CANDIDATE);
        f.binding.beforeCommit = () -> f.time.now = f.time.now.plus(StorageSetupService.TEST_LIFETIME);
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, CANDIDATE, stamps)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero();
    }
    @Test void missingLayerTestAndAnUntrustedConfigurationTargetCannotPublish() {
        var f = new Fixture(); var service = f.service();
        assertThatThrownBy(() -> service.commit("admin", SCOPE, () -> {}, CANDIDATE, Map.of())).isInstanceOf(TapstateException.class);
        var other = new StorageSetupService.Candidate(connection("other_config"), CANDIDATE.cache(), CANDIDATE.master());
        assertThatThrownBy(() -> service.test(SCOPE, () -> {}, other, StorageSetupService.Layer.CONFIG)).isInstanceOf(TapstateException.class);
        assertThat(f.binding.inserts).isZero();
    }
    @Test void aProbeRefusalCarriesOnlyItsLayerAndCanonicalCode() {
        var f = new Fixture();
        f.probe = new StorageConnectionProbe() {
            @Override public StorageBinding.Connection normalize(StorageBinding.Connection input) { return input; }
            @Override public void test(StorageBinding.Connection input) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "sentinel-secret"), null);
            }
        };
        assertThatThrownBy(() -> f.service().test(SCOPE, () -> {}, CANDIDATE, StorageSetupService.Layer.CACHE))
                .isInstanceOfSatisfying(TapstateException.class, refused -> {
                    assertThat(refused.code()).isEqualTo(ControlError.STORAGE_TEST_FAILED);
                    assertThat(refused.args().toString()).doesNotContain("sentinel-secret"); assertThat(refused.getCause()).isNull();
                });
    }
}
