package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoReadBudget;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/** Publishes a formal authentication owner independently from Hazelcast, claims and business readiness. */
final class StorageAuthentication implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(StorageAuthentication.class);
    // Store operations also have a five-second cap, leaving time for transaction abort before HTTP expires.
    static final Duration AUTHORIZATION_READ_BUDGET = Duration.ofSeconds(10);
    private final MongoConfigurationDomain configuration;
    private final CloudRuntimeSettings cloud;
    private final CloudSdkBridge bridge;
    private final TokenSecrets secrets;
    private final Clock clock;
    private final CloudClusterEgressAddressProvider egressAddresses;
    private final byte[] signingKey;
    private final AtomicReference<Epoch> current = new AtomicReference<>();
    private final PendingAuthenticationCohorts<Optional<CredentialAuthentication>> pending;
    private final Bootstrap bootstrap;
    private volatile boolean closed;

    StorageAuthentication(MongoConfigurationDomain configuration, CloudRuntimeSettings cloud, CloudSdkBridge bridge,
            TokenSecrets secrets, Clock clock, byte[] signingKey) {
        this(configuration, cloud, bridge, secrets, clock, signingKey, new PendingAuthenticationCohorts<>());
    }

    StorageAuthentication(MongoConfigurationDomain configuration, CloudRuntimeSettings cloud, CloudSdkBridge bridge,
            TokenSecrets secrets, Clock clock, byte[] signingKey,
            PendingAuthenticationCohorts<Optional<CredentialAuthentication>> pending) {
        this.pending = Objects.requireNonNull(pending);
        this.configuration = configuration; this.cloud = Objects.requireNonNull(cloud);
        this.bridge = bridge; this.secrets = Objects.requireNonNull(secrets); this.clock = Objects.requireNonNull(clock);
        this.egressAddresses = bridge == null ? (organizationId, clusterId) -> Optional.empty()
                : new CachedCloudClusterEgressAddressProvider(bridge, clock);
        this.signingKey = signingKey.clone();
        bootstrap = configuration != null && cloud.cloud() && bridge != null
                && configuration.setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE ? new Bootstrap() : null;
    }

    synchronized void install(StorageRuntime.Bound original) {
        if (closed || configuration == null) throw StorageRuntime.unavailable();
        var existing = current.get();
        if (existing != null) {
            if (existing.bound != original) throw StorageRuntime.unavailable();
            existing.requireActive(); return;
        }
        configuration.requireAuthenticationPublished(original.preparation());
        original.verifyAuthentication();
        Epoch candidate = new Epoch(original);
        original.requireAuthentication();
        if (bootstrap != null) bootstrap.sessions.close();
        current.set(candidate);
    }

    synchronized void invalidateFormal() {
        current.set(null);
        pending.invalidate();
        if (bootstrap != null) bootstrap.sessions.close();
    }

    LocalAuthenticationContext local() {
        return local(new MongoReadBudget(AUTHORIZATION_READ_BUDGET));
    }
    LocalAuthenticationContext local(MongoReadBudget budget) {
        Epoch original = selected();
        if (cloud.cloud()) throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "cloud"), null);
        original.requireActive(budget); return original.local;
    }
    ManagedAuthenticationContext managed() {
        return managed(new MongoReadBudget(AUTHORIZATION_READ_BUDGET));
    }
    ManagedAuthenticationContext managed(MongoReadBudget budget) {
        if (!cloud.cloud()) throw new TapstateException(ControlError.AUTH_MODE_UNAVAILABLE, Map.of("mode", "on_prem"), null);
        if (current.get() == null && bootstrap != null && bootstrap.incomplete()) {
            bootstrap.requireActive(); return new ManagedAuthenticationContext(bootstrap.managed, bootstrap::requireActive);
        }
        Epoch original = selected();
        original.requireActive(budget); return new ManagedAuthenticationContext(original.managed, original::requireActive);
    }
    CredentialAuthenticator credentials() { return CredentialAuthenticator.scoped(this::authenticate); }

    private Optional<CredentialAuthentication> authenticate(String presented) {
        if (current.get() == null && bootstrap != null) return bootstrap.authenticate(presented);
        long deadline = System.nanoTime() + AUTHORIZATION_READ_BUDGET.toNanos();
        var budget = new MongoReadBudget(AUTHORIZATION_READ_BUDGET);
        Epoch original = selected();
        if (cloud.cloud()) return pending.validate(original, presented, deadline,
                () -> authenticate(original, presented, budget), original::requireLocallyActive);
        return authenticate(original, presented, budget);
    }

    private Optional<CredentialAuthentication> authenticate(Epoch original, String presented, MongoReadBudget budget) {
        Optional<VerifiedToken> verified;
        Optional<CredentialEvidence> evidence;
        Optional<String> organization = Optional.empty();
        if (cloud.cloud()) {
            // The fenced session transaction checks CACHE identity and structure before reading.
            // Check CONFIG first, then publish only after the full final CACHE/index validation.
            original.requirePublished(budget);
            try {
                var authenticated = original.managed(budget).authenticateWithContext(presented);
                verified = authenticated.map(AuthenticatedCloudSession::identity);
                organization = authenticated.map(value -> value.clusterContext().organizationId());
            } catch (TapstateException failure) {
                if (failure.code() == IoError.CACHE_AUTH_FENCED || failure.code() == IoError.STORE_UNAVAILABLE)
                    throw StorageRuntime.unavailable();
                original.requireActive(budget);
                throw failure;
            }
            original.requireLocallyActive();
            evidence = verified.map(ignored -> new CredentialEvidence(CredentialType.CLOUD_SESSION,
                    Optional.empty(), original::requireLocallyActive));
        } else {
            var authenticated = original.authenticateTagged(presented, budget);
            verified = authenticated.map(AuthenticatedIdentity::identity);
            evidence = authenticated.map(value -> new CredentialEvidence(value.type(), value.configurationBinding(), () -> {
                original.requireLocallyActive();
                if (value.type() == CredentialType.HUMAN_JWT
                        && original.signer.verify(presented).filter(value.identity()::equals).isEmpty()) throw unauthenticated();
            }, value.humanToken()));
        }
        // A vanished session must not hide a concurrent scope or structure failure as a bad cookie.
        if (cloud.cloud() || verified.isPresent()) original.requireActive(budget);
        if (verified.isEmpty()) return Optional.empty();
        if (cloud.cloud() && organization.isEmpty()) return Optional.empty();
        VerifiedToken identity = verified.orElseThrow();
        return Optional.of(new CredentialAuthentication(identity, Optional.of(original.bound.preparation().authScope()),
                organization, () -> {
                    // Revalidation is a later IO operation, not the lifetime of the original request.
                    long deadline = System.nanoTime() + AUTHORIZATION_READ_BUDGET.toNanos();
                    var revalidation = new MongoReadBudget(AUTHORIZATION_READ_BUDGET);
                    var checked = cloud.cloud()
                            ? pending.validate(original, presented, deadline,
                                    () -> authenticate(original, presented, revalidation), original::requireLocallyActive)
                                    .map(CredentialAuthentication::token)
                            : original.authenticate(presented, revalidation);
                    if (checked.filter(identity::equals).isEmpty()) throw unauthenticated();
                }, evidence));
    }

    /** Converts the original successful verification, including its exact mechanism, into a local alert request. */
    io.tapstate.spi.store.AlertStore.Access alertAccess(CredentialAuthentication original,io.tapstate.spi.store.StorageSetupScope setup) {
        original.requireActive().run(); Epoch epoch=selected();
        if(cloud.cloud()||original.storageScope().filter(epoch.bound.preparation().authScope()::equals).isEmpty())throw unauthenticated();
        var evidence=original.evidence().orElseThrow(StorageAuthentication::unauthenticated);
        var identity=original.token();io.tapstate.core.alerting.AlertTestAuthority actor;
        if(evidence.type()==CredentialType.MACHINE_TOKEN)actor=new io.tapstate.core.alerting.AlertTestAuthority(
                io.tapstate.core.alerting.AlertTestAuthority.Mechanism.MACHINE_TOKEN,setup.installationId(),identity.subject(),identity.scope().name(),
                evidence.configurationTokenBinding().orElseThrow(StorageAuthentication::unauthenticated),null);
        else if(evidence.type()==CredentialType.HUMAN_JWT){var claims=evidence.humanToken().orElseThrow(StorageAuthentication::unauthenticated);
            actor=new io.tapstate.core.alerting.AlertTestAuthority(io.tapstate.core.alerting.AlertTestAuthority.Mechanism.HUMAN_JWT,
                    setup.installationId(),identity.subject(),identity.scope().name(),claims.verifierBinding(),claims.expiresAt());}
        else throw unauthenticated();
        return new io.tapstate.spi.store.AlertStore.Access(actor,original.requireActive(),()->{epoch.requireLocallyActive();evidence.requireLocallyActive().run();},
                ()->((HmacTokenSigner)epoch.signer).verifierBinding());
    }

    /** Reopens only a persisted original test authority; CONFIG still checks its token/user and original permission. */
    io.tapstate.spi.store.AlertStore.Access continueAlertTest(io.tapstate.core.alerting.AlertTestAuthority original) {
        Epoch epoch=selected();if(cloud.cloud())throw unauthenticated();
        return new io.tapstate.spi.store.AlertStore.Access(original,epoch::requireActive,epoch::requireLocallyActive,
                ()->((HmacTokenSigner)epoch.signer).verifierBinding());
    }

    private final class Bootstrap {
        final StorageSetupScope setup = configuration.setupStates().load().scope();
        final BootstrapCloudSessions sessions;
        final CloudAuthenticationService managed;
        Bootstrap() {
            var identity = new CloudSessionIdentity(cloud.baseUrl().toString(), CloudSdkBridge.DEPLOYMENT_ORGANIZATION, cloud.clusterId());
            sessions = new BootstrapCloudSessions(identity, this::requireActive);
            var observer = CloudRuntimeConfiguration.authenticationObserver();
            managed = new CloudAuthenticationService(bridge, bridge,
                    new CloudSessionService(sessions, identity, secrets, clock, observer), observer, egressAddresses);
        }
        boolean incomplete() {
            try { return configuration.setupStates().load().phase() != StorageSetupStateStore.Phase.COMPLETE
                    && configuration.bindings().cachePreparation().map(value -> value.authScope().authGeneration() == 1).orElse(true); }
            catch (TapstateException unavailable) { throw StorageRuntime.unavailable(); }
        }
        void requireActive() {
            if (closed || current.get() != null) throw unauthenticated();
            var actual = configuration.setupStates().load();
            if (!actual.scope().equals(setup) || !cloud.clusterId().equals(setup.clusterId())
                    || !incomplete()) throw unauthenticated();
        }
        Optional<CredentialAuthentication> authenticate(String credential) {
            requireActive();
            var verified = managed.authenticate(credential);
            if (verified.isEmpty()) return Optional.empty();
            var organization = managed.clusterContext(credential).map(CloudSessionContext::organizationId);
            if (organization.isEmpty()) return Optional.empty();
            var identity = verified.orElseThrow(); requireActive();
            return Optional.of(new CredentialAuthentication(identity, Optional.empty(), organization, () -> {
                requireActive();
                if (managed.authenticate(credential).filter(identity::equals).isEmpty()) throw unauthenticated();
                requireActive();
            }));
        }
    }

    private Epoch selected() {
        if (closed) throw StorageRuntime.unavailable();
        Epoch selected = current.get(); if (selected == null) throw StorageRuntime.unavailable(); return selected;
    }

    private record AuthenticatedIdentity(VerifiedToken identity, CredentialType type, Optional<String> configurationBinding,
            Optional<io.tapstate.control.core.HumanTokenEvidence> humanToken) {
        AuthenticatedIdentity(VerifiedToken identity, CredentialType type, Optional<String> binding) { this(identity,type,binding,Optional.empty()); }
    }

    private final class Epoch {
        final StorageRuntime.Bound bound;
        final LocalAuthenticationContext local;
        final CloudAuthenticationService managed;
        final TokenSigner signer;
        Epoch(StorageRuntime.Bound bound) {
            this.bound = bound;
            var scope = bound.preparation().authScope();
            if (cloud.cloud() != (scope.mode() == StorageBinding.Mode.CLOUD)) throw StorageRuntime.unavailable();
            if (cloud.cloud()) {
                if (bridge == null || !cloud.clusterId().equals(scope.clusterId())) throw StorageRuntime.unavailable();
                local = null; signer = null;
                var identity = new CloudSessionIdentity(cloud.baseUrl().toString(), CloudSdkBridge.DEPLOYMENT_ORGANIZATION, cloud.clusterId());
                var observer = CloudRuntimeConfiguration.authenticationObserver();
                managed = new CloudAuthenticationService(bridge, bridge,
                        new CloudSessionService(bound.cache().cloudSessions(scope), identity, secrets, clock, observer), observer, egressAddresses);
            } else {
                managed = null;
                signer = new HmacTokenSigner(signingKey, HmacTokenSigner.DEFAULT_TTL, clock, scope);
                var access = new AccessTokenService(signer, clock);
                local = new LocalAuthenticationContext(new LoginService(configuration.users(), new BCryptPasswordHasher(), access),
                        new SessionService(bound.cache().sessions(scope), secrets, access, clock),
                        new ClusterIdentityService(configuration.clusterIdentity()), this::requireActive);
            }
        }
        CloudAuthenticationService managed(MongoReadBudget budget) {
            var observer = CloudRuntimeConfiguration.authenticationObserver();
            return new CloudAuthenticationService(bridge, bridge,
                    new CloudSessionService(bound.cache().cloudSessions(bound.preparation().authScope(), budget),
                            managed.identity(), secrets, clock, observer), observer, egressAddresses);
        }
        Optional<VerifiedToken> authenticate(String presented, MongoReadBudget budget) {
            return authenticateTagged(presented, budget).map(AuthenticatedIdentity::identity);
        }
        Optional<AuthenticatedIdentity> authenticateTagged(String presented, MongoReadBudget budget) {
            requireActive(budget);
            Optional<AuthenticatedIdentity> result = cloud.cloud() ? managed(budget).authenticate(presented)
                    .map(identity -> new AuthenticatedIdentity(identity, CredentialType.CLOUD_SESSION, Optional.empty()))
                    : TokenService.isMachineToken(presented)
                            ? new TokenService(configuration.tokens(budget), secrets, clock).authenticateBound(presented)
                                    .map(value -> new AuthenticatedIdentity(value.identity(), CredentialType.MACHINE_TOKEN, Optional.of(value.configurationBinding())))
                            : signer.verifyDetailed(presented).map(value -> new AuthenticatedIdentity(value.identity(), CredentialType.HUMAN_JWT, Optional.empty(), Optional.of(value.continuation())));
            requireActive(budget); return result;
        }
        void requireActive() { requireActive(new MongoReadBudget(AUTHORIZATION_READ_BUDGET)); }
        void requireActive(MongoReadBudget budget) {
            requirePublished(budget);
            requireLocallyActive();
            try {
                bound.cache().loadAuthScope(bound.preparation().authScope(), budget);
            }
            catch (TapstateException unavailable) {
                LOG.warn("Storage authentication check failed [stage=cache-authentication-scope, error_code={}]", unavailable.code().code());
                throw StorageRuntime.unavailable();
            }
            requireLocallyActive();
        }
        void requirePublished(MongoReadBudget budget) {
            requireLocallyActive();
            try {
                configuration.requireAuthenticationPublished(bound.preparation(), budget);
            }
            catch (TapstateException unavailable) {
                LOG.warn("Storage authentication check failed [stage=configuration-publication, error_code={}]", unavailable.code().code());
                throw StorageRuntime.unavailable();
            }
            requireLocallyActive();
        }
        void requireLocallyActive() {
            bound.requireAuthentication();
            if (closed || current.get() != this) throw unauthenticated();
        }
    }

    @Override public synchronized void close() {
        closed = true; current.set(null);
        pending.close();
        if (bootstrap != null) bootstrap.sessions.close();
        java.util.Arrays.fill(signingKey, (byte)0);
    }
    private static TapstateException unauthenticated() { return new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null); }
}
