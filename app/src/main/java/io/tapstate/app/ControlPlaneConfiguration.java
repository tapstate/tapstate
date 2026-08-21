package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkCapabilityDeriver;
import io.tapstate.adapters.pdk.PdkConnectionTester;
import io.tapstate.adapters.pdk.PdkDataBrowser;
import io.tapstate.adapters.pdk.PdkSchemaDiscoverer;
import io.tapstate.adapters.pdk.RegistryConnectorProvisioner;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.NestSizingAdvisories;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.AuditedSourceService;
import io.tapstate.control.core.BootstrapService;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ConnectorConfigValidator;
import io.tapstate.control.core.ConnectorRegisterService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.LoginService;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.PasswordHasher;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineLogQueryService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.DataBrowserFollows;
import io.tapstate.control.core.SourceDraftService;
import org.springframework.beans.factory.ObjectProvider;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.SourceService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.DataBrowserCollectionsProbe;
import io.tapstate.runtime.probe.DataBrowserFindProbe;
import io.tapstate.runtime.probe.DataBrowserStatsProbe;
import io.tapstate.runtime.probe.DelegatingConnectionProbe;
import io.tapstate.runtime.probe.DelegatingDataBrowserCollectionsProbe;
import io.tapstate.runtime.probe.DelegatingDataBrowserFindProbe;
import io.tapstate.runtime.probe.DelegatingDataBrowserStatsProbe;
import io.tapstate.runtime.probe.DelegatingDataBrowserTailProbe;
import io.tapstate.runtime.probe.DataBrowserTailProbe;
import io.tapstate.runtime.probe.DelegatingSchemaDiscoveryProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.DataBrowser;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.SchemaDiscoverer;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.TokenStore;
import io.tapstate.spi.store.UserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Clock;

/**
 * Wires the control plane into the assembly root: the authentication ports over the store, the control-core
 * services, and the HTTP control face ({@link ControlHttpFace}) that projects the verbs onto an
 * authenticated {@code /api} surface. This is the driver-free service graph the running server exposes
 * under {@code --role=all}.
 *
 * <p>Gated, like the store it stands on, on {@code tapstate.store.mongo.enabled}: the control plane persists
 * to the store, so a run with no store (a substrate check) brings up neither the store nor the control
 * plane. Because the whole face — controllers and the interceptor that guards them — is imported together
 * through {@link ControlHttpFace}, there is no state in which the verb surface is served without the guard:
 * either the store is present and the entire authenticated face comes up, or it is absent and none of it
 * does.
 */
@Configuration
@ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties({ControlAuthProperties.class, ConnectorPluginProperties.class})
@Import(ControlHttpFace.class)
class ControlPlaneConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ControlPlaneConfiguration.class);

    /** The number of random bytes in an ephemeral signing secret when none is configured. */
    private static final int EPHEMERAL_SECRET_BYTES = 32;

    /** The node-local log tail bounds: how many pipelines to retain lines for, and how many lines each. */
    private static final int MAX_LOGGED_PIPELINES = 64;
    private static final int MAX_LOG_LINES_PER_PIPELINE = 200;

    // ---- authentication ports over the store (the counterpart to StoreConfiguration's StorePort) ----

    @Bean
    MongoAuthStores authStores(MongoConnection storeConnection) {
        return new MongoAuthStores(storeConnection);
    }

    @Bean
    UserStore userStore(MongoAuthStores authStores) {
        return authStores.users();
    }

    @Bean
    TokenStore tokenStore(MongoAuthStores authStores) {
        return authStores.tokens();
    }

    @Bean
    AuditStore auditStore(MongoAuthStores authStores) {
        return authStores.audit();
    }

    @Bean
    ClusterIdentityStore clusterIdentityStore(MongoAuthStores authStores) {
        return authStores.clusterIdentity();
    }

    @Bean
    ClusterIdentityService clusterIdentityService(ClusterIdentityStore store) {
        return new ClusterIdentityService(store);
    }

    // ---- the framework-free primitives bound to their control-ring ports ----

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    PasswordHasher passwordHasher() {
        return new BCryptPasswordHasher();
    }

    @Bean
    TokenSecrets tokenSecrets() {
        return new RandomTokenSecrets();
    }

    @Bean
    TokenSigner tokenSigner(ControlAuthProperties properties, Clock clock) {
        return new HmacTokenSigner(resolveSigningSecret(properties), HmacTokenSigner.DEFAULT_TTL, clock);
    }

    // ---- the control-core services (stateless, composed over the ports above) ----

    @Bean
    OperationRegistry operationRegistry() {
        return ControlOperations.registry();
    }

    @Bean
    TokenService tokenService(TokenStore tokenStore, TokenSecrets tokenSecrets, Clock clock) {
        return new TokenService(tokenStore, tokenSecrets, clock);
    }

    @Bean
    LoginService loginService(UserStore userStore, PasswordHasher passwordHasher, TokenSigner tokenSigner) {
        return new LoginService(userStore, passwordHasher, tokenSigner);
    }

    @Bean
    AuditGate auditGate(AuditStore auditStore, Clock clock) {
        return new AuditGate(auditStore, clock);
    }

    @Bean
    BootstrapService bootstrapService(UserStore userStore, PasswordHasher passwordHasher, AuditGate auditGate) {
        return new BootstrapService(userStore, passwordHasher, auditGate);
    }

    @Bean
    CredentialAuthenticator credentialAuthenticator(TokenService tokenService, TokenSigner tokenSigner) {
        return new CredentialAuthenticator(tokenService, tokenSigner);
    }

    @Bean
    ApplyService applyService(
            ArtifactStore artifactStore, ConnectorCatalogView connectorCatalogView, AuditGate auditGate,
            SchemaStore schemaStore, @Nullable NestSettings nestSettings) {
        // The online apply validates against the live catalog view (the bundled snapshot union the
        // connectors registered so far), so a connector registered at runtime is honoured without a restart.
        // It also reads the schema store, which is what lets it judge a row expression against the columns
        // its sources were discovered to hold - the one check that cannot run offline.
        // The advisory pass sizes each nest against the budget a pipeline that writes none of its own
        // would run on. Passing the running number rather than a copy is what keeps a deployment that
        // changed it from being advised against the number it used to have.
        // The settings are nullable because the control plane is assembled on its own in deployments
        // that run no engine, and the bean carrying them belongs to the engine's substrate. Sizing
        // still happens there, against the built-in default - which is the number a nest would run on
        // wherever it does run, absent a deployment saying otherwise. Requiring the bean instead would
        // take the whole control plane down in exactly the shape that never nests anything locally.
        NestSettings settings = nestSettings == null ? NestSettings.defaults() : nestSettings;
        return new ApplyService(connectorCatalogView::merged, artifactStore, auditGate, schemaStore,
                new NestSizingAdvisories(settings.entriesHeldInMemory()));
    }

    @Bean
    ArtifactQueryService artifactQueryService(ArtifactStore artifactStore) {
        return new ArtifactQueryService(artifactStore);
    }

    @Bean
    ArtifactMutationService artifactMutationService(
            ArtifactStore artifactStore, StorePort storePort, AuditGate auditGate,
            ObjectProvider<DataBrowserFollows> follows) {
        // The removal takes the same artifact store bean apply writes through, so both paths see one
        // view of a resource. The dependent bookkeeping a removed pipeline owns is reclaimed straight
        // off the store port: those facets have no service in front of them.
        return new ArtifactMutationService(
                artifactStore, storePort.desired(), storePort.state(), storePort.observations(),
                storePort.meta(), auditGate, follows.getIfAvailable(() -> DataBrowserFollows.NONE));
    }

    @Bean
    SourceDraftService sourceDraftService(ConnectorCatalogView connectorCatalogView) {
        return new SourceDraftService(connectorCatalogView::merged);
    }

    // ---- the connector plane: the R5 synchronous connection-test verb, wired end to end ----
    // control-core service -> runtime probe -> adapter-pdk tester -> provisioner -> connector registry.
    // The PDK types stay inside the adapter-pdk beans; the runtime and control rings see only ports.

    @Bean
    ConnectorRegistry connectorRegistry(StorePort storePort) {
        return storePort.connectors();
    }

    @Bean
    ConnectorCatalogStore connectorCatalogStore(StorePort storePort) {
        return storePort.connectorCatalog();
    }

    @Bean
    ConnectorSpecStore connectorSpecStore(StorePort storePort) {
        return storePort.connectorSpecs();
    }

    @Bean
    CapabilityDeriver capabilityDeriver(ConnectorProvisioner provisioner) {
        return new PdkCapabilityDeriver(provisioner);
    }

    @Bean
    ConnectorCatalogView connectorCatalogView(
            ConnectorCatalogStore connectorCatalogStore, ConnectorSpecStore connectorSpecStore,
            ConnectorRegistry connectorRegistry) {
        // The online catalog view = the bundled snapshot overlaid with the rows derived for registered
        // connectors, read live; the offline native CLI keeps reading only the bundled snapshot.
        return new ConnectorCatalogView(
                TapstateCatalog.load(), connectorCatalogStore, connectorSpecStore, connectorRegistry);
    }

    @Bean
    ConnectionTestResultStore connectionTestResultStore(StorePort storePort) {
        return storePort.connectionTestResults();
    }

    @Bean
    ConnectorIntrospector connectorIntrospector() {
        return new ConnectorIntrospector();
    }

    @Bean
    ConnectorProvisioner connectorProvisioner(
            ConnectorRegistry registry, ConnectorIntrospector introspector, ConnectorPluginProperties properties) {
        return new RegistryConnectorProvisioner(registry, introspector, properties.getPluginsDir());
    }

    // The startup seed sweep: the release's connectors/ directory goes through the same
    // register-if-absent path an explicit register uses, so restarts and concurrent nodes are harmless.

    @Bean
    ConnectorArtifactRegistrar connectorArtifactRegistrar(
            ConnectorRegistry registry, ConnectorIntrospector introspector,
            CapabilityDeriver capabilityDeriver, ConnectorCatalogStore connectorCatalogStore,
            ConnectorSpecStore connectorSpecStore, ConnectorPluginProperties properties) {
        return new ConnectorArtifactRegistrar(
                registry, introspector, capabilityDeriver, connectorCatalogStore, connectorSpecStore,
                properties.getAlsoAcceptIds());
    }

    @Bean
    SeedConnectorSweep seedConnectorSweep(ConnectorArtifactRegistrar registrar) {
        return new SeedConnectorSweep(registrar);
    }

    @Bean
    SeedSweepRunner seedSweepRunner(SeedConnectorSweep sweep, ConnectorPluginProperties properties) {
        return new SeedSweepRunner(sweep, properties.getSeedDir());
    }

    @Bean
    ViewStoreSeedRunner viewStoreSeedRunner(ArtifactStore artifactStore, MongoProperties mongoProperties) {
        // The managed ArtifactStore, not the raw one behind it. Reaching past the decorator would make
        // this the one write in the process that skips secret tracking -- and the resource it writes is
        // built from the deployment's own store URI, which is the last one that should be the exception.
        // It changes nothing observable while the mongodb catalog marks `uri` non-secret; what it
        // removes is a seam where a later change to that marking would silently not apply here.
        return new ViewStoreSeedRunner(artifactStore, mongoProperties.getUri(), mongoProperties.getTlsCaFile());
    }

    @Bean
    ConnectorRegisterService connectorRegisterService(ConnectorArtifactRegistrar registrar, AuditGate auditGate) {
        // The register verb reaches the distribution store through the same registrar the seed sweep uses; it
        // implements the spi ingestion port, so control-core drives it without depending on the adapters ring.
        return new ConnectorRegisterService(registrar, auditGate);
    }

    @Bean
    ConnectionTester connectionTester(ConnectorProvisioner provisioner, Clock clock) {
        return new PdkConnectionTester(provisioner, clock);
    }

    @Bean
    ConnectionProbe connectionProbe(ConnectionTester tester) {
        return new DelegatingConnectionProbe(tester);
    }

    @Bean
    ConnectorConfigValidator connectorConfigValidator(ConnectorCatalogView connectorCatalogView) {
        return new ConnectorConfigValidator(connectorCatalogView::merged);
    }

    @Bean
    ConnectionTestService connectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate,
            ConnectorConfigValidator configValidator) {
        return new ConnectionTestService(probe, resultStore, auditGate, configValidator);
    }

    @Bean
    ConnectionTestResultQueryService connectionTestResultQueryService(ConnectionTestResultStore resultStore) {
        return new ConnectionTestResultQueryService(resultStore);
    }

    // The schema-discovery half of the connection plane: the same provisioner feeds the PDK discoverer,
    // injected into the runtime seam; discovered models persist through the schema store.

    @Bean
    SchemaStore schemaStore(StorePort storePort) {
        return storePort.schemas();
    }

    @Bean
    SchemaDiscoverer schemaDiscoverer(ConnectorProvisioner provisioner) {
        return new PdkSchemaDiscoverer(provisioner);
    }

    @Bean
    SchemaDiscoveryProbe schemaDiscoveryProbe(SchemaDiscoverer discoverer) {
        return new DelegatingSchemaDiscoveryProbe(discoverer);
    }

    @Bean
    SchemaDiscoveryService schemaDiscoveryService(
            SchemaDiscoveryProbe probe, SchemaStore schemaStore, AuditGate auditGate, Clock clock,
            ConnectorConfigValidator configValidator) {
        return new SchemaDiscoveryService(probe, schemaStore, auditGate, clock, configValidator);
    }

    @Bean
    SchemaQueryService schemaQueryService(SchemaStore schemaStore) {
        return new SchemaQueryService(schemaStore);
    }

    // The read face over a declared source's own database. Unlike the two connection probes, the browser
    // holds live state between calls — a pool of initialized connector instances — so the assembly root
    // owns its shutdown; the three probes share that one browser rather than holding one each, which is
    // also why only the browser bean closes.

    @Bean(destroyMethod = "close")
    DataBrowser dataBrowser(ConnectorProvisioner provisioner) {
        return new PdkDataBrowser(provisioner);
    }

    @Bean
    DataBrowserCollectionsProbe dataBrowserCollectionsProbe(DataBrowser browser) {
        return new DelegatingDataBrowserCollectionsProbe(browser);
    }

    @Bean
    DataBrowserStatsProbe dataBrowserStatsProbe(DataBrowser browser) {
        return new DelegatingDataBrowserStatsProbe(browser);
    }

    @Bean
    DataBrowserFindProbe dataBrowserFindProbe(DataBrowser browser) {
        return new DelegatingDataBrowserFindProbe(browser);
    }

    @Bean
    DataBrowserTailProbe dataBrowserTailProbe(DataBrowser browser) {
        return new DelegatingDataBrowserTailProbe(browser);
    }

    @Bean
    DataBrowserService dataBrowserService(
            ArtifactStore artifactStore, SchemaStore schemaStore,
            DataBrowserCollectionsProbe collectionsProbe,
            DataBrowserStatsProbe statsProbe, DataBrowserFindProbe findProbe,
            DataBrowserTailProbe tailProbe) {
        // The schema store is read, never written, from here: a listing reports what the last discovery
        // found and never runs one, which would turn a read into an audited write.
        return new DataBrowserService(
                artifactStore, schemaStore, collectionsProbe, statsProbe, findProbe, tailProbe);
    }

    @Bean
    PipelineLifecycleService pipelineLifecycleService(
            ArtifactQueryService artifactQueryService, StorePort storePort, AuditGate auditGate) {
        return new PipelineLifecycleService(artifactQueryService, storePort.desired(), auditGate);
    }

    @Bean
    PipelineObservationQueryService pipelineObservationQueryService(
            ArtifactQueryService artifactQueryService, StorePort storePort) {
        return new PipelineObservationQueryService(artifactQueryService, storePort.observations());
    }

    // ---- the node-local log tail: the sink, the appender that feeds it, and the read face over it ----

    /**
     * The one in-process log sink for this node. It is node-local, not store-backed: logs are not fanned
     * into the shared store like the other observation reads, so this bean is both fed (by the appender) and
     * read (by the logs read face) in-process.
     */
    @Bean
    LogSink logSink() {
        return new RingBufferLogSink(MAX_LOGGED_PIPELINES, MAX_LOG_LINES_PER_PIPELINE);
    }

    @Bean
    SecretRedactor secretRedactor() {
        return new SecretRedactor();
    }

    @Bean
    ArtifactStore artifactStore(
            StorePort storePort, ConnectorCatalogView connectorCatalogView, SecretRedactor secretRedactor) {
        return new SecretTrackingArtifactStore(
                storePort.artifacts(), connectorCatalogView::merged, secretRedactor);
    }

    /** Attaches the pipeline log appender to the logging backend so the sink is fed; detaches on shutdown. */
    @Bean
    PipelineLogCapture pipelineLogCapture(LogSink logSink, SecretRedactor secretRedactor) {
        return new PipelineLogCapture(logSink, secretRedactor);
    }

    @Bean
    PipelineLogQueryService pipelineLogQueryService(LogSink logSink) {
        return new PipelineLogQueryService(logSink);
    }

    @Bean
    SourceRepresentation sourceRepresentation(ConnectorCatalogView connectorCatalogView) {
        return new SourceRepresentation(connectorCatalogView::merged);
    }

    @Bean
    SourceService sourceService(
            ConnectorCatalogView connectorCatalogView, ArtifactStore artifactStore,
            SourceRepresentation representation, ObjectProvider<DataBrowserFollows> follows) {
        // Resolved through a provider rather than injected directly: the streaming face is
        // servlet-only, and a control plane assembled without one still deletes sources -- it
        // simply has no follows to stop. Asked for at call time so it cannot depend on which
        // configuration Spring happens to process first.
        return new SourceService(connectorCatalogView::merged, artifactStore, representation,
                follows.getIfAvailable(() -> DataBrowserFollows.NONE));
    }

    @Bean
    AuditedSourceService auditedSourceService(SourceService sourceService, AuditGate auditGate) {
        return new AuditedSourceService(sourceService, auditGate);
    }

    /**
     * The signing secret from configuration, or a fresh random one when none is set. An unset secret is a
     * working single-node default, not an error — but session tokens then do not outlive a restart nor
     * cross nodes, so a warning names the trade-off.
     */
    private static byte[] resolveSigningSecret(ControlAuthProperties properties) {
        String configured = properties.getJwtSecret();
        if (configured != null && !configured.isBlank()) {
            return configured.getBytes(StandardCharsets.UTF_8);
        }
        byte[] ephemeral = new byte[EPHEMERAL_SECRET_BYTES];
        new SecureRandom().nextBytes(ephemeral);
        LOG.warn("No tapstate.control.auth.jwt-secret is configured; signing session tokens with an ephemeral "
                + "secret. Tokens will not survive a restart or work across nodes -- set a secret for a "
                + "restart-stable or multi-node deployment.");
        return ephemeral;
    }
}
