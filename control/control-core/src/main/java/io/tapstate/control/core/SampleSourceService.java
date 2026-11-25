package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Installs selected demonstration databases only when a caller chooses them. */
public final class SampleSourceService {
    public record Descriptor(String id, String name, String description, String connector) { }
    public record Installation(List<SourceView> added, List<String> existing) { }

    private static final List<Descriptor> DESCRIPTORS = List.of(
            new Descriptor("sample_core_banking", "TPC-C MySQL", "Customer, order and order-line tables", "mysql"),
            new Descriptor("sample_cards_crm", "TPC-C PostgreSQL", "Customer, order and order-line tables", "postgres"));

    private final SourceProjectionService sources;
    private final SampleSourceCredentialsProvider credentialsProvider;
    private final SchemaDiscoveryService discovery;
    private final ConnectionTestService connections;
    private final ConnectorCatalogView connectors;

    public SampleSourceService(SourceProjectionService sources, SchemaDiscoveryService discovery,
            ConnectionTestService connections, ConnectorCatalogView connectors,
            SampleSourceCredentialsProvider credentialsProvider) {
        this.sources = Objects.requireNonNull(sources, "sources");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.connections = Objects.requireNonNull(connections, "connections");
        this.connectors = Objects.requireNonNull(connectors, "connectors");
        this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
    }

    public List<Descriptor> available() {
        return catalog().stream().filter(SampleSourceCredentialsProvider.Definition::available)
                .map(sample -> new Descriptor(sample.id(), sample.name(), sample.description(), sample.connector()))
                .toList();
    }

    /** Catalog entries include unavailable future samples without exposing connection settings. */
    public List<SampleSourceCredentialsProvider.Definition> catalog() {
        List<String> registered = connectors.summaries().stream().map(ConnectorSummary::id).toList();
        return credentialsProvider.catalog().stream()
                .map(item -> new SampleSourceCredentialsProvider.Definition(item.id(), item.name(),
                        item.description(), item.connector(), item.available() && registered.contains(item.connector()),
                        item.guidedDemo(), item.rootTable(), item.orderLineTable(), item.customerTable()))
                .toList();
    }

    /** Installs one selected sample; the observer reports actual completed boundaries. */
    public Installation installSelected(String principal, String id, BiConsumer<String, String> progress) {
        SampleSourceCredentialsProvider.Definition definition = catalog().stream()
                .filter(item -> item.id().equals(id) && item.available()).findFirst()
                .orElseThrow(() -> new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "sample source is unavailable"), null));
        Map<String, Object> config = credentialsProvider.settingsFor(id);
        progress.accept(id, "TESTING");
        ConnectionTestReport report = connections.test(id, definition.connector(), config, principal);
        if (report.outcome() != ConnectionTestReport.Outcome.PASSED) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample database connection failed: " + id), null);
        }
        SourceView present = sources.list().stream().filter(source -> source.id().equals(id)).findFirst().orElse(null);
        progress.accept(id, "CREATING");
        SourceView created = null;
        if (present != null) {
            if (present.metadata() == null || !"true".equals(present.metadata().labels().get("sample"))
                    || !definition.connector().equals(present.connector())) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "sample Source id is already used by another connection"), null);
            }
            Map<String, Object> refreshedConfig = new LinkedHashMap<>(present.config());
            refreshedConfig.putAll(config);
            List<SourceTableDraft> tables = present.tables() == null ? null : present.tables().stream()
                    .map(table -> new SourceTableDraft(table.type(), table.name(), table.pattern(),
                            table.filter(), table.pk(), table.options()))
                    .toList();
            SourceInput refreshed = new SourceInput(id, present.metadata(), definition.connector(),
                    refreshedConfig, present.mode(), tables, present.options(), present.srs(),
                    present.experimental(), List.of());
            sources.replace(principal, id, present.contentHash(), refreshed);
        } else {
            SourceInput input = new SourceInput(id, new Metadata(Map.of("sample", "true"), definition.name()),
                    definition.connector(), config, "snapshot", null, null, null, null, null);
            created = sources.create(principal, input);
        }
        progress.accept(id, "DISCOVERING");
        discovery.discover(id, definition.connector(), config, principal);
        progress.accept(id, "READY");
        return new Installation(created == null ? List.of() : List.of(created),
                created == null ? List.of(id) : List.of());
    }

    public Installation install(String principal) {
        if (available().isEmpty()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample database credentials are not configured on the server"), null);
        }
        SampleSourceCredentialsProvider.Credentials credentials = credentialsProvider.fetch();
        List<SourceView> added = new ArrayList<>();
        List<String> existing = new ArrayList<>();
        // Check both before writing either Source so an unavailable connector cannot leave a half-installed sample.
        for (Descriptor descriptor : DESCRIPTORS) {
            Map<String, Object> config = settings(descriptor, credentials);
            ConnectionTestReport report = connections.test(
                    descriptor.id(), descriptor.connector(), config, principal);
            if (report.outcome() != ConnectionTestReport.Outcome.PASSED) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "sample database connection failed: " + descriptor.id()), null);
            }
        }
        for (Descriptor descriptor : DESCRIPTORS) {
            Map<String, Object> config = settings(descriptor, credentials);
            SourceView present = sources.list().stream()
                    .filter(source -> source.id().equals(descriptor.id())).findFirst().orElse(null);
            if (present != null) {
                if (present.metadata() == null
                        || !"true".equals(present.metadata().labels().get("sample"))
                        || !descriptor.connector().equals(present.connector())) {
                    throw new TapstateException(ControlError.MALFORMED_REQUEST,
                            Map.of("reason", "sample Source id is already used by another connection"), null);
                }
                Map<String, Object> refreshedConfig = new LinkedHashMap<>(present.config());
                refreshedConfig.putAll(config);
                List<SourceTableDraft> tables = present.tables() == null ? null : present.tables().stream()
                        .map(table -> new SourceTableDraft(table.type(), table.name(), table.pattern(),
                                table.filter(), table.pk(), table.options()))
                        .toList();
                SourceInput refreshed = new SourceInput(present.id(), present.metadata(), present.connector(),
                        refreshedConfig, present.mode(), tables, present.options(), present.srs(),
                        present.experimental(), List.of());
                sources.replace(principal, descriptor.id(), present.contentHash(), refreshed);
                existing.add(descriptor.id());
                discovery.discover(descriptor.id(), descriptor.connector(), config, principal);
                continue;
            }
            SourceInput input = new SourceInput(descriptor.id(),
                    new Metadata(Map.of("sample", "true"), descriptor.name()),
                    descriptor.connector(), config, "snapshot", null, null, null, null, null);
            SourceView created = sources.create(principal, input);
            added.add(created);
            discovery.discover(descriptor.id(), descriptor.connector(), config, principal);
        }
        return new Installation(List.copyOf(added), List.copyOf(existing));
    }

    private Map<String, Object> settings(
            Descriptor descriptor, SampleSourceCredentialsProvider.Credentials credentials) {
        return descriptor.connector().equals("mysql")
                ? Map.of("host", credentials.host(), "port", 33306, "database", "tpcc_331",
                        "username", "root", "password", credentials.password())
                : Map.of("host", credentials.host(), "port", 55433, "database", "postgres", "schema", "public",
                        "user", "root", "password", credentials.password(),
                        "globalPublicationName", "tapstate_sample_pub");
    }
}
