package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Installs the two configured demonstration databases only when a caller chooses the sample. */
public final class SampleSourceService {
    public record Descriptor(String id, String name, String description, String connector) { }
    public record Installation(List<SourceView> added, List<String> existing) { }

    private static final List<Descriptor> DESCRIPTORS = List.of(
            new Descriptor("sample_core_banking", "TPC-C MySQL", "Customer, order and order-line tables", "mysql"),
            new Descriptor("sample_cards_crm", "TPC-C PostgreSQL", "Customer, order and order-line tables", "postgres"));

    private final SourceProjectionService sources;
    private final String host;
    private final String password;
    private final SchemaDiscoveryService discovery;
    private final ConnectionTestService connections;
    private final ConnectorCatalogView connectors;

    public SampleSourceService(SourceProjectionService sources, SchemaDiscoveryService discovery,
            ConnectionTestService connections, ConnectorCatalogView connectors, String host, String password) {
        this.sources = Objects.requireNonNull(sources, "sources");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.connections = Objects.requireNonNull(connections, "connections");
        this.connectors = Objects.requireNonNull(connectors, "connectors");
        this.host = Objects.requireNonNull(host, "host");
        this.password = Objects.requireNonNull(password, "password");
    }

    public List<Descriptor> available() {
        if (password.isBlank()) return List.of();
        List<String> registered = connectors.summaries().stream().map(ConnectorSummary::id).toList();
        return DESCRIPTORS.stream().allMatch(sample -> registered.contains(sample.connector()))
                ? DESCRIPTORS : List.of();
    }

    public Installation install(String principal) {
        if (available().isEmpty()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample database credentials are not configured on the server"), null);
        }
        List<SourceView> added = new ArrayList<>();
        List<String> existing = new ArrayList<>();
        // Check both before writing either Source so an unavailable connector cannot leave a half-installed sample.
        for (Descriptor descriptor : DESCRIPTORS) {
            Map<String, Object> config = settings(descriptor);
            ConnectionTestReport report = connections.test(
                    descriptor.id(), descriptor.connector(), config, principal);
            if (report.outcome() != ConnectionTestReport.Outcome.PASSED) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "sample database connection failed: " + descriptor.id()), null);
            }
        }
        for (Descriptor descriptor : DESCRIPTORS) {
            Map<String, Object> config = settings(descriptor);
            SourceView present = sources.list().stream()
                    .filter(source -> source.id().equals(descriptor.id())).findFirst().orElse(null);
            if (present != null) {
                if (present.metadata() == null
                        || !"true".equals(present.metadata().labels().get("sample"))
                        || !descriptor.connector().equals(present.connector())) {
                    throw new TapstateException(ControlError.MALFORMED_REQUEST,
                            Map.of("reason", "sample Source id is already used by another connection"), null);
                }
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

    private Map<String, Object> settings(Descriptor descriptor) {
        return descriptor.connector().equals("mysql")
                ? Map.of("host", host, "port", 33306, "database", "tpcc_331",
                        "username", "root", "password", password)
                : Map.of("host", host, "port", 55433, "database", "postgres", "schema", "public",
                        "user", "root", "password", password,
                        "globalPublicationName", "tapstate_sample_pub");
    }
}
