package io.tapstate.control.core;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Service dispatch tests; the codec fixture does not claim transport encryption or database atomicity. */
class ConfigurationImportServiceTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T00:00:00Z"), ZoneOffset.UTC);
    private static final ConfigurationImportStore.Scope OP = new ConfigurationImportStore.Scope("cluster", 1, StorageBinding.Mode.OP, null);
    private static SourceResource source(String connector) {
        return new SourceResource("orders", null, connector, Map.of("host", "localhost", "port", 3306,
                "database", "orders", "username", "root", "password", "sentinel-source-password"), null, null, null, null);
    }
    private static ConfigurationImportStore.Payload payload(List<Resource> resources, List<User> users) {
        return new ConfigurationImportStore.Payload(resources, users, List.of(), Map.of(), Map.of());
    }
    private static final class Definitions implements ArtifactStore {
        boolean unavailable;
        @Override public void saveAll(List<Resource> resources) { throw new AssertionError("ordinary authoring writes are not import commits"); }
        @Override public Optional<Resource> get(String id) { if (unavailable) throw new AssertionError("completed import must not reload mutable definitions"); return Optional.empty(); }
        @Override public List<Resource> list() { if (unavailable) throw new AssertionError("completed import must not reload mutable definitions"); return List.of(); }
    }
    private static final class Imports implements ConfigurationImportStore {
        Status status; Payload prepared; int admissions; int commits;
        @Override public ConfigurationImportStore guardedBy(Runnable active) {
            return new ConfigurationImportStore() {
                @Override public ConfigurationImportStore guardedBy(Runnable other) { return Imports.this.guardedBy(() -> { active.run(); other.run(); }); }
                @Override public Status preflight(Scope scope, Payload payload) { active.run(); return Imports.this.preflight(scope,payload); }
                @Override public Optional<Status> find(Scope scope,String id) { active.run(); return Imports.this.find(scope,id); }
                @Override public Receipt commit(Ticket ticket,Payload payload) { active.run(); return Imports.this.commit(ticket,payload); }
            };
        }
        @Override public Status preflight(Scope scope, Payload payload) {
            admissions++; prepared = payload;
            var ticket = new Ticket(UUID.randomUUID().toString(), scope, "a".repeat(64), "b".repeat(64), 0,
                    CLOCK.instant(), CLOCK.instant().plus(PREFLIGHT_LIFETIME));
            status = new Status(ticket, Phase.PENDING, 0, null); return status;
        }
        @Override public Optional<Status> find(Scope scope, String id) {
            return status != null && status.ticket().scope().equals(scope) && status.ticket().id().equals(id) ? Optional.of(status) : Optional.empty();
        }
        @Override public Receipt commit(Ticket ticket, Payload payload) {
            commits++;
            assertThat(payload.referenceHashes()).isEqualTo(prepared.referenceHashes());
            assertThat(payload.resources().stream().map(new CanonicalWriter()::write).toList())
                    .isEqualTo(prepared.resources().stream().map(new CanonicalWriter()::write).toList());
            if (status.phase() == Phase.COMMITTED) return status.receipt();
            var hashes = new TreeMap<String,String>(); prepared.resources().forEach(resource -> hashes.put(resource.id(), CanonicalHash.of(resource)));
            var receipt = new Receipt(ticket.id(), ticket.inputHash(), 1, hashes,
                    new TreeSet<>(prepared.users().stream().map(User::username).toList()), Set.of());
            status = new Status(ticket, Phase.COMMITTED, 1, receipt); return receipt;
        }
    }
    private static final class Codec implements ConfigurationImportService.TransferCodec {
        final Map<String,ConfigurationImportStore.Payload> packages = new HashMap<>();
        @Override public ConfigurationImportStore.Payload open(String packet,String key,Map<String,String> mapping) {
            var payload = packages.get(packet); if (payload == null) throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "invalid test packet"), null);
            return payload;
        }
        @Override public String seal(ConfigurationImportStore.Payload prepared,String key) {
            String packet = "prepared-" + packages.size(); packages.put(packet, prepared); return packet;
        }
    }
    private static final class Fixture {
        final Definitions definitions = new Definitions(); final Imports imports = new Imports(); final Codec codec = new Codec();
        final List<AuditRecord> records = new ArrayList<>();
        boolean auditAvailable = true;
        final AuditGate audit = new AuditGate(record -> {
            if (!auditAvailable) throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "fixture"), null);
            records.add(record);
        }, CLOCK);
        final ApplyService validation = new ApplyService(TapstateCatalog::load, definitions, audit, new EmptySchemaStore(), PlanAdvisories.none(), SchemaDerivation.none());
        final ConfigurationImportService service = new ConfigurationImportService(imports, codec, validation, audit);
        ConfigurationImportService.Submission submit(ConfigurationImportStore.Payload payload) {
            codec.packages.put("input", payload);
            return new ConfigurationImportService.Submission("input", "sentinel-transfer-key", Map.of());
        }
    }
    @Test void preflightUsesCentralValidationAndReturnsOnlyAnEncryptedConfirmationAndSmallOperation() {
        var f = new Fixture(); var input = f.submit(payload(List.of(source("mysql")), List.of()));
        var result = f.service.preflight("admin", OP, () -> {}, input);
        assertThat(f.imports.admissions).isEqualTo(1);
        assertThat(f.records).singleElement().extracting(AuditRecord::operationId).isEqualTo("configuration.import.preflight");
        assertThat(result.toString()).doesNotContain("sentinel-transfer-key", "sentinel-source-password");
        assertThat(input.toString()).doesNotContain("sentinel-transfer-key");
        assertThat(f.codec.packages.get(result.preparedTransfer()).resources()).hasSize(1);
    }
    @Test void cloudUserPackagesAreRefusedBeforeAdmissionOrAudit() {
        var f = new Fixture(); var input = f.submit(payload(List.of(), List.of(new User("admin", "hash", "admin"))));
        var cloud = new ConfigurationImportStore.Scope("cluster", 1, StorageBinding.Mode.CLOUD, "organization");
        assertThatThrownBy(() -> f.service.preflight("cloud-user", cloud, () -> {}, input)).isInstanceOf(TapstateException.class);
        assertThat(f.imports.admissions).isZero(); assertThat(f.records).isEmpty();
    }
    @Test void invalidDefinitionsDoNotReachTheImportStore() {
        var f = new Fixture();
        var config = new HashMap<>(source("mysql").config()); config.put("port", "not-a-number");
        var invalid = new SourceResource("orders", null, "mysql", config, null, null, null, null);
        var input = f.submit(payload(List.of(invalid), List.of()));
        assertThatThrownBy(() -> f.service.preflight("admin", OP, () -> {}, input)).isInstanceOf(TapstateException.class);
        assertThat(f.imports.admissions).isZero();
    }
    @Test void committedRetryReturnsTheOriginalReceiptAfterDefinitionsDisappear() {
        var f = new Fixture(); var input = f.submit(payload(List.of(source("mysql")), List.of()));
        var admitted = f.service.preflight("admin", OP, () -> {}, input);
        var confirmation = new ConfigurationImportService.Submission(admitted.preparedTransfer(), input.transferKey(), Map.of());
        var receipt = f.service.commit("admin", OP, () -> {}, admitted.operation().ticket().id(), confirmation);
        f.definitions.unavailable = true;
        assertThat(f.service.commit("other-admin", OP, () -> {}, receipt.id(), confirmation)).isEqualTo(receipt);
        assertThat(f.imports.commits).isEqualTo(2);
        assertThat(f.records).hasSize(2);
    }
    @Test void auditFailureBlocksAdmissionAndQueryOfAnUnknownIdCannotCreateOne() {
        var f = new Fixture(); f.auditAvailable = false;
        var input = f.submit(payload(List.of(), List.of(new User("admin", "hash", "admin"))));
        assertThatThrownBy(() -> f.service.preflight("admin", OP, () -> {}, input)).isInstanceOf(TapstateException.class);
        String unknown = UUID.randomUUID().toString();
        assertThat(f.service.status(OP, () -> {}, unknown).phase()).isEqualTo("UNKNOWN");
        assertThat(f.imports.admissions).isZero(); assertThat(f.imports.commits).isZero();
    }
    @Test void aCurrentAuthorizationGuardIsRequiredEvenForAnOriginalCommittedResult() {
        var f = new Fixture(); var input = f.submit(payload(List.of(), List.of(new User("admin", "hash", "admin"))));
        var admitted = f.service.preflight("admin", OP, () -> {}, input);
        Runnable lost = () -> { throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null); };
        assertThatThrownBy(() -> f.service.status(OP, lost, admitted.operation().ticket().id())).isInstanceOf(TapstateException.class);
        assertThat(f.imports.commits).isZero();
    }
}
