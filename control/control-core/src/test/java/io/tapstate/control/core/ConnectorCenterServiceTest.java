package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AuditRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorCenterServiceTest {
    @Test
    void catalogAndProgressReadsDoNotWriteAudit() {
        var records = new ArrayList<AuditRecord>();
        var center = new FixtureCenter();
        var service = new ConnectorCenterService(center, new AuditGate(records::add, Clock.systemUTC()));

        assertThat(service.catalog()).containsEntry("kind", "catalog");
        assertThat(service.operation("original-operation")).containsEntry("id", "original-operation");
        assertThat(records).isEmpty();
    }

    @Test
    void mutationsAuditTheVerifiedPrincipalBeforeCallingTheCenter() {
        var records = new ArrayList<AuditRecord>();
        FixtureCenter center = new FixtureCenter() {
            @Override public Map<String, Object> refresh() {
                assertThat(records).hasSize(1);
                return super.refresh();
            }
            @Override public Map<String, Object> download(String id) {
                assertThat(records).hasSize(2);
                return super.download(id);
            }
            @Override public Map<String, Object> upload(InputStream artifact, long contentLength, String expectedConnectorId) {
                assertThat(records).hasSize(3);
                return super.upload(artifact, contentLength, expectedConnectorId);
            }
        };
        var service = new ConnectorCenterService(center, new AuditGate(records::add, Clock.systemUTC()));
        var stream = new ByteArrayInputStream(new byte[]{1, 2, 3});

        service.refresh("admin");
        service.download("mongodb", "admin");
        service.upload(stream, 3, "mysql", "admin");

        assertThat(records).extracting(AuditRecord::operationId).containsExactly(
                "connector-center.refresh", "connector-center.download", "connector-center.upload");
        assertThat(records).extracting(AuditRecord::principal).containsOnly("admin");
        assertThat(records).extracting(AuditRecord::resourceId).containsExactly(
                "connector-catalog", "mongodb", "connector-upload");
        assertThat(center.received).isSameAs(stream);
        assertThat(center.contentLength).isEqualTo(3);
        assertThat(center.expectedConnectorId).isEqualTo("mysql");
    }

    @Test
    void anAuditFailurePreventsAllMutationAndStreamConsumption() {
        var center = new FixtureCenter();
        var service = new ConnectorCenterService(center, new AuditGate(record -> {
            throw new IllegalStateException("fixture audit unavailable");
        }, Clock.systemUTC()));
        List<Runnable> commands = List.of(
                () -> service.refresh("admin"),
                () -> service.download("mongodb", "admin"),
                () -> service.upload(new ByteArrayInputStream(new byte[]{1}), 1, null, "admin"));

        for (var command : commands) {
            assertThatThrownBy(command::run).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(ControlError.AUDIT_BLOCKED));
        }
        assertThat(center.mutations).isZero();
        assertThat(center.received).isNull();
    }

    private static class FixtureCenter implements ConnectorCenter {
        private int mutations;
        private InputStream received;
        private long contentLength;
        private String expectedConnectorId;

        @Override public Map<String, Object> catalog() { return Map.of("kind", "catalog"); }
        @Override public Map<String, Object> operation(String id) { return Map.of("id", id); }
        @Override public Map<String, Object> refresh() { mutations++; return Map.of("stage", "refreshed"); }
        @Override public Map<String, Object> download(String id) { mutations++; return Map.of("connectorId", id); }
        @Override public Map<String, Object> upload(InputStream artifact, long length, String expectedConnectorId) {
            mutations++;
            received = artifact;
            contentLength = length;
            this.expectedConnectorId = expectedConnectorId;
            return Map.of("stage", "installing");
        }
    }
}
