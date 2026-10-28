package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.alerting.NotificationChannel;
import java.net.URI;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CloudAlertEventWireTest {
    @Test void episodeIdentityIsIndependentOfTheFrozenBatchNumber() throws Exception {
        var scope=new AlarmKey.Scope("organization","cluster-a");
        var alarm=new AlarmKey(scope,new AlarmKey.Resource("pipeline","orders","1"),"pipeline.failed","rule");
        var key=new DeliveryKey(new DeliveryKey.NotificationRound(scope,"group-a","7","route","route-binding",DeliveryKey.Kind.FAULT,3),"cloud-sdk","receiver","cluster-a");
        var destination=new NotificationChannel.Destination(scope,"cloud-sdk","receiver","cluster-a","binding");
        var bytes=CloudAlertEventWire.encode(URI.create("https://cloud.example.test"),destination,key,alarm,Instant.EPOCH,AlarmSeverity.MAJOR,"pipeline.failed",false,17L);
        var encoded=new ObjectMapper().readTree(bytes);
        assertThat(encoded.path("payload").path("episode").asLong()).isEqualTo(17);
        assertThat(encoded.path("payload").path("subjectIncarnation").asText()).isEqualTo("7");
        assertThat(encoded.path("severity").asText()).isEqualTo("CRITICAL");
        assertThat(encoded.path("dedupeKey").asText()).isEqualTo(CloudAlertEventWire.dedupe(URI.create("https://cloud.example.test"),destination,key));
    }
    @Test void recoveryKeepsTheOriginalEpisodeAndMapsOnlyTheCloudEventSeverity() throws Exception {
        var scope=new AlarmKey.Scope("organization","cluster-a");var alarm=new AlarmKey(scope,new AlarmKey.Resource("pipeline","orders","1"),"pipeline.failed","rule");
        var key=new DeliveryKey(new DeliveryKey.NotificationRound(scope,"group-a","8","route","route-binding",DeliveryKey.Kind.RECOVERY,4),"cloud-sdk","receiver","cluster-a");
        var destination=new NotificationChannel.Destination(scope,"cloud-sdk","receiver","cluster-a","binding");
        var encoded=new ObjectMapper().readTree(CloudAlertEventWire.encode(URI.create("https://cloud.example.test"),destination,key,alarm,Instant.EPOCH,AlarmSeverity.MAJOR,"pipeline.failed",true,17L));
        assertThat(encoded.path("severity").asText()).isEqualTo("INFO");assertThat(encoded.path("payload").path("originalSeverity").asText()).isEqualTo("MAJOR");assertThat(encoded.path("payload").path("episode").asLong()).isEqualTo(17);assertThat(encoded.path("payload").path("recoverySummary").asBoolean()).isTrue();
    }
    @Test void managedTargetBindingIncludesTheTrustedControlOriginAndOriginalDeployment() {
        var scope=new AlarmKey.Scope("organization","cluster-a");var setup=new io.tapstate.spi.store.StorageSetupScope("cluster-a","installation","operation",1);var origin=URI.create("https://cloud.example.test");var binding=AlertNotificationPlanner.managedBinding(origin,scope,setup);
        assertThat(AlertNotificationPlanner.managedBinding(origin,scope,setup)).isEqualTo(binding);
        assertThat(AlertNotificationPlanner.managedBinding(URI.create("https://another.example.test"),scope,setup)).isNotEqualTo(binding);
        assertThat(AlertNotificationPlanner.managedBinding(origin,scope,new io.tapstate.spi.store.StorageSetupScope("cluster-a","installation","replacement",2))).isNotEqualTo(binding);
        assertThat(AlertNotificationPlanner.managedBinding(origin,new AlarmKey.Scope("another-organization","cluster-a"),setup)).isNotEqualTo(binding);
    }

}
