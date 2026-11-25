package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import static org.assertj.core.api.Assertions.*;

class AlertRetentionSettingsTest {
    @Configuration(proxyBeanMethods=false) @EnableConfigurationProperties(AlertRetentionSettings.class) static class Bound { }
    @Test void actualPropertyBindingProducesOneValidatedFrozenProfile() {
        new ApplicationContextRunner().withUserConfiguration(Bound.class).withPropertyValues("tapstate.alerting.retention.accepted-payload=PT1H","tapstate.alerting.retention.ended-episode=P7D")
                .run(context->{assertThat(context).hasNotFailed();var value=context.getBean(AlertRetentionSettings.class).resolve(AlertChannelPolicy.Deployment.ON_PREM);
                    assertThat(value.acceptedPayload()).isEqualTo(Duration.ofHours(1));assertThat(value.duration(AlertRetentionPolicy.Setting.ENDED_EPISODE)).isEqualTo(Duration.ofDays(7));});
    }
    @Test void invalidForbiddenAndAliasedSettingsFailTheWholeCandidateWithCodedDiagnostics() {
        for(var candidate:List.of(Map.of("unknown","PT1H"),Map.of("accepted-payload","PT1S"),Map.of("accepted-payload","PT0S"),Map.of("accepted-payload","P365D"),Map.of("failed-result","invalid"),Map.of("accepted-payload","PT1H","accepted_payload","PT1H"))){
            var properties=new AlertRetentionSettings();properties.setRetention(candidate);assertThatThrownBy(()->properties.resolve(AlertChannelPolicy.Deployment.ON_PREM)).isInstanceOf(TapstateException.class);
        }
        var cloud=new AlertRetentionSettings();cloud.setRetention(Map.of("accepted-payload","PT1H"));assertThatThrownBy(()->cloud.resolve(AlertChannelPolicy.Deployment.CLOUD_MANAGED)).isInstanceOf(TapstateException.class);
    }
    @Test void laterConfigurationDoesNotChangeAnEarlierImmutablePolicyAndCloudSnapshotRemainsFixed() {
        var properties=new AlertRetentionSettings();properties.setRetention(Map.of("accepted-payload","PT1H"));var old=properties.resolve(AlertChannelPolicy.Deployment.ON_PREM);
        properties.setRetention(Map.of("accepted-payload","PT2H"));assertThat(properties.resolve(AlertChannelPolicy.Deployment.ON_PREM).acceptedPayload()).isEqualTo(Duration.ofHours(2));assertThat(old.acceptedPayload()).isEqualTo(Duration.ofHours(1));
        properties.setRetention(Map.of());assertThat(properties.resolve(AlertChannelPolicy.Deployment.CLOUD_MANAGED).acceptedPayload()).isEqualTo(Duration.ofHours(1));
    }
}
