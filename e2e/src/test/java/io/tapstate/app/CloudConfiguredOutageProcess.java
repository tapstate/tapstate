package io.tapstate.app;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.core.env.StandardEnvironment;

/** Real Cloud storage and business runtime; the external provider alone is a controlled SDK fixture. */
public final class CloudConfiguredOutageProcess {
    private CloudConfiguredOutageProcess() {}
    public static void main(String[] args) {
        var arguments=new ArrayList<String>();
        for(String argument:args) {
            if(argument.startsWith("--tapstate.store.mongo.uri=")) arguments.add("--tapstate.cloud.atlas-uri="+argument.substring(argument.indexOf('=')+1));
            else if(!argument.startsWith("--tapstate.store.mongo.enabled=")) arguments.add(argument);
        }
        arguments.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example","--tapstate.cloud.token=outage-fixture-runtime-token",
                "--SDK_STATUS_SENDER_ENABLED=false","--tapstate.control.auth.jwt-secret=cloud-outage-stable-test-key"));
        var environment=new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        var application=new SpringApplicationBuilder(Bootstrap.class).environment(environment).initializers(context -> {
            var definition=new RootBeanDefinition(CloudSdkBridge.class,() -> {
                var settings=context.getBean(CloudRuntimeSettings.class);
                return new CloudSdkBridge(settings,new Provider(settings.clusterId()));
            }); definition.setPrimary(true);
            ((BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("outageFixtureSdk",definition);
        }).run(arguments.toArray(String[]::new));
        // Spring returns only after its real ready event and every startup runner have completed.
        if(!application.isActive())throw new AssertionError("the Cloud fixture application did not complete startup");
        System.out.println("CLOUD_FIXTURE_APPLICATION_READY");System.out.flush();
    }
    private static final class Provider implements CloudSdkBridge.SdkRuntime {
        private final String cluster;
        private final Set<String> consumed=ConcurrentHashMap.newKeySet();
        Provider(String cluster) { this.cluster=cluster; }
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            if(code==null || code.isBlank() || !consumed.add(code))throw new CloudSdkBridge.ProviderFailure();
            return new CloudSdkBridge.ExchangeResult(code,"organization",cluster,"Fixture","Cluster","local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("cloud-user","organization",cluster,jwt,Instant.now().plusSeconds(600),
                    "https://cloud.example","127.0.0.1",List.of("workload:write"));
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization",cluster,null,"PROVISIONING");
        }
        @Override public boolean verifyCallback(String method,String timestamp,String nonce,String data,String signature) { return false; }
        @Override public void report(String cluster,String nonce,io.tapstate.control.core.CloudRuntimeStatus status,String token) {
            throw new AssertionError("the controlled provider disables status reporting");
        }
    }
}
