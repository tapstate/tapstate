package io.tapstate.app;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ManagedAiProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4FamilyHttpSigner;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpRequest;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "tapstate.ai.bedrock.enabled", havingValue = "true")
class ManagedAiConfiguration {

    @Bean(destroyMethod = "close")
    DefaultCredentialsProvider bedrockCredentials() {
        return DefaultCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
    }

    @Bean
    ManagedAiProvider managedAiProvider(DefaultCredentialsProvider credentials,
            @Value("${tapstate.ai.bedrock.region:ap-southeast-1}") String region,
            @Value("${tapstate.ai.bedrock.model:global.moonshotai.kimi-k3}") String model,
            @Value("${tapstate.ai.bedrock.requests-per-minute:30}") int requestsPerMinute) {
        var signer = AwsV4HttpSigner.create();
        return ManagedAiProvider.bedrock(region, model, requestsPerMinute,
                (request, endpoint, json) -> sign(credentials, signer, region, request, endpoint, json));
    }

    static void sign(AwsCredentialsProvider credentials, AwsV4HttpSigner signer, String region,
            HttpRequest.Builder request, URI endpoint, byte[] json) {
        try {
            if (!(credentials.resolveCredentials() instanceof AwsSessionCredentials identity)) {
                throw AiProviderService.error(ControlError.AI_PROVIDER_AUTH_FAILED);
            }
            var unsigned = SdkHttpRequest.builder().uri(endpoint).method(SdkHttpMethod.POST)
                    .putHeader("content-type", "application/json").build();
            var signed = signer.sign(spec -> spec.identity(identity).request(unsigned)
                    .payload(() -> new ByteArrayInputStream(json))
                    .putProperty(AwsV4FamilyHttpSigner.SERVICE_SIGNING_NAME, "bedrock")
                    .putProperty(AwsV4HttpSigner.REGION_NAME, region));
            signed.request().headers().forEach((name, values) -> {
                if (!"host".equalsIgnoreCase(name) && !"content-type".equalsIgnoreCase(name)
                        && !"content-length".equalsIgnoreCase(name)) {
                    values.forEach(value -> request.header(name, value));
                }
            });
        } catch (SdkException refused) {
            throw AiProviderService.error(ControlError.AI_PROVIDER_AUTH_FAILED);
        }
    }
}
