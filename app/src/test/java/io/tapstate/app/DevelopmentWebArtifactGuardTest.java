package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class DevelopmentWebArtifactGuardTest {
    private static final AtomicBoolean SERVICE_CREATED = new AtomicBoolean();

    @Configuration(proxyBeanMethods = false)
    @Import(DevelopmentWebArtifactGuard.class)
    static class GuardConfiguration {
        @Bean
        Object serviceBoundary() {
            SERVICE_CREATED.set(true);
            return new Object();
        }
    }

    private ApplicationContextRunner runner(String metadata) {
        SERVICE_CREATED.set(false);
        return new ApplicationContextRunner().withUserConfiguration(GuardConfiguration.class)
                .withInitializer(context -> ((GenericApplicationContext) context).setResourceLoader(
                        new DefaultResourceLoader() {
                            @Override
                            public Resource getResource(String location) {
                                if ("classpath:META-INF/tapstate-web.properties".equals(location)) {
                                    return new ByteArrayResource(metadata.getBytes(StandardCharsets.UTF_8));
                                }
                                return super.getResource(location);
                            }
                        }));
    }

    @Test
    void rejectsLocalWebBeforeCreatingAnyServiceWithoutExplicitConfirmation() {
        runner("purpose=local-development\nprofile=op\n").run(context -> {
            assertThat(context.getStartupFailure()).isInstanceOf(TapstateException.class);
            assertThat(((TapstateException) context.getStartupFailure()).code())
                    .isEqualTo(BootError.WEB_DEVELOPMENT_CONFIRMATION_REQUIRED);
            assertThat(SERVICE_CREATED).isFalse();
        });
    }

    @Test
    void startsConfirmedLocalOpAndPreservesOrdinaryReleaseMetadata() {
        runner("purpose=local-development\nprofile=op\n")
                .withPropertyValues("tapstate.local-development.enabled=true")
                .run(context -> {
                    assertThat(context.getStartupFailure()).isNull();
                    assertThat(SERVICE_CREATED).isTrue();
                });
        runner("repository=tapstate/tapstate-web\nrevision=legacy\n").run(context -> {
            assertThat(context.getStartupFailure()).isNull();
            assertThat(SERVICE_CREATED).isTrue();
        });
        runner("purpose=release\n").run(context -> assertThat(context.getStartupFailure()).isNull());
    }

    @Test
    void doesNotUseConfirmationToAcceptUnsupportedCloudOrUnknownPurpose() {
        for (String metadata : new String[] {
                "purpose=local-development\nprofile=cloud\n", "purpose=local-development\n", "purpose=unknown\n",
                "purpose=\\uZZZZ\n"
        }) {
            runner(metadata).withPropertyValues("tapstate.local-development.enabled=true").run(context -> {
                assertThat(context.getStartupFailure()).isInstanceOf(TapstateException.class);
                assertThat(((TapstateException) context.getStartupFailure()).code())
                        .isEqualTo(BootError.WEB_DEVELOPMENT_ARTIFACT_INVALID);
                assertThat(SERVICE_CREATED).isFalse();
            });
        }
    }
}
