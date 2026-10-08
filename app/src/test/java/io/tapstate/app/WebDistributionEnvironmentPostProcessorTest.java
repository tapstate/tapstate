package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.support.ResourcePropertySource;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebDistributionEnvironmentPostProcessorTest {

    private static final List<String> KEYS = List.of(
            "tapstate.cloud.base-url", "tapstate.cloud.token", "tapstate.cloud.atlas-uri", "tapstate.cloud.cluster-id");
    private static final List<String> VALUES = List.of(
            "https://cloud.example", "sentinel-token", "mongodb://user:sentinel-password@atlas.example/metadata",
            "fixture-cluster");

    @TempDir
    Path directory;

    @Test
    void theRealBoot4ProcessorIsRegisteredAfterConfigData() throws Exception {
        String key = "org.springframework.boot.EnvironmentPostProcessor";
        boolean registered = false;
        var resources = getClass().getClassLoader().getResources("META-INF/spring.factories");
        while (resources.hasMoreElements()) {
            Properties properties = new Properties();
            try (InputStream input = resources.nextElement().openStream()) {
                properties.load(input);
            }
            String configured = properties.getProperty(key);
            if (configured != null && Arrays.asList(configured.split(","))
                    .contains(WebDistributionEnvironmentPostProcessor.class.getName())) {
                registered = true;
            }
        }

        assertThat(registered).isTrue();
        assertThat(new WebDistributionEnvironmentPostProcessor().getOrder())
                .isEqualTo(ConfigDataEnvironmentPostProcessor.ORDER + 1);
    }

    @ParameterizedTest(name = "{0} {1} configuration mask {2}")
    @MethodSource("configurationMatrix")
    void everyCarrierUsesTheSameStrictSixteenCombinationMatrix(
            Carrier carrier, WebDistribution.Profile profile, int mask) throws Exception {
        ConfigurableEnvironment environment = environment(carrier, values(mask));
        WebDistributionEnvironmentPostProcessor processor = new WebDistributionEnvironmentPostProcessor(
                () -> new WebDistribution(profile, profile == WebDistribution.Profile.CLOUD
                        ? "https://console.example/" : null));
        boolean paired = profile == WebDistribution.Profile.CLOUD ? mask == 15 : mask == 0;

        if (paired) {
            processor.postProcessEnvironment(environment, new SpringApplication(SentinelConfiguration.class));
        } else {
            assertThatThrownBy(() -> processor.postProcessEnvironment(
                    environment, new SpringApplication(SentinelConfiguration.class)))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(mask == 0 || mask == 15
                                ? BootError.WEB_PROFILE_MODE_MISMATCH : BootError.CLOUD_CONFIG_INCOMPLETE);
                        assertThat(failure.getCause()).isNull();
                        assertThat(failure.args().toString()).doesNotContain("sentinel-token", "sentinel-password");
                    });
        }
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void springActuallyLoadsACompleteCloudCarrierBeforeTheProfileGate(Carrier carrier) throws Exception {
        Counters counters = new Counters();
        var prepared = prepare(carrier, values(15), "cloud", counters);

        try (var context = prepared.application().run(prepared.arguments())) {
            assertThat(context.isActive()).isTrue();
            assertThat(counters.contexts()).hasValue(1);
            assertThat(counters.networkClients()).hasValue(1);
            assertThat(counters.ready()).hasValue(1);
        }
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void everyOnPremCarrierRemainsValidWithNoCloudValues(Carrier carrier) throws Exception {
        Counters counters = new Counters();
        var prepared = prepare(carrier, Map.of("tapstate.store.mongo.uri", "mongodb+srv://ordinary.example/op"),
                "onprem", counters);

        try (var context = prepared.application().run(prepared.arguments())) {
            assertThat(context.isActive()).isTrue();
            assertThat(counters.contexts()).hasValue(1);
            assertThat(counters.networkClients()).hasValue(1);
            assertThat(counters.ready()).hasValue(1);
        }
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void cloudWebWithNoSettingsIsRejectedBeforeContextOrNetworkClientCreation(Carrier carrier) throws Exception {
        Counters counters = new Counters();
        var prepared = prepare(carrier, Map.of(), "cloud", counters);

        assertEarlyFailure(prepared, counters, BootError.WEB_PROFILE_MODE_MISMATCH);
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void onPremWebCannotStartWithACompleteCloudCarrier(Carrier carrier) throws Exception {
        Counters counters = new Counters();
        var prepared = prepare(carrier, values(15), "onprem", counters);

        assertEarlyFailure(prepared, counters, BootError.WEB_PROFILE_MODE_MISMATCH);
    }

    @ParameterizedTest
    @EnumSource(Carrier.class)
    void aPartialCloudCarrierStillUsesTheExistingCodeBeforeAnySideEffect(Carrier carrier) throws Exception {
        Counters counters = new Counters();
        var prepared = prepare(carrier, values(7), "cloud", counters);

        assertEarlyFailure(prepared, counters, BootError.CLOUD_CONFIG_INCOMPLETE);
    }

    @Test
    void externalPropertiesCannotOverrideTheTrustedPackagedProfile() throws Exception {
        var environment = environment(Carrier.JVM, Map.of(
                "web.profile", "onprem", "tapstate.web.profile", "onprem", "tapstate.web-profile", "onprem"));
        WebDistributionEnvironmentPostProcessor processor = new WebDistributionEnvironmentPostProcessor(
                () -> new WebDistribution(WebDistribution.Profile.CLOUD, "https://console.example/"));

        assertThatThrownBy(() -> processor.postProcessEnvironment(
                environment, new SpringApplication(SentinelConfiguration.class)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BootError.WEB_PROFILE_MODE_MISMATCH));
    }

    @Test
    void theClusterIdAliasHasTheSameCanonicalPrecedenceAtTheEarlyGate() throws Exception {
        Map<String, String> given = new LinkedHashMap<>(values(7));
        given.put("CLUSTER_ID", "alias-cluster");
        ConfigurableEnvironment environment = environment(Carrier.JVM, given);
        WebDistributionEnvironmentPostProcessor processor = new WebDistributionEnvironmentPostProcessor(
                () -> new WebDistribution(WebDistribution.Profile.CLOUD, "https://console.example/"));

        processor.postProcessEnvironment(environment, new SpringApplication(SentinelConfiguration.class));
        environment.getPropertySources().addFirst(new MapPropertySource(
                "blank-canonical-id", Map.of("tapstate.cloud.cluster-id", "")));
        assertThatThrownBy(() -> processor.postProcessEnvironment(
                environment, new SpringApplication(SentinelConfiguration.class)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(BootError.CLOUD_CONFIG_INCOMPLETE));
    }

    @Test
    void anUnencodedProgrammerFailureIsNotLaunderedIntoAProfileError() {
        IllegalStateException defect = new IllegalStateException("fixture programming defect");
        WebDistributionEnvironmentPostProcessor processor = new WebDistributionEnvironmentPostProcessor(() -> {
            throw defect;
        });

        assertThatThrownBy(() -> processor.postProcessEnvironment(
                CloudFixtureEnvironment.isolated(), new SpringApplication(SentinelConfiguration.class)))
                .isSameAs(defect);
    }

    private Prepared prepare(Carrier carrier, Map<String, String> values, String profile,
            Counters counters) throws Exception {
        URL origin = origin(profile);
        var environment = CloudFixtureEnvironment.isolated();
        List<String> arguments = new ArrayList<>(List.of("--logging.level.root=OFF"));
        if (carrier == Carrier.PROPERTIES) {
            Path properties = properties(values);
            arguments.add("--spring.config.location=" + properties.toUri());
        } else {
            addCarrier(environment, carrier, values);
        }
        SpringApplication application = new SpringApplication(SentinelConfiguration.class);
        application.setEnvironment(environment);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.setBannerMode(Banner.Mode.OFF);
        application.setLogStartupInfo(false);
        application.setApplicationContextFactory(type -> {
            counters.contexts().incrementAndGet();
            return new AnnotationConfigApplicationContext();
        });
        application.addInitializers(context -> {
            counters.networkClients().incrementAndGet();
            context.getBeanFactory().registerSingleton("networkClientSentinel", new Object());
        });
        application.addListeners((ApplicationListener<ApplicationReadyEvent>) event ->
                counters.ready().incrementAndGet());
        application.addListeners(new FixtureProfileListener(
                new WebDistributionEnvironmentPostProcessor(() -> WebDistribution.read(origin))));
        return new Prepared(application, arguments.toArray(String[]::new));
    }

    private ConfigurableEnvironment environment(Carrier carrier, Map<String, String> values) throws Exception {
        var environment = CloudFixtureEnvironment.isolated();
        if (carrier == Carrier.PROPERTIES) {
            environment.getPropertySources().addFirst(new ResourcePropertySource(properties(values).toUri().toString()));
        } else {
            addCarrier(environment, carrier, values);
        }
        return environment;
    }

    private static void addCarrier(ConfigurableEnvironment environment, Carrier carrier, Map<String, String> values) {
        if (carrier == Carrier.ENVIRONMENT) {
            Map<String, Object> variables = new LinkedHashMap<>();
            values.forEach((key, value) -> variables.put(key.replace('.', '_').replace('-', '_').toUpperCase(
                    java.util.Locale.ROOT), value));
            environment.getPropertySources().addFirst(new SystemEnvironmentPropertySource(
                    StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, variables));
        } else {
            Properties properties = new Properties();
            properties.putAll(values);
            environment.getPropertySources().addFirst(new PropertiesPropertySource(
                    StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME, properties));
        }
    }

    private Path properties(Map<String, String> values) throws Exception {
        Path propertiesFile = Files.createTempFile(directory, "application-", ".properties");
        Properties properties = new Properties();
        properties.putAll(values);
        try (var output = Files.newOutputStream(propertiesFile)) {
            properties.store(output, "isolated configuration fixture");
        }
        return propertiesFile;
    }

    private URL origin(String profile) throws Exception {
        Path root = Files.createTempDirectory(directory, "distribution-");
        Path applicationClass = root.resolve("io/tapstate/app/WebDistribution.class");
        Files.createDirectories(applicationClass.getParent());
        Files.write(applicationClass, new byte[]{0});
        Files.createDirectories(root.resolve("META-INF"));
        Files.writeString(root.resolve("META-INF/tapstate-web.properties"), "web.profile=" + profile + "\n"
                + ("cloud".equals(profile) ? "cloud.console.url=https://console.example/\n" : ""));
        Files.createDirectories(root.resolve("static"));
        Files.writeString(root.resolve("static/index.html"), "<!doctype html><title>Isolated fixture</title>");
        return root.toUri().toURL();
    }

    private static Map<String, String> values(int mask) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int bit = 0; bit < KEYS.size(); bit++) {
            if ((mask & (1 << bit)) != 0) values.put(KEYS.get(bit), VALUES.get(bit));
        }
        return values;
    }

    static Stream<Arguments> configurationMatrix() {
        return Arrays.stream(Carrier.values()).flatMap(carrier ->
                Stream.of(WebDistribution.Profile.CLOUD, WebDistribution.Profile.ON_PREM).flatMap(profile ->
                        java.util.stream.IntStream.range(0, 16).mapToObj(mask -> Arguments.of(carrier, profile, mask))));
    }

    private static void assertEarlyFailure(Prepared prepared, Counters counters, BootError expected) {
        assertThatThrownBy(() -> prepared.application().run(prepared.arguments()))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(expected);
                    assertThat(failure.getCause()).isNull();
                    assertThat(failure.getMessage()).doesNotContain("sentinel-token", "sentinel-password");
                });
        assertThat(counters.contexts()).hasValue(0);
        assertThat(counters.networkClients()).hasValue(0);
        assertThat(counters.ready()).hasValue(0);
    }

    enum Carrier { PROPERTIES, ENVIRONMENT, JVM }

    @Configuration(proxyBeanMethods = false)
    static class SentinelConfiguration {
    }

    private record Counters(AtomicInteger contexts, AtomicInteger networkClients, AtomicInteger ready) {
        Counters() {
            this(new AtomicInteger(), new AtomicInteger(), new AtomicInteger());
        }
    }

    private record Prepared(SpringApplication application, String[] arguments) {
    }

    /** The fixture changes only the trusted resource origin, never a production operator setting. */
    private record FixtureProfileListener(WebDistributionEnvironmentPostProcessor processor)
            implements ApplicationListener<ApplicationEnvironmentPreparedEvent>, Ordered {
        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE + 11;
        }

        @Override
        public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
            processor.postProcessEnvironment(event.getEnvironment(), event.getSpringApplication());
        }
    }
}
