package io.tapstate.adapters.pdk;

import io.tapdata.ErrorCodeEntity;
import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.ContentHash;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Build-time export and admission checks. Runtime resolution uses only the resulting data. */
class PdkDiagnosticCatalogTest {
    @TapExClass(code = 43, module = "first", prefix = "example")
    public static class First {
        @TapExCode(describe = "The operation was refused.", dynamicDescription = "Ordered {} then {}.")
        public static final String REFUSED = "043001";
    }

    @TapExClass(code = 43, module = "second", prefix = "EXAMPLE")
    public static class Second {
        @TapExCode(describe = "Another operation was refused.")
        public static final String REFUSED = "043001";
    }

    @Test
    void exportChecksCompleteCodesBeforeSerializationAndPreservesTheOriginalWidth() throws Exception {
        var catalog = PdkDiagnosticCatalog.read("fixture", export("fixture", List.of(First.class)));
        assertThat(catalog.definition("043001").code()).isEqualTo("EXAMPLE043001");
        assertThat(catalog.definition("43001")).isNull();
        assertThatThrownBy(() -> export("fixture", List.of(First.class, Second.class)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repeated full code");

        var failure = new TapTestItemException();
        failure.setMessage("Driver refused 127.0.0.1:5432");
        failure.setDynamicDescriptionParameters(new String[]{"alpha", "beta"});
        var document = PdkConnectionDiagnostic.capture(failure, "Fallback", null, "043001", catalog, Locale.ENGLISH);
        assertThat(document).containsEntry("code", "EXAMPLE043001").containsEntry("arguments", List.of("alpha", "beta"))
                .containsEntry("message", "The operation was refused.\nOrdered alpha then beta.")
                .containsEntry("rawMessage", "Driver refused 127.0.0.1:5432")
                .doesNotContainKey("definitionStatus");
    }

    @Test
    void theCatalogFollowsTheSelectedArtifactAndNeverBorrowsAnotherSourcesEntry(@TempDir Path temp) throws Exception {
        var one = jar(temp.resolve("one.jar"), export("one", List.of(First.class)));
        var two = jar(temp.resolve("two.jar"), export("two", List.of(Second.class)));
        var oneCatalog = PdkDiagnosticCatalog.load("one", one);
        var twoCatalog = PdkDiagnosticCatalog.load("two", two);
        assertThat(oneCatalog.definition("EXAMPLE043001").text(Locale.ENGLISH).get("message")).startsWith("The operation");
        assertThat(twoCatalog.definition("EXAMPLE043001").text(Locale.ENGLISH).get("message")).startsWith("Another");
        assertThat(PdkDiagnosticCatalog.load("two", one).definition("EXAMPLE043001")).isNull();
        var unverified = new ConnectorRef(one.classpath(), one.className(), null, null);
        assertThat(PdkDiagnosticCatalog.load("one", unverified).definition("EXAMPLE043001")).isNull();
    }

    @Test
    void reusesTheCatalogOnlyForTheSameConnectorAndArtifact(@TempDir Path temp) throws Exception {
        var first = jar(temp.resolve("first.jar"), export("fixture", List.of(First.class)));
        var replacement = jar(temp.resolve("replacement.jar"), export("fixture", List.of(Second.class)));
        var cache = new PdkDiagnosticCatalog.Cache();

        var original = cache.resolve("fixture", first);
        assertThat(cache.resolve("fixture", first)).isSameAs(original);
        assertThat(cache.resolve("other", first).definition("EXAMPLE043001")).isNull();
        var revised = cache.resolve("fixture", replacement);
        assertThat(revised).isNotSameAs(original);
        assertThat(revised.definition("EXAMPLE043001").text(Locale.ENGLISH).get("message")).startsWith("Another");
    }

    @Test
    void rejectsRepeatedKeysAndRepeatedFullCodesInSuppliedCatalogs() {
        String row = "{\"rawCode\":\"0043\",\"code\":\"EXT0043\",\"en\":{\"message\":\"Reason\"}}";
        String repeatedCode = "{\"formatVersion\":1,\"connectorId\":\"fixture\",\"definitions\":[" + row + "," + row + "]}";
        byte[] repeatedCodeBytes = repeatedCode.getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> PdkDiagnosticCatalog.read("fixture", repeatedCodeBytes))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("repeated full");
        String repeatedKey = "{\"formatVersion\":2,\"formatVersion\":1,\"connectorId\":\"fixture\",\"definitions\":[]}";
        byte[] repeatedKeyBytes = repeatedKey.getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> PdkDiagnosticCatalog.read("fixture", repeatedKeyBytes))
                .isInstanceOf(IOException.class);
    }

    @Test
    void capsExpansionWithoutRejectingACompressibleCatalog(@TempDir Path temp) throws Exception {
        String resource = """
                {"formatVersion":1,"connectorId":"fixture","definitions":[
                  {"rawCode":"043","code":"EXT043","en":{"message":"Reason"}}
                ]}
                """;
        var bounded = jar(temp.resolve("bounded.jar"), (resource + " ".repeat(128 * 1024)).getBytes(StandardCharsets.UTF_8));
        var oversized = jar(temp.resolve("oversized.jar"), (resource + " ".repeat(256 * 1024)).getBytes(StandardCharsets.UTF_8));

        assertThat(PdkDiagnosticCatalog.load("fixture", bounded).definition("043")).isNotNull();
        assertThat(PdkDiagnosticCatalog.load("fixture", oversized).definition("043")).isNull();
    }

    @Test
    void resolvesArgumentsAndFallsBackThroughAvailableLocaleMaps() throws Exception {
        byte[] resource = """
                {"formatVersion":1,"connectorId":"fixture","definitions":[
                  {"rawCode":"043","code":"EXT043","en":{"message":"Login failed","dynamic":"Account {} on {}"},
                   "zh-CN":{"message":"Simplified"},"zh-TW":{}},
                  {"rawCode":"044","code":"EXT044","en":{"dynamic":"Account {}"}},
                  {"rawCode":"045","code":"EXT045","en":{"message":"English"},"zh-TW":{"message":"Traditional"}}
                ]}
                """.getBytes(StandardCharsets.UTF_8);
        var catalog = PdkDiagnosticCatalog.read("fixture", resource);

        assertThat(catalog.resolve("043", Map.of(), List.of("user", "server"), Locale.ENGLISH).orElseThrow().message())
                .isEqualTo("Login failed\nAccount user on server");
        assertThat(catalog.resolve("EXT043", Map.of(), List.of("user"), Locale.ENGLISH).orElseThrow().message())
                .isEqualTo("Login failed");
        assertThat(catalog.resolve("043", Map.of(), List.of("user", "server", "extra"), Locale.ENGLISH).orElseThrow().message())
                .isEqualTo("Login failed");
        assertThat(catalog.resolve("043", Map.of("account", "user"), List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.resolve("043", Map.of(), List.of(), Locale.TAIWAN).orElseThrow().message()).isEqualTo("Simplified");
        assertThat(catalog.resolve("044", Map.of(), List.of("user"), Locale.CHINA).orElseThrow().message()).isEqualTo("Account user");
        assertThat(catalog.resolve("045", Map.of(), List.of(), Locale.forLanguageTag("zh-Hant")).orElseThrow().message())
                .isEqualTo("Traditional");
    }

    @Test
    void anAmbiguousRawCodeDoesNotBorrowAnotherDefinition() throws Exception {
        byte[] resource = """
                {"formatVersion":1,"connectorId":"fixture","definitions":[
                  {"rawCode":"043","code":"EXT043","en":{"message":"First"}},
                  {"rawCode":"043","code":"OTHER043","en":{"message":"Second"}},
                  {"rawCode":"EXT043","code":"OUTEREXT043","en":{"message":"Third"}}
                ]}
                """.getBytes(StandardCharsets.UTF_8);
        var catalog = PdkDiagnosticCatalog.read("fixture", resource);

        assertThat(catalog.definition("043")).isNull();
        assertThat(catalog.definition("EXT043")).isNull();
        assertThat(catalog.definition("OTHER043").text(Locale.ENGLISH)).containsEntry("message", "Second");
        assertThat(catalog.definition("OUTEREXT043").text(Locale.ENGLISH)).containsEntry("message", "Third");
    }

    @Test
    void anAmbiguousRawCodeCannotResolveThroughTheFullCodeIndex() throws Exception {
        var resource = """
                {"formatVersion":1,"connectorId":"fixture","definitions":[
                  {"rawCode":"043","code":"EXT043","en":{"message":"First"}},
                  {"rawCode":"EXT043","code":"ALTEXT043","en":{"message":"Second"}},
                  {"rawCode":"EXT043","code":"OTHEREXT043","en":{"message":"Third"}}
                ]}
                """.getBytes(StandardCharsets.UTF_8);
        var catalog = PdkDiagnosticCatalog.read("fixture", resource);

        assertThat(catalog.definition("EXT043")).isNull();
        assertThat(catalog.resolve("EXT043", Map.of(), List.of(), Locale.ENGLISH)).isEmpty();
        assertThat(catalog.definition("043").text(Locale.ENGLISH)).containsEntry("message", "First");
        assertThat(catalog.definition("ALTEXT043").text(Locale.ENGLISH)).containsEntry("message", "Second");
    }

    @Test
    void capturesExplicitCommonCausesAndKeepsSuppressedFailuresOutOfTheirArray() throws Exception {
        var outer = new TapTestItemException();
        var group = new io.tapdata.entity.error.CoreException("Common failure");
        group.coreException(new io.tapdata.entity.error.CoreException("First failure"));
        group.coreException(new io.tapdata.entity.error.CoreException("Second failure"));
        group.addSuppressed(new IllegalStateException("Cleanup diagnostic"));
        outer.initCause(group);
        var catalog = PdkDiagnosticCatalog.read("fixture", export("fixture", List.of(First.class)));
        var result = PdkConnectionDiagnostic.capture(outer, "Failed", null, null, catalog, Locale.ENGLISH);
        var captured = (Map<?, ?>) result.get("cause");
        assertThat(captured.containsKey("cause")).isFalse();
        assertThat((String) captured.get("stackTrace")).contains("Cleanup diagnostic");
        assertThat((List<?>) captured.get("causes")).hasSize(2);
        assertThat(captured.get("causes").toString()).contains("First failure", "Second failure").doesNotContain("Cleanup diagnostic");
    }

    @Test
    void aBrokenSecondaryGetterKeepsThePrimaryDiagnostic() throws Exception {
        var outer = new TapTestItemException() {
            @Override
            public String getStack() {
                throw new IllegalStateException("secondary getter failure");
            }
        };
        var catalog = PdkDiagnosticCatalog.read("fixture", export("fixture", List.of(First.class)));

        var result = PdkConnectionDiagnostic.capture(outer, "Connection refused", null, "driver-state",
                catalog, Locale.ENGLISH);

        assertThat(result).containsEntry("code", "driver-state").containsEntry("definitionStatus", "unverified")
                .containsEntry("message", "Connection refused").doesNotContainKey("stackTrace");
    }

    @Test
    void exportFromExplicitArtifactWhenRequested() throws Exception {
        String artifact = System.getProperty("tapstate.diagnostics.export.artifact");
        String classes = System.getProperty("tapstate.diagnostics.export.classes");
        String id = System.getProperty("tapstate.diagnostics.export.connector");
        String output = System.getProperty("tapstate.diagnostics.export.output");
        if (artifact == null && classes == null && id == null && output == null) return;
        assertThat(artifact).isNotBlank();
        assertThat(classes).isNotBlank();
        assertThat(id).isNotBlank();
        assertThat(output).isNotBlank();
        var source = Path.of(artifact).toRealPath().toUri().toURL();
        try (var loader = new URLClassLoader(new java.net.URL[]{source}, getClass().getClassLoader())) {
            List<Class<?>> types = new ArrayList<>();
            for (String name : classes.split(",")) {
                Class<?> type = Class.forName(name.trim(), false, loader);
                assertThat(type.getProtectionDomain().getCodeSource().getLocation()).isEqualTo(source);
                types.add(type);
            }
            byte[] result = export(id, types);
            PdkDiagnosticCatalog.read(id, result);
            Files.write(Path.of(output), result);
        }
    }

    private static byte[] export(String connector, List<Class<?>> types) throws IllegalAccessException {
        Map<String, Map<String, Object>> entries = new TreeMap<>();
        for (var type : types) {
            if (type.getDeclaredAnnotation(TapExClass.class) == null) {
                throw new IllegalArgumentException("missing explicit PDK diagnostic module");
            }
            for (var field : type.getDeclaredFields()) {
                var annotation = field.getAnnotation(TapExCode.class);
                if (annotation == null) continue;
                if (!Modifier.isStatic(field.getModifiers()) || !Modifier.isFinal(field.getModifiers())
                        || field.getType() != String.class || !field.trySetAccessible()) {
                    throw new IllegalArgumentException("PDK diagnostic code must be a static final string");
                }
                String raw = (String) field.get(null);
                String code = ErrorCodeEntity.create().code(raw).sourceExClass(type).fullErrorCode();
                if (code.isBlank()) throw new IllegalArgumentException("PDK diagnostic code is blank");
                Map<String, Object> entry = new TreeMap<>();
                entry.put("rawCode", raw);
                entry.put("code", code);
                entry.put("en", language(annotation.describe(), annotation.solution(), annotation.dynamicDescription()));
                var simplified = language(annotation.describeCN(), annotation.solutionCN(), annotation.dynamicDescriptionCN());
                if (!simplified.isEmpty()) entry.put("zh-CN", simplified);
                if (entries.putIfAbsent(code, entry) != null) {
                    throw new IllegalArgumentException("repeated full code " + code);
                }
            }
        }
        Map<String, Object> result = new TreeMap<>();
        result.put("formatVersion", 1);
        result.put("connectorId", connector);
        result.put("definitions", List.copyOf(entries.values()));
        return JsonWriter.write(result).getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, String> language(String message, String solution, String dynamic) {
        Map<String, String> text = new TreeMap<>();
        if (!message.isBlank()) text.put("message", message);
        if (!solution.isBlank()) text.put("solution", solution);
        if (!dynamic.isBlank()) text.put("dynamic", dynamic);
        return text;
    }

    private static ConnectorRef jar(Path path, byte[] data) throws Exception {
        try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
            jar.putNextEntry(new JarEntry(PdkDiagnosticCatalog.RESOURCE));
            jar.write(data);
            jar.closeEntry();
        }
        try (var input = Files.newInputStream(path)) {
            return new ConnectorRef(List.of(path), "fixture.Provider", null, null, null, ContentHash.of(input), false);
        }
    }
}
