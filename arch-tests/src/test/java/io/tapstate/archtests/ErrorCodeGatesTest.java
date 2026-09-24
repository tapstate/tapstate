package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Domain;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The error-code build gates — the mono-repo replacement for the legacy "maintain a
 * spreadsheet" myth. One reactor build sees every first-party code, so global uniqueness / format /
 * registered-domain / stability become a build-time assertion instead of human discipline.
 *
 * <p>Scanning reuses arch-tests' existing capability (ArchUnit's {@code ClassFileImporter} to find
 * the enums, then {@code values()} via reflection) — no second scanning library.
 * Production scope only ({@code DO_NOT_INCLUDE_TESTS}), so throwaway test enums never leak in.
 *
 * <p>Codes belonging to connectors are deliberately out of scope: those arrive in runtime-loaded jars
 * the build cannot see, and are guarded at runtime instead.
 *
 * <p>What is in scope, and is not a connector code, is the wording this repository ships for the
 * connector API's own diagnostic keys. Those keys are a fixed part of that API, the connectors carry
 * them untranslated, and the text is ours; it lives under a reserved prefix so it cannot collide with
 * a first-party code, and it is gated here because nothing else can see it — a first-party code is
 * held in place from both directions, while this text answers to an API outside the build.
 */
class ErrorCodeGatesTest {

    /** <domain>.<symbol>, both lower-case kebab-case; symbol carries no further dot. */
    private static final Pattern SEGMENT = Pattern.compile("[a-z][a-z0-9]*(-[a-z0-9]+)*");

    /** A named {@code {placeholder}} reference inside a catalog message template. */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([a-zA-Z0-9_]+)}");

    /** The mandatory locale every first-party code must be documented in. */
    private static final String CATALOG_RESOURCE = "/messages/en.yml";

    private static List<TapstateErrorCode> codes;

    /** Bundled en catalog: canonical code -> {message, [solution]} (the templates, not rendered). */
    private static Map<String, Map<String, String>> catalog;

    @BeforeAll
    static void scanFirstPartyCodes() {
        JavaClasses classes = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
        List<TapstateErrorCode> found = new ArrayList<>();
        for (JavaClass jc : classes) {
            if (jc.isEnum() && jc.isAssignableTo(TapstateErrorCode.class)) {
                for (Object constant : jc.reflect().getEnumConstants()) {
                    found.add((TapstateErrorCode) constant);
                }
            }
        }
        codes = found;
    }

    @BeforeAll
    static void loadBundledCatalog() {
        try (InputStream in = ErrorCodeGatesTest.class.getResourceAsStream(CATALOG_RESOURCE)) {
            assertThat(in).as("bundled message catalog %s on the test classpath", CATALOG_RESOURCE).isNotNull();
            Map<String, Object> raw = new Yaml().load(in);
            Map<String, Map<String, String>> parsed = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : raw.entrySet()) {
                Map<?, ?> body = (Map<?, ?>) e.getValue();
                Map<String, String> entry = new LinkedHashMap<>();
                Object message = body.get("message");
                Object solution = body.get("solution");
                if (message != null) {
                    entry.put("message", message.toString());
                }
                if (solution != null) {
                    entry.put("solution", solution.toString());
                }
                parsed.put(e.getKey(), entry);
            }
            catalog = parsed;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Test
    @DisplayName("the scan actually finds codes (else every gate below is vacuously green)")
    void scanIsNotEmpty() {
        assertThat(codes).isNotEmpty();
    }

    @Test
    @DisplayName("D5-1: every canonical code is globally unique across all enums and modules")
    void everyCanonicalCodeIsUnique() {
        Map<String, Long> byCode = codes.stream().collect(groupingBy(TapstateErrorCode::code, counting()));
        List<String> duplicates = byCode.entrySet().stream()
                .filter(e -> e.getValue() > 1).map(Map.Entry::getKey).sorted().toList();
        assertThat(duplicates)
                .as("canonical codes claimed by more than one constant")
                .isEmpty();
    }

    @Test
    @DisplayName("D5-2: every code is <domain>.<symbol> kebab-case with a registered domain")
    void everyCodeMatchesFormatAndRegisteredDomain() {
        for (TapstateErrorCode c : codes) {
            String code = c.code();
            int dot = code.indexOf('.');
            assertThat(dot).as("code '%s' must be <domain>.<symbol>", code).isPositive();
            String domain = code.substring(0, dot);
            String symbol = code.substring(dot + 1);
            assertThat(SEGMENT.matcher(domain).matches()).as("domain part of '%s'", code).isTrue();
            assertThat(SEGMENT.matcher(symbol).matches())
                    .as("symbol part of '%s' (lower-case kebab, no nested dot)", code).isTrue();
            assertThat(Domain.isRegistered(domain))
                    .as("domain '%s' of code '%s' is not in the Domain registry", domain, code).isTrue();
        }
    }

    @Test
    @DisplayName("D5-5: the canonical code set matches the checked-in golden (stability lock)")
    void canonicalCodesMatchGolden() throws IOException {
        List<String> scanned = codes.stream().map(TapstateErrorCode::code).distinct().sorted().toList();
        Path goldenFile = Path.of("src", "test", "resources", "error-codes.golden");
        List<String> golden = Files.readAllLines(goldenFile).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .toList();
        assertThat(scanned)
                .as("canonical codes drifted from error-codes.golden — a new code appends to the "
                        + "golden in the same change set; a rename/removal is a breaking contract "
                        + "change (review + golden edit)")
                .isEqualTo(golden);
    }

    @Test
    @DisplayName("D5-3: every first-party code has an en catalog entry with a message")
    void everyCodeHasAnEnglishCatalogEntry() {
        List<String> missing = codes.stream()
                .map(TapstateErrorCode::code)
                .distinct()
                .sorted()
                .filter(code -> {
                    Map<String, String> entry = catalog.get(code);
                    return entry == null || entry.get("message") == null || entry.get("message").isBlank();
                })
                .toList();
        assertThat(missing)
                .as("codes with no en message in %s — add one in the same change set (gate D5-3)", CATALOG_RESOURCE)
                .isEmpty();
    }

    @Test
    @DisplayName("D5-4: each catalog template's {names} match the code's declared placeholders()")
    void catalogPlaceholdersMatchTheCodeContract() {
        Map<String, Set<String>> declared = new HashMap<>();
        for (TapstateErrorCode c : codes) {
            declared.put(c.code(), new TreeSet<>(c.placeholders()));
        }
        for (Map.Entry<String, Map<String, String>> entry : catalog.entrySet()) {
            String code = entry.getKey();
            Set<String> used = new TreeSet<>();
            used.addAll(placeholdersIn(entry.getValue().get("message")));
            used.addAll(placeholdersIn(entry.getValue().get("solution")));
            // Wording for a connector-supplied diagnostic, which is not a first-party code and so has
            // no placeholders() to match. The connector API defines these keys and constructs its test
            // exceptions with them rather than with sentences, and nothing resolves them on the way in;
            // the catalog supplies the wording instead. They live under a reserved prefix that no
            // <domain>.<symbol> code can occupy, so they cannot shadow a first-party entry.
            //
            // The orphan check does not apply, but the reason behind it does: an entry nothing can fill
            // is dead text. Nothing passes arguments for these, so the standing rule is stricter than
            // matching a contract - they must carry no placeholders at all, or they would render with a
            // {name} still in them.
            if (code.startsWith(PDK_TEST_ITEM_PREFIX)) {
                assertThat(used)
                        .as("connector-diagnostic wording '%s' is rendered with no arguments, so its "
                                + "template must carry no placeholders", code)
                        .isEmpty();
                continue;
            }
            assertThat(declared)
                    .as("catalog entry '%s' has no matching first-party error code (orphan template)", code)
                    .containsKey(code);
            assertThat(used)
                    .as("placeholders in the catalog templates for '%s' must equal its declared "
                            + "placeholders() (gate D5-4)", code)
                    .isEqualTo(declared.get(code));
        }
    }

    /** The reserved namespace connector-supplied diagnostics are given wording under. */
    private static final String PDK_TEST_ITEM_PREFIX = "pdk.testitem.";

    /**
     * The connector-diagnostic wordings are exactly the set we mean to carry.
     *
     * <p>The rest of the catalog is held in place from both directions: a code with no entry fails
     * one gate, an entry with no code fails another. These entries answer to the connector API
     * instead, which is outside this build, so neither direction reaches them - and without a list to
     * check against, a wording deleted by accident, a key misspelled so nothing ever resolves it, or
     * one quietly added for a diagnostic nobody emits would all pass unnoticed. Naming the set is what
     * makes those visible; changing it is then a deliberate edit here rather than a silent drift.
     *
     * <p>The names come from the typed test exceptions the connector API defines, each of which
     * carries a failure headline, a reason and a solution. It is a closed set because that API is frozen.
     */
    @Test
    @DisplayName("the connector-diagnostic wordings are exactly the declared set")
    void connectorDiagnosticWordingsAreExactlyTheDeclaredSet() {
        Set<String> expected = new TreeSet<>();
        for (String diagnostic : List.of(
                "check.cdc.privilege", "read.privilege", "write.privilege", "create.table.privilege",
                "pdk.version", "pdk.connection", "check.host.port", "time.consistent", "stream.read")) {
            expected.add(PDK_TEST_ITEM_PREFIX + diagnostic + ".fail");
            expected.add(PDK_TEST_ITEM_PREFIX + diagnostic + ".reason");
            expected.add(PDK_TEST_ITEM_PREFIX + diagnostic + ".solution");
        }

        Set<String> present = new TreeSet<>();
        for (String key : catalog.keySet()) {
            if (key.startsWith(PDK_TEST_ITEM_PREFIX)) {
                present.add(key);
            }
        }

        assertThat(present)
                .as("catalog wordings under %s; add or remove one here in the same change set",
                        PDK_TEST_ITEM_PREFIX)
                .isEqualTo(expected);
        for (String key : present) {
            assertThat(catalog.get(key).get("message"))
                    .as("wording for '%s' must actually say something", key)
                    .isNotBlank();
        }
    }

    private static Set<String> placeholdersIn(String template) {
        Set<String> names = new TreeSet<>();
        if (template == null) {
            return names;
        }
        Matcher m = PLACEHOLDER.matcher(template);
        while (m.find()) {
            names.add(m.group(1));
        }
        return names;
    }
}
