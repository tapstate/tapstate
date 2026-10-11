package io.tapstate.testsupport;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.opentest4j.TestAbortedException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class CiCaseEvidenceTest {
    private final CiCaseEvidence observer = new CiCaseEvidence();

    @Test
    void equalTotalsWithChangedArgumentValuesHaveDifferentIdentities() {
        CiCaseEvidence.Canonical before = CiCaseEvidence.canonical(List.of("case-a", 3));
        CiCaseEvidence.Canonical after = CiCaseEvidence.canonical(List.of("case-b", 3));
        assertThat(before.stable()).isTrue();
        assertThat(after.stable()).isTrue();
        assertThat(CiCaseEvidence.hash(before.json())).isNotEqualTo(CiCaseEvidence.hash(after.json()));
    }

    @Test
    void structuralIdentityDoesNotDependOnMapInsertionOrderOrPathSpelling() {
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("z", new Sample("value", Path.of("fixtures", "unused", "..", "sample"), List.of(1, 2)));
        first.put("a", new BigDecimal("1.000"));
        Map<String, Object> second = new LinkedHashMap<>();
        second.put("a", new BigDecimal("1.000"));
        second.put("z", new Sample("value", Path.of("fixtures", "sample"), List.of(1, 2)));
        assertThat(CiCaseEvidence.canonical(first).stable()).isTrue();
        assertThat(CiCaseEvidence.canonical(first).json()).isEqualTo(CiCaseEvidence.canonical(second).json());
        assertThat(CiCaseEvidence.canonical(List.of(1, 2)).json())
                .isNotEqualTo(CiCaseEvidence.canonical(List.of(2, 1)).json());
    }

    @Test
    void mapOrderingUsesUnicodeScalarUtf8OrderRatherThanUtf16CodeUnits() {
        Map<String, Integer> map = new LinkedHashMap<>();
        map.put("\uD800\uDC00", 1);
        map.put("\uE000", 2);
        String canonical = CiCaseEvidence.canonical(map).json();
        assertThat(canonical.indexOf("\uE000")).isLessThan(canonical.indexOf("\uD800\uDC00"));
    }

    @Test
    void opaqueObjectsDoNotInvokeArbitraryGettersOrObjectToString() {
        CiCaseEvidence.Canonical opaque = CiCaseEvidence.canonical(new Opaque());
        assertThat(opaque.stable()).isFalse();
        assertThat(opaque.reason()).isEqualTo("unsupported-type");
        assertThat(opaque.json()).contains("opaque", Opaque.class.getName()).doesNotContain("@");
        assertThat(CiCaseEvidence.canonical(new AccessorTrap("stored-value")).stable()).isTrue();
    }

    @Test
    void arbitraryCollectionsAreNotAskedToRunTheirOwnCodeDuringObservation() throws Exception {
        AtomicInteger collectionCalls = new AtomicInteger();
        List<Object> list = new AbstractList<>() {
            @Override
            public Object get(int index) {
                collectionCalls.incrementAndGet();
                throw new AssertionError("custom list get was called");
            }

            @Override
            public int size() {
                collectionCalls.incrementAndGet();
                throw new AssertionError("custom list size was called");
            }
        };
        Map<String, Object> map = new AbstractMap<>() {
            @Override
            public Set<Entry<String, Object>> entrySet() {
                collectionCalls.incrementAndGet();
                throw new AssertionError("custom map entrySet was called");
            }
        };
        AtomicInteger invocations = new AtomicInteger();
        Method method = method("namedFixture", String.class, Object.class);
        for (Object value : List.of(list, map, Collections.unmodifiableList(list), Collections.unmodifiableMap(map))) {
            Captured captured = capture(true, () -> observer.interceptTestTemplateMethod(() -> {
                invocations.incrementAndGet();
                return null;
            }, invocation(method, List.of("source-bound-name", value)),
                    context(method, "case", "[test-template-invocation:#1]")));
            assertThat(captured.failure()).isNull();
            assertThat(captured.output()).contains("unsupported-collection-implementation", "\"stable\":false");
        }
        assertThat(collectionCalls).hasValue(0);
        assertThat(invocations).hasValue(4);
    }

    @Test
    void metadataErrorsCannotReplaceTheOriginalInvocationOrItsThrowable() throws Exception {
        Method method = method("fixture", Object.class);
        ExtensionContext brokenMetadata = (ExtensionContext) Proxy.newProxyInstance(ExtensionContext.class.getClassLoader(),
                new Class<?>[] {ExtensionContext.class}, (proxy, called, arguments) -> {
                    throw new AssertionError("metadata reader failed");
                });
        AtomicInteger invocations = new AtomicInteger();
        AssertionError original = new AssertionError("original test failure");
        Captured captured = capture(true, () -> observer.interceptTestMethod(() -> {
            invocations.incrementAndGet();
            throw original;
        }, invocation(method, List.of("value")), brokenMetadata));
        assertThat(invocations).hasValue(1);
        assertThat(captured.failure()).isSameAs(original);
        assertThat(captured.output()).contains("observation-unavailable", "\"stable\":false", "\"status\":\"failed\"")
                .doesNotContain("metadata reader failed", "original test failure");
    }

    @Test
    void invalidUnicodeIsNotCollapsedIntoAFalseStableUtf8Identity() {
        CiCaseEvidence.Canonical high = CiCaseEvidence.canonical("\uD800");
        CiCaseEvidence.Canonical low = CiCaseEvidence.canonical("\uDC00");
        assertThat(high.stable()).isFalse();
        assertThat(low.stable()).isFalse();
        assertThat(high.reason()).isEqualTo("ill-formed-unicode");
        assertThat(low.reason()).isEqualTo("ill-formed-unicode");
        assertThat(CiCaseEvidence.canonical("\uD800\uDC00").stable()).isTrue();
    }

    @Test
    void hiddenCarriersExposeTheirDeclaredSlotWithoutAddressesOrInvocation() throws Exception {
        Supplier<String> carrier = () -> {
            throw new AssertionError("carrier was invoked");
        };
        Method method = method("carrierFixture", String.class, Supplier.class);
        Captured captured = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(method, List.of("source-bound-name", carrier)),
                context(method, "case", "[test-template-invocation:#1]")));
        assertThat(captured.failure()).isNull();
        assertThat(captured.output()).contains("opaque-generated", "java.util.function.Supplier", "\"stable\":false")
                .doesNotContain("$$Lambda", "/0x", "carrier was invoked");
    }

    @Test
    void cyclesAndOversizedValuesAreExplicitlyIncomplete() {
        List<Object> cycle = new ArrayList<>();
        cycle.add(cycle);
        CiCaseEvidence.Canonical cyclic = CiCaseEvidence.canonical(cycle);
        assertThat(cyclic.stable()).isFalse();
        assertThat(cyclic.reason()).isEqualTo("cycle");
        CiCaseEvidence.Canonical large = CiCaseEvidence.canonical("x".repeat(65537));
        assertThat(large.stable()).isFalse();
        assertThat(large.reason()).isEqualTo("size-limit");
    }

    @Test
    void numbersEnumsAndArraysRetainTheirSemanticTypeAndValue() {
        assertThat(CiCaseEvidence.canonical(1).json()).isNotEqualTo(CiCaseEvidence.canonical(1L).json());
        assertThat(CiCaseEvidence.canonical(new int[] {1, 2}).json())
                .isNotEqualTo(CiCaseEvidence.canonical(List.of(1, 2)).json());
        assertThat(CiCaseEvidence.canonical(Mode.FIRST).json()).contains(Mode.class.getName(), "FIRST");
        assertThat(CiCaseEvidence.canonical(Double.NaN).stable()).isTrue();
        assertThat(CiCaseEvidence.canonical(-0.0d).json()).isNotEqualTo(CiCaseEvidence.canonical(0.0d).json());
        assertThat(CiCaseEvidence.canonical(new BigDecimal("1.0")).json())
                .isNotEqualTo(CiCaseEvidence.canonical(new BigDecimal("1.00")).json());
        assertThat(CiCaseEvidence.canonical(new BigDecimal("1E-2147483647")).stable()).isFalse();
    }

    @Test
    void anOriginalFailureIsRethrownUnchangedAndInvokedExactlyOnce() throws Exception {
        AssertionError original = new AssertionError("must-never-appear-in-the-marker");
        AtomicInteger calls = new AtomicInteger();
        Method method = method("fixture", Object.class);
        Captured captured = capture(true, () -> observer.interceptTestMethod(() -> {
            calls.incrementAndGet();
            throw original;
        }, invocation(method, List.of("token-value")), context(method, "display-token-value", "[test:fixture]")));
        assertThat(calls).hasValue(1);
        assertThat(captured.failure()).isSameAs(original);
        assertThat(captured.output().lines()).hasSize(2);
        assertThat(captured.output()).contains("\"phase\":\"start\"", "\"status\":\"failed\"")
                .doesNotContain("token-value", "must-never-appear-in-the-marker", "display-token-value");
    }

    @Test
    void abortsRetainTheirOriginalThrowableAndAreNotReportedAsPassing() throws Exception {
        TestAbortedException original = new TestAbortedException("private abort detail");
        Method method = method("fixture", Object.class);
        Captured captured = capture(true, () -> observer.interceptTestTemplateMethod(() -> {
            throw original;
        }, invocation(method, List.of(4)), context(method, "case", "[test-template-invocation:#7]")));
        assertThat(captured.failure()).isSameAs(original);
        assertThat(captured.output()).contains("\"status\":\"aborted\"").doesNotContain("private abort detail");
    }

    @Test
    void disabledObservationDoesNotSerializeArgumentsOrProduceOutput() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Method method = method("fixture", Object.class);
        Captured captured = capture(false, () -> observer.interceptTestMethod(() -> {
            calls.incrementAndGet();
            return null;
        }, invocation(method, List.of(new Opaque())), context(method, "case", "[test:fixture]")));
        assertThat(calls).hasValue(1);
        assertThat(captured.failure()).isNull();
        assertThat(captured.output()).isEmpty();
    }

    @Test
    void opaqueNamedCasesExposeOnlyDigestsAndTheMissingStructuralProof() throws Exception {
        Method method = method("namedFixture", String.class, Object.class);
        Captured captured = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(method, List.of("secret-label", new Opaque())),
                context(method, "secret-label, secret-password", "[test-template-invocation:#17]")));
        assertThat(captured.failure()).isNull();
        assertThat(captured.output()).contains("\"stable\":false", "\"reason\":\"unsupported-type\"",
                "\"first_string_digest\":\"", "\"repetition\":null", "\"status\":\"executed\"")
                .doesNotContain("secret-label", "secret-password");
    }

    @Test
    void repeatedInvocationsUseTheirDeclaredRepetitionWithoutReusingParameterIndices() throws Exception {
        Method repeated = method("repeatedFixture");
        Captured captured = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(repeated, List.of()), context(repeated, "repeat", "[test-template-invocation:#2]")));
        assertThat(captured.output()).contains("\"identity_kind\":\"repeated\"", "\"repetition\":2",
                "\"stable\":true");
        Method ordinary = method("fixture", Object.class);
        Captured parameter = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(ordinary, List.of(2)), context(ordinary, "parameter", "[test-template-invocation:#2]")));
        assertThat(parameter.output()).contains("\"repetition\":null", "\"identity_kind\":\"template\"");
    }

    @Test
    void injectedTempDirectoriesDoNotReplaceTheSourceCaseIdentity() throws Exception {
        Method method = method("temporaryFixture", String.class, Path.class);
        Captured first = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(method, List.of("case", Path.of("/tmp/first-owned-fixture"))),
                context(method, "case", "[test-template-invocation:#1]")));
        Captured second = capture(true, () -> observer.interceptTestTemplateMethod(() -> null,
                invocation(method, List.of("case", Path.of("/tmp/second-owned-fixture"))),
                context(method, "case", "[test-template-invocation:#1]")));
        assertThat(first.failure()).isNull();
        assertThat(first.output()).isEqualTo(second.output()).contains("\"injected\":true")
                .doesNotContain("first-owned-fixture", "second-owned-fixture");
    }

    @Test
    void dynamicNamesAreHashedAndBoundToTheOriginalFactoryMethod() throws Exception {
        Method method = method("dynamicFixture");
        Captured first = capture(true, () -> observer.interceptDynamicTest(() -> null, null,
                context(method, "dynamic-secret-one", "[dynamic-test:#1]")));
        Captured second = capture(true, () -> observer.interceptDynamicTest(() -> null, null,
                context(method, "dynamic-secret-two", "[dynamic-test:#1]")));
        assertThat(first.output()).contains("\"method\":\"dynamicFixture\"", "\"identity_kind\":\"dynamic\"")
                .doesNotContain("dynamic-secret-one");
        assertThat(first.output()).isNotEqualTo(second.output());
    }

    @Test
    void serviceRegistrationNamesOnlyThisOptInObserver() throws Exception {
        try (var stream = getClass().getClassLoader().getResourceAsStream(
                "META-INF/services/org.junit.jupiter.api.extension.Extension")) {
            assertThat(stream).isNotNull();
            assertThat(new String(stream.readAllBytes(), StandardCharsets.UTF_8))
                    .isEqualTo(CiCaseEvidence.class.getName() + "\n");
        }
    }

    @Test
    void beforeAllRecordsDistinctPredicatesWithoutRevealingConfiguredPaths() throws Exception {
        String property = "tapstate.e2e.connectors-dir";
        String previous = System.getProperty(property);
        try {
            System.clearProperty(property);
            String absent = CiCaseEvidence.gateInputs();
            System.setProperty(property, "  ");
            String blank = CiCaseEvidence.gateInputs();
            System.setProperty(property, "/private/credential-path-sentinel");
            String named = CiCaseEvidence.gateInputs();
            assertThat(absent).contains("\"tapstate.e2e.connectors-dir\":{\"present\":false,\"nonblank\":false");
            assertThat(blank).contains("\"tapstate.e2e.connectors-dir\":{\"present\":true,\"nonblank\":false");
            assertThat(named).contains("\"tapstate.e2e.connectors-dir\":{\"present\":true,\"nonblank\":true")
                    .doesNotContain("credential-path-sentinel");
            AtomicInteger calls = new AtomicInteger();
            Method method = method("dynamicFixture");
            Captured result = capture(true, () -> observer.interceptBeforeAllMethod(() -> {
                calls.incrementAndGet(); return null;
            }, invocation(method, List.of()), context(method, "before all", "[class:fixture]")));
            assertThat(result.failure()).isNull();
            assertThat(calls).hasValue(1);
            assertThat(result.output()).contains(CiCaseEvidence.CLASS_MARKER, "\"phase\":\"start\"", "\"phase\":\"finish\"", "\"status\":\"executed\"")
                    .doesNotContain("credential-path-sentinel");
        } finally {
            if (previous == null) System.clearProperty(property); else System.setProperty(property, previous);
        }
    }

    @Test
    void beforeAllAbortsKeepTheOriginalExceptionAndHashOnlyTheReason() throws Exception {
        TestAbortedException original = new TestAbortedException("private-reason-sentinel");
        AtomicInteger calls = new AtomicInteger();
        Method method = method("dynamicFixture");
        Captured result = capture(true, () -> observer.interceptBeforeAllMethod(() -> {
            calls.incrementAndGet(); throw original;
        }, invocation(method, List.of()), context(method, "before all", "[class:fixture]")));
        assertThat(result.failure()).isSameAs(original);
        assertThat(calls).hasValue(1);
        assertThat(result.output()).contains("\"status\":\"aborted\"", "\"reason_digest\":")
                .doesNotContain("private-reason-sentinel");
    }

    @Test
    void beforeAllObservationErrorsNeverReplaceTheOriginalOutcome() throws Exception {
        Method method = method("dynamicFixture");
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException original = new IllegalStateException("original failure");
        ExtensionContext broken = (ExtensionContext) Proxy.newProxyInstance(ExtensionContext.class.getClassLoader(),
                new Class<?>[] {ExtensionContext.class}, (proxy, called, arguments) -> { throw new AssertionError("metadata failure"); });
        Captured result = capture(true, () -> observer.interceptBeforeAllMethod(() -> {
            calls.incrementAndGet(); throw original;
        }, invocation(method, List.of()), broken));
        assertThat(result.failure()).isSameAs(original);
        assertThat(calls).hasValue(1);
        assertThat(result.output()).contains("\"stable\":false", "class-observation-unavailable", "\"status\":\"failed\"");
    }

    @Test
    void disabledBeforeAllObservationAddsNoReceiptsOrContextAccess() throws Exception {
        Method method = method("dynamicFixture");
        AtomicInteger calls = new AtomicInteger();
        Captured result = capture(false, () -> observer.interceptBeforeAllMethod(() -> {
            calls.incrementAndGet(); return null;
        }, invocation(method, List.of()), null));
        assertThat(result.failure()).isNull();
        assertThat(calls).hasValue(1);
        assertThat(result.output()).isEmpty();
    }

    private static Method method(String name, Class<?>... arguments) throws NoSuchMethodException {
        return CiCaseEvidenceTest.class.getDeclaredMethod(name, arguments);
    }

    private static ReflectiveInvocationContext<Method> invocation(Method method, List<Object> arguments) {
        return new ReflectiveInvocationContext<>() {
            @Override
            public Class<?> getTargetClass() {
                return method.getDeclaringClass();
            }

            @Override
            public Method getExecutable() {
                return method;
            }

            @Override
            public List<Object> getArguments() {
                return arguments;
            }

            @Override
            public Optional<Object> getTarget() {
                return Optional.empty();
            }
        };
    }

    private static ExtensionContext context(Method method, String display, String uniqueId) {
        return (ExtensionContext) Proxy.newProxyInstance(ExtensionContext.class.getClassLoader(),
                new Class<?>[] {ExtensionContext.class}, (proxy, called, arguments) -> switch (called.getName()) {
                    case "getTestClass" -> Optional.of(CiCaseEvidenceTest.class);
                    case "getTestMethod" -> Optional.of(method);
                    case "getParent" -> Optional.empty();
                    case "getDisplayName" -> display;
                    case "getUniqueId" -> uniqueId;
                    default -> throw new UnsupportedOperationException(called.getName());
                });
    }

    private static Captured capture(boolean enabled, Operation operation) {
        String previous = System.getProperty(CiCaseEvidence.ENABLED);
        String previousDirectory = System.getProperty(CiCaseEvidence.CLASS_DIRECTORY);
        PrintStream output = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Throwable failure = null;
        try (PrintStream stream = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            System.setProperty(CiCaseEvidence.ENABLED, Boolean.toString(enabled));
            System.clearProperty(CiCaseEvidence.CLASS_DIRECTORY);
            System.setOut(stream);
            try {
                operation.run();
            } catch (Throwable original) {
                failure = original;
            }
        } finally {
            System.setOut(output);
            if (previousDirectory == null) System.clearProperty(CiCaseEvidence.CLASS_DIRECTORY);
            else System.setProperty(CiCaseEvidence.CLASS_DIRECTORY, previousDirectory);
            if (previous == null) {
                System.clearProperty(CiCaseEvidence.ENABLED);
            } else {
                System.setProperty(CiCaseEvidence.ENABLED, previous);
            }
        }
        return new Captured(bytes.toString(StandardCharsets.UTF_8), failure);
    }

    private record Captured(String output, Throwable failure) {
    }

    @FunctionalInterface
    private interface Operation {
        void run() throws Throwable;
    }

    public record Sample(String label, Path path, List<Integer> values) {
    }

    private record AccessorTrap(String label) {
        @Override
        public String label() {
            throw new AssertionError("a custom record accessor was called");
        }
    }

    private enum Mode {
        FIRST
    }

    private static final class Opaque {
        public String getSecret() {
            throw new AssertionError("an arbitrary getter was called");
        }

        @Override
        public String toString() {
            throw new AssertionError("an arbitrary toString was called");
        }
    }

    private void fixture(Object argument) {
    }

    private void namedFixture(String label, Object argument) {
    }

    @RepeatedTest(3)
    private void repeatedFixture() {
    }

    private void carrierFixture(String label, Supplier<?> carrier) {
    }

    private void temporaryFixture(String label, @TempDir Path directory) {
    }

    private void dynamicFixture() {
    }
}
