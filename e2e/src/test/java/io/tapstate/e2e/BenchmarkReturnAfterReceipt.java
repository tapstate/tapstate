package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

/** Pure correspondence and arithmetic over acquired facts; it never queries a running root. */
final class BenchmarkReturnAfterReceipt {
    static final String FINAL_PREFIX = "benchmark-return-work-final=";
    // This is a predeclared acquired Failsafe-output cap, distinct from manifest transport limits.
    static final int XML_BYTES = 128 * 1024 * 1024;
    private static final List<String> UNITS = List.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates");

    record Registration(String runId, String inputSha256, BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile authenticatedProfile,
                        BenchmarkReturnWorkBudgetAdmission.Runtime root,
                        BenchmarkReturnWorkBudgetProducerSeal.Registration producer,
                        BenchmarkReturnWorkBudgetProfile.WorkloadInput workload,
                        BenchmarkReturnWorkBudgetDefinitions.OutputPlan reportPlan, String reportPath, String producerBodyPath,
                        String testClass, String testMethod,
                        BenchmarkReturnWorkBudget.Work cachedColdUpper,
                        BenchmarkReturnWorkBudget.Work cachedFactoryUpper,
                        BenchmarkReturnWorkBudget.Work cachedTerminalUpper) {
        Registration {
            text(runId, 128); digest(inputSha256); text(testClass, 256); text(testMethod, 256);
            require(authenticatedProfile != null && root != null && producer != null && workload != null && reportPlan != null,
                    "registered typed source inputs are missing");
            require(runId.equals(root.runId()) && runId.equals(producer.runId()) && inputSha256.equals(producer.inputSha256())
                    && root.pid() != producer.runtime().pid(), "registered root, producer or run differs");
            absolute(reportPath); absolute(producerBodyPath);
            require(cachedColdUpper != null && cachedFactoryUpper != null && cachedTerminalUpper != null,
                    "registered fixed source tails are missing");
            var prepared = authenticatedProfile.prepared();
            require(runId.equals(prepared.inputs().runId()) && inputSha256.equals(prepared.inputs().inputSha256())
                    && workload.equals(prepared.inputs().workload()) && reportPlan.equals(prepared.fieldsRegisteredOutputPlan()),
                    "cached authenticated registration differs");
        }
        BenchmarkReturnWorkBudget.Definition definition() { return authenticatedProfile.definition(); }
    }

    /** These are borrowed acquired bytes until verify applies the independently registered caps. */
    record ByteFact(String path, BenchmarkReturnWorkBudgetBindings.Blob fingerprint, byte[] bytes) { }

    /** The binding verifier remains an independent pure component; this type carries its cached result. */
    record BindingFacts(BenchmarkReturnWorkBudgetBindings.HelperManifest helper,
                        BenchmarkReturnWorkBudgetBindings.JdkManifest jdk,
                        BenchmarkReturnWorkBudgetBindings.ActualFacts actual,
                        BenchmarkReturnWorkBudgetBindings.OwnedRuntimes owners,
                        List<String> unresolvedCoverage) {
        BindingFacts { unresolvedCoverage = List.copyOf(unresolvedCoverage); }
    }

    record Inputs(Map<String, Object> closedSnapshot, ByteFact report, ByteFact protocolStdout, ByteFact failsafeXml,
                  BenchmarkReturnWorkBudgetAdmission.Runtime actualFailsafeRoot, BindingFacts bindings,
                  ByteFact producerBody, BenchmarkReturnWorkBudgetProducerSeal.Bound producer,
                  Map<String, Object> originalProducerOutcome) { }

    record Receipt(BenchmarkReturnWorkBudget.Work sourceUpper, Map<String, Object> evidence) {
        Receipt { evidence = Map.copyOf(evidence); }
    }

    private BenchmarkReturnAfterReceipt() { }

    static Receipt verify(Registration registered, Inputs acquired) {
        require(BenchmarkReturnWorkBudget.current() == null, "after-receipt consumer must be outside the original root scope");
        require(registered != null && acquired != null && acquired.closedSnapshot() != null, "acquired root facts are missing");
        require(registered.root().equals(acquired.actualFailsafeRoot()), "actual Failsafe root identity differs");
        require(registered.cachedColdUpper().equals(BenchmarkReturnRootClockWork.coldInitializationUpper())
                && registered.cachedFactoryUpper().equals(BenchmarkReturnWorkBudgetSourceTails.factoryUpper(registered.workload()))
                && registered.cachedTerminalUpper().equals(BenchmarkReturnWorkBudgetSourceTails.terminalUpper(registered.reportPlan())),
                "cached source tail differs from its pure source recipe");

        var closedPlan = closedPlan();
        Map<String, Object> closed = object(freeze(acquired.closedSnapshot(), closedPlan));
        var counters = BenchmarkReturnWorkBudgetAdmission.verifyCounters(registered.definition(), registered.root(), closed);
        var selfUpper = counters.selfUpper();
        byte[] reportBytes = bytes(acquired.report(), registered.reportPlan().encodedUpperBytes());
        require(registered.reportPath().equals(acquired.report().path()), "actual report path differs");
        var output = object(closed.get("output"));
        require(number(output, "actualBytes") == reportBytes.length
                && hash(reportBytes).equals(output.get("sha256"))
                && number(output, "independentUpperBytes") <= registered.reportPlan().encodedUpperBytes()
                && registered.reportPlan().sha256().equals(output.get("priorTreePlanSha256")), "actual report hash or prior plan differs");
        Map<String, Object> report = canonical(reportBytes, registered.reportPlan());
        var prefix = object(report.get("returnWorkBudgetPrefix"));
        require(registered.runId().equals(prefix.get("runId"))
                && registered.definition().sourceFormulaSha256().equals(prefix.get("sourceFormulaSha256")), "report root source identity differs");
        var reportOwner = object(prefix.get("rootRuntime"));
        var closedOwner = object(closed.get("rootRuntime"));
        require(reportOwner.keySet().equals(closedOwner.keySet())
                && number(reportOwner, "pid") == number(closedOwner, "pid")
                && number(reportOwner, "jvmStartTimeMillis") == number(closedOwner, "jvmStartTimeMillis"),
                "report root owner differs");
        var profile = object(report.get("returnWorkBudgetProfile"));
        require(registered.runId().equals(profile.get("runId")) && registered.inputSha256().equals(profile.get("inputSha256")),
                "report registration input differs");

        long stdoutUpper = Math.addExact(closedPlan.encodedUpperBytes(), FINAL_PREFIX.length() + 1L);
        byte[] stdoutBytes = bytes(acquired.protocolStdout(), stdoutUpper);
        String stdout = utf8(stdoutBytes);
        require(stdout.endsWith("\n") && stdout.indexOf('\n') == stdout.length() - 1
                && stdout.startsWith(FINAL_PREFIX), "final stdout is partial, duplicated or contains another protocol line");
        byte[] finalJson = stdout.substring(FINAL_PREFIX.length(), stdout.length() - 1).getBytes(StandardCharsets.UTF_8);
        require(JsonWriter.write(canonical(finalJson, closedPlan)).equals(JsonWriter.write(closed)),
                "final stdout and original CLOSED snapshot differ");
        Map<String, Object> outcome = successfulCase(bytes(acquired.failsafeXml(), XML_BYTES), registered, stdoutBytes);
        BindingFacts binding = acquired.bindings();
        require(binding != null && binding.owners() != null
                && registered.root().pid() == binding.owners().root().pid()
                && registered.root().jvmStartTimeMillis() == binding.owners().root().jvmStartTimeMillis()
                && registered.producer().runtime().pid() == binding.owners().producer().pid()
                && registered.producer().runtime().jvmStartTimeMillis() == binding.owners().producer().jvmStartTimeMillis(),
                "verified binding owners or input differ");
        Map<String, Object> verified = BenchmarkReturnWorkBudgetBindings.verify(registered.authenticatedProfile(),
                binding.helper(), binding.jdk(), binding.actual(), binding.owners());
        require("REGISTERED_BINDING_FACTS_MATCHED".equals(verified.get("state"))
                && registered.runId().equals(verified.get("runId")) && registered.inputSha256().equals(verified.get("inputSha256"))
                && registered.definition().definitionId().equals(verified.get("rootDefinitionId"))
                && registered.definition().sourceFormulaSha256().equals(verified.get("rootFormulaSha256"))
                && BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256.equals(verified.get("producerFormulaSha256")),
                "cached independent binding result differs");
        rejectQualificationClaims(verified);

        require(acquired.producer() != null && acquired.originalProducerOutcome() != null, "original producer completion is missing");
        BenchmarkReturnWorkBudgetProducerSeal.requireExited(acquired.originalProducerOutcome(), registered.producer());
        var producerPlan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(65536, 12, 1024, 65536, 128, 24);
        byte[] bodyBytes = bytes(acquired.producerBody(), BenchmarkReturnWorkBudgetProducerSeal.MAX_BODY_BYTES);
        require(registered.producerBodyPath().equals(acquired.producerBody().path())
                && bodyBytes.length == acquired.producer().bodyBytes() && hash(bodyBytes).equals(acquired.producer().bodySha256()),
                "producer bound body hash, length or path differs");
        Map<String, Object> producerBody = canonical(bodyBytes, producerPlan);
        var cachedBody = freeze(acquired.producer().body(), producerPlan);
        // The immutable cached map need not preserve the original byte order or integer wrapper types.
        require(producerBody.equals(canonical(JsonWriter.write(cachedBody).getBytes(StandardCharsets.UTF_8), producerPlan)),
                "producer bound body content differs from acquired bytes");
        require(registered.runId().equals(producerBody.get("runId")) && registered.inputSha256().equals(producerBody.get("inputSha256"))
                && number(producerBody, "pid") == registered.producer().runtime().pid()
                && number(producerBody, "jvmStartTimeMillis") == registered.producer().runtime().jvmStartTimeMillis(),
                "producer root, runtime or input differs");
        require("SHUTDOWN_HOOK".equals(producerBody.get("stage")) && "SHUTDOWN_HOOK".equals(producerBody.get("lifecycle"))
                && "RECORDED_STRUCTURAL_DIAGNOSTIC".equals(producerBody.get("state")), "producer completion is refused");
        require(number(producerBody, "generation") == acquired.producer().generation() && acquired.producer().mappingForceAttempts() == 2
                && number(producerBody, "pending") == 0 && number(producerBody, "openWriters") == 0
                && number(producerBody, "omitted") == 0 && number(producerBody, "lateMutations") == 0
                && Boolean.FALSE.equals(producerBody.get("overflow")) && Boolean.FALSE.equals(producerBody.get("overBudget"))
                && ((List<?>) producerBody.get("gaps")).isEmpty()
                && hash(registered.producer().nonce().getBytes(StandardCharsets.US_ASCII)).equals(producerBody.get("nonceSha256")),
                "producer sealed lifecycle or nonce differs");
        digest(acquired.producer().companionSha256());
        var producerEvaluation = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(producerBody);
        require(Boolean.TRUE.equals(producerEvaluation.get("countPairingComplete"))
                && Boolean.TRUE.equals(producerEvaluation.get("sealTailIncluded")), "producer counts or seal tail are incomplete");
        var producerUpper = work(producerEvaluation.get("recomputedSourceUpper"));

        var partitions = new LinkedHashMap<String, Object>();
        partitions.put("rootActors", counters.totalUpper().evidence()); partitions.put("rootSelf", selfUpper.evidence());
        partitions.put("producerIncludingSealTail", producerUpper.evidence());
        partitions.put("fixedRootCold", registered.cachedColdUpper().evidence());
        partitions.put("cachedFactory", registered.cachedFactoryUpper().evidence());
        partitions.put("terminalRenderPrintReturnAndRelease", registered.cachedTerminalUpper().evidence());
        BenchmarkReturnWorkBudget.Work total;
        try { total = counters.totalUpper().plus(selfUpper).plus(producerUpper).plus(registered.cachedColdUpper())
                .plus(registered.cachedFactoryUpper()).plus(registered.cachedTerminalUpper()); }
        catch (ArithmeticException overflow) { throw new AssertionError("after receipt source sum overflowed", overflow); }
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("state", "COLLECTION_METHOD_AFTER_FACTS_MATCHED_DIAGNOSTIC_ONLY");
        evidence.put("runId", registered.runId()); evidence.put("inputSha256", registered.inputSha256());
        evidence.put("rootRuntime", Map.of("pid", registered.root().pid(), "jvmStartTimeMillis", registered.root().jvmStartTimeMillis()));
        evidence.put("caseOutcome", outcome); evidence.put("reportSha256", hash(reportBytes)); evidence.put("reportBytes", reportBytes.length);
        evidence.put("finalStdoutSha256", hash(stdoutBytes)); evidence.put("failsafeXmlSha256", acquired.failsafeXml().fingerprint().sha256());
        evidence.put("sourceUpper", total.evidence()); evidence.put("partitions", Map.copyOf(partitions));
        evidence.put("sourceUpperIsActualWork", false);
        evidence.put("overlap", "CONSERVATIVE_LOCAL_SELF_FINISH_AND_TERMINAL_OVERLAP_ALLOWED_PRODUCER_SEAL_INCLUDED_ONCE");
        evidence.put("completionScope", "FINAL_RENDER_PRINTLN_METHOD_RETURN_AND_TRY_WITH_RESOURCES_RELEASE");
        evidence.put("rootOsTermination", Map.of("state", "UNKNOWN", "reason", "ORIGINAL_ROOT_PROCESS_NOT_OWNED_BY_THIS_COMPONENT"));
        evidence.put("unresolvedCoverage", binding.unresolvedCoverage());
        evidence.put("inputAuthenticationScope", "PURE_CHECKS_OF_ACQUIRED_BYTES_AND_CACHED_BINDING_RESULTS_IO_PROVENANCE_REMAINS_EXTERNAL");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> evidence.put(flag, false));
        return new Receipt(total, evidence);
    }

    static BenchmarkReturnWorkBudgetDefinitions.OutputPlan closedPlan() {
        long nodes = Math.addExact(1024, Math.addExact(128L * 57, 8L * 128));
        long text = Math.addExact(4096, Math.addExact(1024L * 57, 128L * 128));
        return new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(nodes, 16, 65536, text, 128, 24);
    }

    private static byte[] bytes(ByteFact fact, long upper) {
        require(fact != null && fact.fingerprint() != null && fact.bytes() != null && upper > 0
                && fact.bytes().length <= upper && fact.fingerprint().bytes() == fact.bytes().length,
                "acquired bytes exceed their registered prior bound or length");
        absolute(fact.path()); byte[] copy = fact.bytes().clone();
        require(hash(copy).equals(fact.fingerprint().sha256()), "acquired byte hash differs"); return copy;
    }
    private static String utf8(byte[] bytes) {
        String raw = new String(bytes, StandardCharsets.UTF_8);
        require(java.util.Arrays.equals(bytes, raw.getBytes(StandardCharsets.UTF_8)), "acquired text is not canonical UTF-8"); return raw;
    }
    private static Map<String, Object> canonical(byte[] bytes, BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan) {
        String raw = utf8(bytes);
        BenchmarkReturnWorkBudgetDefinitions.requireJsonShape(raw, (int) Math.min(Integer.MAX_VALUE - 1L, plan.encodedUpperBytes()),
                plan.depthUpper(), (int) Math.min(Integer.MAX_VALUE - 1L, plan.nodeUpper()), plan.leafUtf8Upper());
        Object parsed = JsonReader.parse(raw); Object frozen = freeze(parsed, plan);
        require(raw.equals(JsonWriter.write(frozen)), "acquired JSON is noncanonical or contains duplicate fields"); return object(frozen);
    }
    private static Object freeze(Object value, BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan) {
        return BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(value, plan).value();
    }
    private static Map<String, Object> successfulCase(byte[] xml, Registration registered, byte[] finalProtocol) {
        try {
            var factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            factory.setXIncludeAware(false); factory.setExpandEntityReferences(false);
            var document = factory.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
            var suite = document.getDocumentElement();
            require(suite.getTagName().equals("testsuite") && suite.getAttribute("tests").equals("1")
                    && suite.getAttribute("failures").equals("0") && suite.getAttribute("errors").equals("0")
                    && suite.getAttribute("skipped").equals("0"), "Failsafe suite is not one successful case");
            var cases = document.getElementsByTagName("testcase"); require(cases.getLength() == 1, "Failsafe testcase is duplicated or missing");
            var selected = (org.w3c.dom.Element) cases.item(0);
            require(selected.getAttribute("classname").equals(registered.testClass())
                    && selected.getAttribute("name").equals(registered.testMethod())
                    && selected.getElementsByTagName("failure").getLength() == 0
                    && selected.getElementsByTagName("error").getLength() == 0
                    && selected.getElementsByTagName("skipped").getLength() == 0, "Failsafe selected case identity or success differs");
            var outputs = selected.getElementsByTagName("system-out");
            require(outputs.getLength() == 1, "selected Failsafe system-out is missing or duplicated");
            String output = outputs.item(0).getTextContent();
            int found = -1, count = 0, at = 0;
            while ((at = output.indexOf(FINAL_PREFIX, at)) >= 0) {
                if (at == 0 || output.charAt(at - 1) == '\n') { found = at; count++; }
                at = Math.addExact(at, FINAL_PREFIX.length());
            }
            require(count == 1, "selected Failsafe final marker is missing or duplicated");
            int end = output.indexOf('\n', found);
            require(end >= 0 && java.util.Arrays.equals(output.substring(found, end + 1).getBytes(StandardCharsets.UTF_8), finalProtocol),
                    "selected Failsafe final marker differs from acquired protocol");
            return Map.of("state", "SUCCESS", "className", registered.testClass(), "methodName", registered.testMethod(),
                    "rootPid", registered.root().pid(), "rootJvmStartTimeMillis", registered.root().jvmStartTimeMillis(),
                    "ownerBindingScope", "SELECTED_CASE_SYSTEM_OUT_MATCHES_ORIGINAL_ROOT_CLOSED_PROTOCOL");
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError("Failsafe outcome XML is unavailable or malformed", failure); }
    }
    private static void rejectQualificationClaims(Map<String, Object> data) {
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) if (data.containsKey(flag)) require(Boolean.FALSE.equals(data.get(flag)), "binding claimed a qualification");
    }
    private static BenchmarkReturnWorkBudget.Work work(Object value) {
        var data = object(value); require(data.keySet().equals(Set.copyOf(UNITS)), "work vector unit roster differs");
        return new BenchmarkReturnWorkBudget.Work(number(data, "calls"), number(data, "requests"), number(data, "records"),
                number(data, "frames"), number(data, "bytes"), number(data, "passes"), number(data, "elements"), number(data, "updates"));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> object(Object value) {
        require(value instanceof Map<?, ?>, "required proof object is missing"); return (Map<String, Object>) value;
    }
    private static long number(Map<?, ?> data, String key) {
        Object value = data.get(key); require(value instanceof Long || value instanceof Integer, "exact integer proof field is missing");
        long result = ((Number) value).longValue(); require(result >= 0, "proof integer is negative"); return result;
    }
    private static String hash(byte[] bytes) {
        try { return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void digest(String value) { require(value != null && value.matches("[a-f0-9]{64}"), "proof digest differs"); }
    private static void absolute(String path) {
        text(path, 4096); require(Path.of(path).isAbsolute() && Path.of(path).equals(Path.of(path).normalize()), "proof path is noncanonical");
    }
    private static void text(String value, int maximum) { require(value != null && !value.isBlank() && value.length() <= maximum, "proof text is missing or oversized"); }
    private static void require(boolean value, String reason) { if (!value) throw new AssertionError("return after receipt " + reason); }
}
