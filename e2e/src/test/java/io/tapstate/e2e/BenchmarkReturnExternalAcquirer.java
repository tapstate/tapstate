package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Element;
import io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.Runtime;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** External acquisition only. This process does not activate the original measurement scope. */
final class BenchmarkReturnExternalAcquirer {
    static final int INPUT_BYTES = 2 * 1024 * 1024;
    static final int REPORT_ACQUISITION_BYTES = 128 * 1024 * 1024;
    static final int OUTPUT_BYTES = 2 * 1024 * 1024;
    static final int BUFFER = 8192;
    private static final Set<String> UNITS = Set.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates");
    @FunctionalInterface interface Streams { InputStream open(Path path) throws IOException; }
    private static final Streams FILES = path -> Files.newInputStream(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
    record Acquired(Path path, byte[] bytes, Blob blob) {
        BenchmarkReturnAfterReceipt.ByteFact byteFact() {
            return new BenchmarkReturnAfterReceipt.ByteFact(path.toString(), blob, bytes);
        }
    }
    record SelectedCase(byte[] protocol, Map<String, Object> closed, Map<String, String> properties) { }
    private final List<Map<String, Object>> reads = new ArrayList<>();
    private final Map<String, Object> facts = new LinkedHashMap<>();

    private BenchmarkReturnExternalAcquirer() { }

    static AcquiredCompletion acquireCompletion(Path registrationPath, String registrationSha,
            Path reportPath, Path xmlPath, Path output) throws Exception {
        require(BenchmarkReturnWorkBudget.current() == null && BenchmarkReturnWorkBudgetRun.current() == null,
                "external completion acquisition must be outside every original Run");
        require(output != null && output.isAbsolute() && output.equals(output.normalize())
                && output.getParent().toRealPath().equals(output.getParent())
                && !Files.exists(output, LinkOption.NOFOLLOW_LINKS),
                "output directory must be new under an existing canonical parent");
        Files.createDirectory(output);
        var verifier = new BenchmarkReturnExternalAcquirer();
        try {
            var completed = verifier.verify(registrationPath, registrationSha, reportPath, xmlPath, output);
            write(output.resolve("after-receipt.json"), completed.evidence());
            return completed;
        } catch (Exception | Error primary) {
            var refused = new LinkedHashMap<String, Object>(verifier.facts);
            refused.put("state", "UNKNOWN"); refused.put("failure", throwable(primary));
            refused.put("acquiredReads", List.copyOf(verifier.reads));
            refused.put("originalMethodCredit", "REFUSED_NO_RETRY_OR_REFILL");
            BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> refused.put(flag, false));
            try { write(output.resolve("refusal.json"), refused); }
            catch (Exception | Error recordingFailure) { if (recordingFailure != primary) primary.addSuppressed(recordingFailure); }
            throw primary;
        }
    }

    /** Only real, complete acquisition can construct this authority carrier. */
    static final class AcquiredCompletion {
        private final BenchmarkReturnAfterReceipt.Registration registration;
        private final BenchmarkReturnAfterReceipt.Receipt after;
        private final Map<String, Object> report, evidence;
        private final Blob registrationBytes;
        private final HelperManifest helper;
        private final BenchmarkReturnWorkBudgetBindingsIo.Result staticBytes;
        private AcquiredCompletion(BenchmarkReturnAfterReceipt.Registration registration,
                BenchmarkReturnAfterReceipt.Receipt after, Map<String, Object> report,
                Map<String, Object> evidence, Blob registrationBytes, HelperManifest helper,
                BenchmarkReturnWorkBudgetBindingsIo.Result staticBytes) {
            this.registration = registration; this.after = after; this.report = report;
            this.evidence = Map.copyOf(evidence); this.registrationBytes = registrationBytes;
            this.helper = helper; this.staticBytes = staticBytes;
        }
        BenchmarkReturnAfterReceipt.Registration registration() { return registration; }
        BenchmarkReturnAfterReceipt.Receipt after() { return after; }
        Map<String, Object> report() { return report; }
        Map<String, Object> evidence() { return evidence; }
        Blob registrationBytes() { return registrationBytes; }
        FileFact application() {
            return staticBytes.files().stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR)
                    .findFirst().orElseThrow();
        }
        BenchmarkReturnMethodAdmission.Preflight completedPreflight() {
            var windows = list(object(report.get("measurement")).get("measuredPhaseWindows"), 6);
            require(!windows.isEmpty(), "original measured phase windows are missing");
            var clock = object(object(windows.getFirst()).get("targetClockQualification"));
            var metadata = object(object(clock.get("nativeClockBefore")).get("rootMetadata"));
            require(Boolean.TRUE.equals(metadata.get("rawRetained")), "original root native raw metadata is missing");
            var nativeFile = staticBytes.files().stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY)
                    .findFirst().orElseThrow();
            return new BenchmarkReturnMethodAdmission.Preflight(BenchmarkReturnMethodAdmission.METHOD,
                    BenchmarkReturnMethodAdmission.structuralRecipe(registration.authenticatedProfile()), helper, staticBytes,
                    new BenchmarkCausalClock.Identity(registration.root().pid(), registration.root().jvmStartTimeMillis()),
                    nativeFile.path(), text(metadata.get("raw"), 8192));
        }
    }

    private AcquiredCompletion verify(Path registrationPath, String registrationSha, Path reportPath, Path xmlPath, Path output) throws Exception {
        require(BenchmarkReturnWorkBudget.current() == null, "external verifier must not activate an original-run scope");
        var registrationBytes = acquire(registrationPath, INPUT_BYTES, null, reads, FILES);
        require(registrationBytes.blob().sha256().equals(registrationSha), "registration digest differs from the independently supplied digest");
        preserve(output, registrationBytes, "prospective-inputs.acquired.json");
        var registration = json(registrationBytes.bytes(), new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(
                65536, 16, 65536, INPUT_BYTES, 256, 24));
        require(number(registration, "schemaVersion") == 1, "prospective schema differs");
        acquisitionCaps(object(registration.get("acquisitionCaps")));
        String runId = text(registration.get("runId"), 128), inputSha = digest(text(registration.get("inputSha256"), 64));
        facts.put("runId", runId); facts.put("inputSha256", inputSha); facts.put("registration", blobEvidence(registrationBytes));
        require(reportPath.toString().equals(text(registration.get("reportPath"), 4096)), "report path differs from prospective registration");
        var declared = files(registration.get("files"));
        var app = file(declared, BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR);
        var library = file(declared, BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY);
        var helperFile = file(declared, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST);
        var jdkFile = file(declared, BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST);
        var scalarFiles = declared.stream().map(f -> new BenchmarkReturnWorkBudgetProfile.FileInput(f.role(), f.blob().sha256(), f.blob().bytes())).toList();
        var flags = registeredFlags(registration);
        String workloadId = text(registration.get("workloadId"), 32);
        require(List.of("copy", "stateless", "stateful").contains(workloadId), "original workload differs");
        var workload = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(workloadId);
        var prepared = BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs(
                runId, inputSha, workload, scalarFiles, flags, writerBinding(registration)));
        var authenticated = BenchmarkReturnWorkBudgetBindings.authenticate(prepared);
        var plan = prepared.fieldsRegisteredOutputPlan();
        compareCachedRegistration(registration, authenticated, workload, plan);

        var helper = BenchmarkReturnBindingsManifestReader.readHelper(path(helperFile.path()), helperFile.blob());
        var jdk = BenchmarkReturnBindingsManifestReader.readJdk(path(jdkFile.path()), jdkFile.blob());
        require(helper.runId().equals(runId) && helper.inputSha256().equals(inputSha)
                && helper.buildId().equals(text(registration.get("buildId"), 128)), "helper registration identity differs");
        require(helper.failsafeClasspath().equals(strings(registration.get("orderedClasspath"), MAX_CLASSPATH, 4096)),
                "registered ordered classpath differs");
        var preflight = object(registration.get("failsafeXml"));
        var preflightBytes = acquire(path(text(preflight.get("path"), 4096)), BenchmarkReturnAfterReceipt.XML_BYTES,
                blob(preflight), reads, FILES);
        require(preflightBytes.blob().equals(helper.failsafeXml()), "actual preflight XML differs from the helper manifest");
        facts.put("preflightXml", blobEvidence(preflightBytes));

        var reportBytes = acquire(reportPath, Math.min(plan.encodedUpperBytes(), REPORT_ACQUISITION_BYTES), null, reads, FILES);
        preserve(output, reportBytes, "report.acquired.json"); facts.put("originalReport", blobEvidence(reportBytes));
        var report = json(reportBytes.bytes(), plan);
        var caseDefinition = object(registration.get("expectedCase"));
        String testClass = text(caseDefinition.get("className"), 256), testMethod = text(caseDefinition.get("name"), 256);
        require(testClass.equals(RealBenchmarkForkDriverIT.class.getName()) && testMethod.equals(switch (workloadId) {
            case "copy" -> "copyForkUsesRealTargetChangesAndItsOwnTerminalPosition";
            case "stateless" -> "statelessForkUsesPgoutputAndItsOwnTerminalPosition";
            case "stateful" -> "statefulForkObservesColdReadAndCdcAcrossBothTargets";
            default -> throw new AssertionError("unknown original case");
        }), "registered original testcase differs");
        var outcomeBytes = acquire(xmlPath, BenchmarkReturnAfterReceipt.XML_BYTES, null, reads, FILES);
        preserve(output, outcomeBytes, "actual-failsafe.acquired.xml"); facts.put("actualOutcomeXml", blobEvidence(outcomeBytes));
        require(!outcomeBytes.path().equals(preflightBytes.path()), "original fork outcome XML must be separate from the pure preflight XML");
        var selected = selectedCase(outcomeBytes.bytes(), testClass, testMethod);
        var closed = selected.closed();
        var producerFacts = object(report.get("returnWorkBudgetProducer"));
        require("POST_EXIT_BYTES_AND_SOURCE_ARITHMETIC_BOUND".equals(producerFacts.get("state")), "retained producer outcome is incomplete");
        var originalBindings = object(producerFacts.get("actualInputBindings"));
        var actualRoot = object(originalBindings.get("actualRootRuntime"));
        var root = new Runtime(Owner.ROOT, runId, number(actualRoot, "pid"), number(actualRoot, "jvmStartTimeMillis"),
                text(actualRoot.get("javaHome"), 4096), text(actualRoot.get("javaVersion"), 128));
        require(jdk.javaHome().equals(root.javaHome()) && jdk.javaVersion().equals(root.javaVersion()), "retained root JDK differs");
        require(number(object(closed.get("rootRuntime")), "pid") == root.pid()
                && number(object(closed.get("rootRuntime")), "jvmStartTimeMillis") == root.jvmStartTimeMillis(), "closed root differs from the original retained root");
        require(root.javaHome().equals(selected.properties().get("java.home"))
                && root.javaVersion().equals(selected.properties().get("java.version")), "actual selected Failsafe JDK differs from the retained root");
        verifyRootProperties(selected.properties(), registration, declared, flags);
        var classpath = classpath(selected.properties().get("surefire.test.class.path"));
        require(classpath.equals(helper.failsafeClasspath())
                && classpath.equals(strings(originalBindings.get("actualOrderedClasspath"), MAX_CLASSPATH, 4096)),
                "original root, selected XML or registered classpath differs");
        requireOriginalBindings(originalBindings, authenticated, helper);

        var runtimeEvidence = object(object(report.get("measurement")).get("runtimeEvidence"));
        var originalPid = number(object(runtimeEvidence.get("productAtCapture")), "pid");
        var originalChildPackets = list(runtimeEvidence.get("childJvmRuntime"), BenchmarkOwnedProcessReceipt.MAX_ARGUMENTS);
        require(!originalChildPackets.isEmpty(), "original Process child runtime packets are missing");
        long start = number(object(originalChildPackets.getFirst()), "jvmStartTimeMillis");
        var producerIdentity = new BenchmarkCausalClock.Identity(originalPid, start);
        var producer = new Runtime(Owner.PRODUCER, runId, originalPid, start, jdk.javaHome(), jdk.javaVersion());
        var producerRegistration = new BenchmarkReturnWorkBudgetProducerSeal.Registration(runId, producerIdentity, inputSha,
                text(registration.get("nonce"), 64), app.blob().sha256());
        BenchmarkReturnWorkBudgetProducerSeal.requireExited(runtimeEvidence, producerRegistration);
        Path bodyPath = path(text(registration.get("producerBodyPath"), 4096));
        var bodyBytes = acquire(bodyPath, BenchmarkReturnWorkBudgetProducerSeal.MAX_BODY_BYTES, null, reads, FILES);
        preserve(output, bodyBytes, "producer-body.acquired.json"); facts.put("producerBody", blobEvidence(bodyBytes));
        var companionBytes = acquire(path(bodyPath + ".seal"), BenchmarkReturnWorkBudgetProducerSeal.COMPANION_BYTES, null, reads, FILES);
        preserve(output, companionBytes, "producer-body.acquired.seal"); facts.put("producerCompanion", blobEvidence(companionBytes));
        require(companionBytes.bytes().length == BenchmarkReturnWorkBudgetProducerSeal.COMPANION_BYTES, "companion size differs");
        var bound = BenchmarkReturnWorkBudgetProducerSeal.verifyFiles(bodyPath, producerRegistration);
        require(bodyBytes.blob().sha256().equals(bound.bodySha256()) && bodyBytes.blob().bytes() == bound.bodyBytes()
                && companionBytes.blob().sha256().equals(bound.companionSha256()), "producer raw files changed between independent acquisitions");
        require(bodyBytes.blob().sha256().equals(producerFacts.get("bodySha256"))
                && companionBytes.blob().sha256().equals(producerFacts.get("companionSha256")), "retained producer hashes differ from the actual files");
        require(json(bodyBytes.bytes(), producerPlan()).equals(normalized(producerFacts.get("body"), producerPlan())), "retained original producer body differs");

        Path sourceRoot = path(text(registration.get("sourceRoot"), 4096));
        var staticBytes = BenchmarkReturnWorkBudgetBindingsIo.authenticate(sourceRoot, path(app.path()), declared, helper, jdk);
        requireOriginalIo(object(originalBindings.get("actualIo")), staticBytes.evidence());
        verifyEmitted(object(originalBindings.get("actualEmittedRoster")), helper, staticBytes);
        var rootOrigins = object(producerFacts.get("actualRootOrigins"));
        require(rootOrigins.equals(object(originalBindings.get("actualRootOrigins"))), "duplicate retained root origin receipts differ");
        var observed = rootOrigins(rootOrigins, root, helper, staticBytes);
        var phases = workload.phases().stream().filter(BenchmarkReturnWorkBudgetProfile.PhaseInput::measured)
                .map(phase -> workload.id() + "/" + phase.id()).toList();
        var packets = list(producerFacts.get("producerClassMetadata"), 12);
        require(packets.size() == 2 * phases.size()
                && packets.equals(list(originalBindings.get("actualProducerOrigins"), 12)),
                "original before/after phase packet count or retained receipts differ");
        require(strings(producerFacts.get("producerClassMetadataExpectedPhaseKeys"), 6, 128).equals(phases)
                && number(producerFacts, "producerClassMetadataExpectedPackets") == packets.size(),
                "original registered phase/cut roster differs");
        verifyOriginalPacketRoster(packets, phases, producerIdentity);
        verifyMeasurementNativePackets(list(object(report.get("measurement")).get("measuredPhaseWindows"), 6), packets, phases);
        observed.addAll(producerOrigins(packets, producer, producerIdentity, app, library, helper, staticBytes));
        var owners = new OwnedRuntimes(root, producer);
        var actual = new ActualFacts(runId, inputSha, helper.buildId(), Lifecycle.FAILSAFE_VERIFY, preflightBytes.blob(),
                classpath, staticBytes.files(), staticBytes.sources(), staticBytes.classes(), observed, staticBytes.jdkFiles(), Set.of(), staticBytes.producerWriter());
        var bindingFacts = new BenchmarkReturnAfterReceipt.BindingFacts(helper, jdk, actual, owners,
                List.of("VM_GENERATED_OR_TRANSFORMED_INSTRUCTION_BODY_NOT_PROVEN",
                        "OPAQUE_PROVIDER_INSTRUCTIONS_CPU_OR_ELAPSED_NOT_IN_STRUCTURAL_VECTOR",
                        "ROOT_OS_TERMINATION_REMAINS_INDEPENDENT_UNKNOWN",
                        "EXTERNAL_VERIFIER_ACQUISITION_IS_OUTSIDE_ORIGINAL_METHOD_BUDGET"));
        var rootIdentity = new BenchmarkReturnWorkBudgetAdmission.Runtime(runId, root.pid(), root.jvmStartTimeMillis());
        var afterRegistration = new BenchmarkReturnAfterReceipt.Registration(runId, inputSha, authenticated, rootIdentity, producerRegistration,
                workload, plan, reportPath.toString(), bodyPath.toString(), testClass, testMethod,
                BenchmarkReturnRootClockWork.coldInitializationUpper(), BenchmarkReturnWorkBudgetSourceTails.factoryUpper(workload),
                BenchmarkReturnWorkBudgetSourceTails.terminalUpper(plan));
        var protocolFact = new BenchmarkReturnAfterReceipt.ByteFact(output.resolve("final-protocol.txt").toString(),
                new Blob(hash(selected.protocol()), selected.protocol().length), selected.protocol());
        var afterInputs = new BenchmarkReturnAfterReceipt.Inputs(closed, reportBytes.byteFact(), protocolFact, outcomeBytes.byteFact(),
                rootIdentity, bindingFacts, bodyBytes.byteFact(), bound, runtimeEvidence);
        var after = BenchmarkReturnAfterReceipt.verify(afterRegistration, afterInputs);
        Files.write(output.resolve("final-protocol.txt"), selected.protocol(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        var evidence = new LinkedHashMap<String, Object>(after.evidence());
        evidence.put("externalAcquisitionState", "BOUNDED_IMMUTABLE_BYTES_EOF_HASH_AND_OWNED_STREAM_CLOSE_RECORDED");
        evidence.put("externalVerifierWorkScope", "NOT_ADDED_TO_ORIGINAL_METHOD_SOURCE_UPPER");
        evidence.put("originalReport", blobEvidence(reportBytes)); evidence.put("actualOutcomeXml", blobEvidence(outcomeBytes));
        evidence.put("preflightXml", blobEvidence(preflightBytes)); evidence.put("registeredInputs", blobEvidence(registrationBytes));
        evidence.put("producerBody", blobEvidence(bodyBytes)); evidence.put("producerCompanion", blobEvidence(companionBytes));
        evidence.put("externalStaticAuthentication", staticBytes.evidence());
        evidence.put("retainedRootClasses", observed.stream().filter(o -> o.origin().owner() == Owner.ROOT).count());
        evidence.put("retainedProducerClasses", observed.stream().filter(o -> o.origin().owner() == Owner.PRODUCER).count());
        evidence.put("originalOwnedProcessOutcome", runtimeEvidence);
        evidence.put("acquiredReads", List.copyOf(reads));
        evidence.put("registrationScope", "ONE_REGISTERED_ORIGINAL_A_OR_B_FORK_NO_RETRY_OR_REFILL");
        evidence.put("workloadId", workloadId);
        evidence.put("arm", flags.arm());
        String ordinal = text(object(registration.get("properties")).get("tapstate.e2e.benchmark-smoke.fork"), 20);
        require(ordinal.matches("[1-5]"), "original fork ordinal differs");
        evidence.put("ordinal", Long.parseLong(ordinal));
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> evidence.put(flag, false));
        return new AcquiredCompletion(afterRegistration, after, report, evidence, registrationBytes.blob(), helper, staticBytes);
    }

    static void verifyOriginalPacketRoster(List<?> packets, List<String> phases, BenchmarkCausalClock.Identity owner) {
        require(packets != null && packets.size() <= 12 && phases != null && phases.size() <= 6
                && !phases.isEmpty() && packets.size() == 2 * phases.size() && owner != null,
                "original phase/cut packet roster is incomplete");
        for (int i = 0; i < packets.size(); i++) {
            var packet = object(packets.get(i));
            require(phases.get(i / 2).equals(packet.get("phaseKey"))
                    && (i % 2 == 0 ? "BEFORE" : "AFTER").equals(packet.get("cut"))
                    && owner.pid() == number(packet, "pid") && owner.jvmStartTimeMillis() == number(packet, "jvmStartTimeMillis"),
                    "original phase/cut packet order or owner differs");
        }
    }

    static void verifyMeasurementNativePackets(List<?> windows, List<?> packets, List<String> phaseKeys) {
        require(windows != null && windows.size() <= 6 && packets != null && packets.size() <= 12
                && phaseKeys != null && phaseKeys.size() <= 6 && windows.size() == phaseKeys.size()
                && packets.size() == 2 * windows.size(), "measurement cold packet roster differs");
        for (int i = 0; i < windows.size(); i++) {
            var window = object(windows.get(i));
            String key = text(phaseKeys.get(i), 128);
            require(key.endsWith("/" + text(window.get("id"), 128)), "measurement native phase identity differs");
            var clocks = object(window.get("targetClockQualification"));
            for (int cut = 0; cut < 2; cut++) {
                var nativeMap = object(clocks.get(cut == 0 ? "nativeClockBefore" : "nativeClockAfter"));
                var packet = object(packets.get(2 * i + cut));
                require(key.equals(packet.get("phaseKey")) && (cut == 0 ? "BEFORE" : "AFTER").equals(packet.get("cut")),
                        "measurement native phase/cut differs from authenticated packet");
                var declared = strings(object(packet.get("producerOrigins")).get("rawParts"),
                        BenchmarkReturnProducerOrigins.PARTS, BenchmarkReturnProducerOrigins.ORIGIN_BYTES);
                var used = strings(object(nativeMap.get("producerOrigins")).get("rawParts"),
                        BenchmarkReturnProducerOrigins.PARTS, BenchmarkReturnProducerOrigins.ORIGIN_BYTES);
                require(declared.size() == BenchmarkReturnProducerOrigins.PARTS && declared.equals(used)
                        && declared.getFirst().equals(text(nativeMap.get("raw"), BenchmarkReturnComponentSourceWork.NATIVE_CHARS)),
                        "measurement native raw or origin parts differ from its authenticated phase/cut packet");
            }
        }
    }

    static Acquired acquire(Path path, long priorCap, Blob expected, List<Map<String, Object>> retained, Streams streams) throws IOException {
        require(path != null && path.isAbsolute() && path.equals(path.normalize()) && priorCap > 0 && priorCap <= Integer.MAX_VALUE - 1L,
                "acquisition path or independent byte cap differs");
        require(expected == null || expected.bytes() <= priorCap, "registered material exceeds its independent cap");
        require(retained != null && retained.size() < 32, "acquisition evidence roster exceeds its fixed local cap");
        var evidence = new LinkedHashMap<String, Object>(); evidence.put("path", path.toString()); evidence.put("priorByteCap", priorCap);
        evidence.put("state", "PENDING_READ"); long consumed = 0; boolean eof = false, closed = false; Throwable primary = null;
        try {
            require(path.toRealPath().equals(path), "acquired path is linked or noncanonical");
            var before = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            require(before.isRegularFile() && before.size() > 0 && before.size() <= priorCap, "acquired file exceeds its independent prior bound or is nonregular");
            require(expected == null || before.size() == expected.bytes(), "acquired file size differs from its registration");
            evidence.put("sizeBefore", before.size());
            byte[] copy = new byte[Math.toIntExact(before.size())]; var sha = sha();
            InputStream input = streams.open(path); require(input != null, "acquired stream is absent");
            Throwable readPrimary = null;
            try {
                int at = 0;
                while (at < copy.length) {
                    int wanted = Math.min(BUFFER, copy.length - at); int read = input.read(copy, at, wanted);
                    require(read > 0 && read <= wanted, "acquired file is truncated or its read makes no progress");
                    sha.update(copy, at, read); at = Math.addExact(at, read); consumed = at;
                }
                require(input.read() == -1, "acquired file has bytes beyond the bounded original size"); eof = true;
            } catch (IOException | RuntimeException | Error failure) { readPrimary = failure; throw failure; }
            finally {
                try { input.close(); closed = true; }
                catch (IOException | RuntimeException | Error closeFailure) {
                    if (readPrimary == null) throw closeFailure;
                    if (closeFailure != readPrimary) readPrimary.addSuppressed(closeFailure);
                }
            }
            var after = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            require(after.isRegularFile() && after.size() == before.size() && Objects.equals(before.fileKey(), after.fileKey())
                    && before.lastModifiedTime().equals(after.lastModifiedTime()) && path.toRealPath().equals(path), "acquired file changed while read");
            String digest = HexFormat.of().formatHex(sha.digest()); var actual = new Blob(digest, consumed);
            require(expected == null || expected.equals(actual), "acquired digest differs from independent registration");
            evidence.put("state", "IMMUTABLE_BYTES_ACQUIRED"); evidence.put("sha256", digest);
            return new Acquired(path, copy, actual);
        } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
        finally {
            evidence.put("actualBytesConsumed", consumed); evidence.put("actualEofObserved", eof); evidence.put("ownedStreamCloseCompleted", closed);
            if (primary != null) evidence.put("failure", throwable(primary));
            retained.add(Map.copyOf(evidence));
        }
    }

    static SelectedCase selectedCase(byte[] xml, String selectedClass, String selectedMethod) throws Exception {
        require(xml != null && xml.length > 0 && xml.length <= BenchmarkReturnAfterReceipt.XML_BYTES, "outcome XML exceeds its prior cap");
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setXIncludeAware(false); factory.setExpandEntityReferences(false);
        var document = factory.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
        Element suite = document.getDocumentElement();
        require(suite != null && suite.getTagName().equals("testsuite") && suite.getAttribute("tests").equals("1")
                && suite.getAttribute("failures").equals("0") && suite.getAttribute("errors").equals("0") && suite.getAttribute("skipped").equals("0"),
                "actual outcome is not one successful testcase");
        var cases = suite.getElementsByTagName("testcase"); require(cases.getLength() == 1, "actual testcase is missing or duplicated");
        Element selected = (Element) cases.item(0);
        require(selectedClass.equals(selected.getAttribute("classname")) && selectedMethod.equals(selected.getAttribute("name"))
                && selected.getElementsByTagName("failure").getLength() == 0 && selected.getElementsByTagName("error").getLength() == 0
                && selected.getElementsByTagName("skipped").getLength() == 0, "actual testcase identity or success differs");
        var out = selected.getElementsByTagName("system-out"); require(out.getLength() == 1, "selected testcase system-out is missing or duplicated");
        String stdout = out.item(0).getTextContent(); int at = 0, found = -1, count = 0;
        while ((at = stdout.indexOf(BenchmarkReturnAfterReceipt.FINAL_PREFIX, at)) >= 0) {
            if (at == 0 || stdout.charAt(at - 1) == '\n') { found = at; count = Math.incrementExact(count); }
            at = Math.addExact(at, BenchmarkReturnAfterReceipt.FINAL_PREFIX.length());
        }
        require(count == 1, "selected testcase final marker is missing or duplicated"); int end = stdout.indexOf('\n', found);
        require(end >= 0, "selected testcase final marker is partial");
        String line = stdout.substring(found, end + 1);
        long prior = Math.addExact(BenchmarkReturnAfterReceipt.closedPlan().encodedUpperBytes(), BenchmarkReturnAfterReceipt.FINAL_PREFIX.length() + 1L);
        require(line.length() <= prior && Math.multiplyExact(4L, line.length()) <= Math.multiplyExact(4L, prior), "final marker exceeds its independent bound");
        byte[] protocol = line.getBytes(StandardCharsets.UTF_8); require(protocol.length <= prior, "final marker UTF-8 exceeds its prior bound");
        byte[] body = line.substring(BenchmarkReturnAfterReceipt.FINAL_PREFIX.length(), line.length() - 1).getBytes(StandardCharsets.UTF_8);
        var closed = json(body, BenchmarkReturnAfterReceipt.closedPlan());
        var properties = new LinkedHashMap<String, String>(); var nodes = suite.getElementsByTagName("property");
        require(nodes.getLength() <= 512, "actual Failsafe property roster exceeds its prior cap");
        for (int i = 0; i < nodes.getLength(); i++) {
            Element property = (Element) nodes.item(i); String name = property.getAttribute("name"), value = property.getAttribute("value");
            require(name.length() <= 4096 && value.length() <= INPUT_BYTES, "actual Failsafe property material exceeds prior cap");
            require(properties.put(name, value) == null, "actual Failsafe property is repeated");
        }
        return new SelectedCase(protocol, closed, Map.copyOf(properties));
    }

    static BenchmarkReturnWorkBudgetProfile.Flags registeredFlags(Map<String, Object> registration) {
        var properties = object(registration.get("properties"));
        String arm = text(properties.get("tapstate.e2e.benchmark-smoke.arm"), 1);
        require(List.of("A", "B").contains(arm)
                && BenchmarkReturnMethodAdmission.METHOD.equals(properties.get(BenchmarkReturnForkBridge.PROPERTY))
                && "PLAIN".equals(properties.get("tapstate.e2e.benchmark-smoke.capture-mode"))
                && !properties.containsKey(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY)
                && "true".equals(properties.get("tapstate.e2e.benchmark-smoke.steady-pilot"))
                && "true".equals(properties.get(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY))
                && "true".equals(properties.get(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY))
                && "true".equals(properties.get(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY))
                && "true".equals(properties.get(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY))
                && properties.get(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY) instanceof String
                && properties.get("tapstate.e2e.benchmark-smoke.fork-output") instanceof String
                && list(registration.get("otherControls"), 16).isEmpty()
                && Boolean.FALSE.equals(registration.get("rootCpu")), "prospective original return route differs");
        for (String key : BenchmarkReturnForkBridge.conflictProperties()) require(!properties.containsKey(key),
                "registered return route conflicts with " + key);
        falseFlags(object(registration.get("qualificationFlags")));
        return new BenchmarkReturnWorkBudgetProfile.Flags(arm, "PLAIN", "PERIODIC", "ON",
                true, true, true, true, false, Set.of());
    }

    private static ProducerWriterBinding writerBinding(Map<String, Object> registration) {
        Object raw = registration.get("producerWriter");
        String arm = text(object(registration.get("properties")).get("tapstate.e2e.benchmark-smoke.arm"), 1);
        if ("B".equals(arm)) { require(raw == null, "candidate cannot borrow the baseline writer"); return null; }
        var writer = object(raw);
        require("PRODUCER".equals(writer.get("owner")) && "PDK_WRITER".equals(writer.get("role"))
                && SourceRole.PDK_WRITER.binaryName().equals(writer.get("binaryName"))
                && SourceRole.PDK_WRITER.sourcePath().equals(writer.get("sourcePath")), "baseline writer tuple differs");
        var result = new ProducerWriterBinding(Owner.PRODUCER, text(writer.get("sourceRoot"), 4096),
                new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(),
                        new Blob(digest(text(writer.get("sourceSha256"), 64)), number(writer, "sourceBytes"))),
                new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(),
                        new Blob(digest(text(writer.get("classSha256"), 64)), number(writer, "classBytes"))));
        require(result.compilerPath().equals(writer.get("compilerPath")), "baseline writer compiler path differs");
        return result;
    }

    private static void verifyRootProperties(Map<String, String> actual, Map<String, Object> registration, List<FileFact> files,
            BenchmarkReturnWorkBudgetProfile.Flags flags) {
        Map<String, String> required = Map.of(
                "tapstate.e2e.benchmark-smoke.arm", flags.arm(), "tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN",
                "tapstate.e2e.benchmark-smoke.steady-pilot", "true", BenchmarkReturnForkBridge.PROPERTY, BenchmarkReturnMethodAdmission.METHOD,
                RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true", RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY, "true",
                RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY, "true", RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, "true",
                "tapstate.e2e.benchmark-smoke.fork-output", text(registration.get("reportPath"), 4096),
                RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, file(files, BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).path());
        require(actual.get(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY) == null,
                "actual original return route contains a calibration selector");
        for (String key : BenchmarkReturnForkBridge.conflictProperties()) require(actual.get(key) == null,
                "actual original return route conflicts with " + key);
        required.forEach((key, value) -> require(value.equals(actual.get(key)), "actual original root property differs: " + key));
        require(flags.rootCpu() ? "true".equals(actual.get(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY))
                : actual.get(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY) == null, "actual root CPU property differs");
        var registeredProperties = object(registration.get("properties"));
        registeredProperties.forEach((key, value) -> require(text(value, INPUT_BYTES).equals(actual.get(key)), "actual registered root property differs: " + key));
        String prefix = BenchmarkReturnWorkBudgetRun.PREFIX;
        require(text(registration.get("runId"), 128).equals(actual.get(prefix + "runId"))
                && text(registration.get("inputSha256"), 64).equals(actual.get(prefix + "inputSha256"))
                && text(registration.get("producerBodyPath"), 4096).equals(actual.get(prefix + "producerBody"))
                && text(registration.get("nonce"), 64).equals(actual.get(prefix + "nonce"))
                && text(registration.get("sourceRoot"), 4096).equals(actual.get(prefix + "sourceRoot")), "actual original root registration properties differ");
        for (var file : files) {
            String key = prefix + "file." + file.role().name() + ".";
            require(file.path().equals(actual.get(key + "path")) && file.blob().sha256().equals(actual.get(key + "sha256"))
                    && Long.toString(file.blob().bytes()).equals(actual.get(key + "bytes")), "actual original root file scalar differs");
        }
    }

    static void compareCachedRegistration(Map<String, Object> input, BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile authenticated,
            BenchmarkReturnWorkBudgetProfile.WorkloadInput workload, BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan) {
        var cached = object(input.get("cached")); var definition = object(cached.get("definition"));
        require(authenticated.definition().definitionId().equals(definition.get("definitionId"))
                && authenticated.definition().sourceFormulaSha256().equals(definition.get("sourceFormulaSha256")), "cached definition differs from the trusted source recipe");
        var actorBounds = object(definition.get("perOperationUpper"));
        var expectedActors = authenticated.prepared().perOperationUpper();
        require(actorBounds.size() == expectedActors.size(), "cached activity bound roster differs");
        for (var entry : expectedActors.entrySet()) require(entry.getValue().equals(vector(actorBounds.get(entry.getKey().name()))), "cached activity source bound differs");
        require(new HashSet<>(strings(definition.get("explicitExcludedActivities"), 57, 128)).equals(
                authenticated.prepared().explicitExcludedActivities().stream().map(Enum::name).collect(java.util.stream.Collectors.toSet())), "cached excluded activity roster differs");
        var output = object(cached.get("outputPlan"));
        require(number(output, "nodeUpper") == plan.nodeUpper() && number(output, "depthUpper") == plan.depthUpper()
                && number(output, "leafUtf8Upper") == plan.leafUtf8Upper() && number(output, "stringUtf8Upper") == plan.stringUtf8Upper()
                && number(output, "keyUtf8Upper") == plan.keyUtf8Upper() && number(output, "numberTextUpper") == plan.numberTextUpper()
                && plan.sha256().equals(output.get("sha256")), "cached six-field output plan differs");
        require(vector(cached.get("coldUpper")).equals(BenchmarkReturnRootClockWork.coldInitializationUpper())
                && vector(cached.get("factoryUpper")).equals(BenchmarkReturnWorkBudgetSourceTails.factoryUpper(workload))
                && vector(cached.get("terminalUpper")).equals(BenchmarkReturnWorkBudgetSourceTails.terminalUpper(plan)), "cached source tail differs");
    }

    private static void requireOriginalBindings(Map<String, Object> original, BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile authenticated, HelperManifest helper) {
        require("ACTUAL_DECLARED_BYTES_AND_ROOT_RUNTIME_RECORDED".equals(original.get("state"))
                && helper.runId().equals(original.get("runId")) && helper.inputSha256().equals(original.get("inputSha256"))
                && authenticated.definition().definitionId().equals(original.get("rootDefinitionId"))
                && authenticated.definition().sourceFormulaSha256().equals(original.get("rootFormulaSha256"))
                && helper.producerFormulaSha256().equals(original.get("producerFormulaSha256")), "original static authentication identity differs");
        falseFlags(original); require("CURRENT_SUREFIRE_TEST_CLASS_PATH_PROPERTY".equals(original.get("classpathSource")), "original classpath source differs");
    }

    private static void requireOriginalIo(Map<String, Object> original, Map<String, Object> fresh) {
        require("DECLARED_BYTE_FACTS_AUTHENTICATED".equals(original.get("state")) && original.get("state").equals(fresh.get("state"))
                && original.get("manifestEncoding").equals(fresh.get("manifestEncoding")) && original.get("scope").equals(fresh.get("scope")),
                "original IO authentication is unavailable");
        for (String key : List.of("consumedBytes", "hashedBytes", "zipEntries")) require(number(original, key) == number(fresh, key), "original authenticated byte or archive totals differ");
        require(number(original, "requests") > 0, "original authentication request attempts are absent"); falseFlags(original);
    }

    static void verifyEmitted(Map<String, Object> receipt, HelperManifest helper, BenchmarkReturnWorkBudgetBindingsIo.Result actual) {
        require("COMPILER_SOURCE_ROSTER_OBSERVED".equals(receipt.get("state"))
                && "FOUR_OWNED_COMPILER_OUTPUTS_SOURCEFILE_BINARY_AND_BYTE_FACTS_ONLY".equals(receipt.get("scope")), "original emitted source receipt is missing");
        require(number(receipt, "directoryVisits") > 0 && number(receipt, "directoryVisits") <= 40000
                && number(receipt, "serviceRequests") <= BenchmarkReturnEmittedRoster.serviceRequestUpper()
                && number(receipt, "serviceRequestUpper") == BenchmarkReturnEmittedRoster.serviceRequestUpper(), "original emitted traversal is out of its declared source bounds");
        falseFlags(receipt);
        var expectedSources = new HashMap<SourceRole, Source>();
        for (var source : actual.sources()) if (source.role() != SourceRole.JNI_SOURCE) expectedSources.put(source.role(), source);
        for (Object raw : list(receipt.get("sources"), SourceRole.values().length)) {
            var source = object(raw); var role = SourceRole.valueOf(text(source.get("role"), 128)); var expected = expectedSources.remove(role);
            require(expected != null && expected.path().equals(source.get("path")) && expected.blob().equals(blob(source)), "original emitted source is foreign, repeated or changed");
        }
        require(expectedSources.isEmpty(), "original emitted source coverage is incomplete");
        var expectedMembers = classes(helper); long consumed = 0;
        for (Object raw : list(receipt.get("selectedMembers"), MAX_CLASSES)) {
            var member = object(raw); String binary = text(member.get("binaryName"), 256); var expected = expectedMembers.remove(binary);
            require(expected != null && expected.role().name().equals(member.get("role")) && expected.blob().equals(blob(member)), "original emitted member is foreign, repeated or changed");
            String sourceFile = expected.role().binaryName().substring(expected.role().binaryName().lastIndexOf('.') + 1) + ".java";
            require(sourceFile.equals(member.get("sourceFile")) && (binary.replace('.', '/') + ".class").equals(member.get("entry")), "original SourceFile or emitted member path differs");
            String suffix = switch (expected.role().module()) {
                case "e2e" -> "/e2e/target/test-classes";
                case "adapter-pdk" -> "/adapters/adapter-pdk/target/classes";
                case "core-common" -> "/core/core-common/target/classes";
                case "test-support" -> "/test-support/target/test-classes";
                default -> throw new AssertionError("unexpected emitted Java module");
            };
            require(path(text(member.get("compilerContainer"), 4096)).toString().endsWith(suffix), "original compiler output route differs");
            consumed = Math.addExact(consumed, expected.blob().bytes());
        }
        require(expectedMembers.isEmpty() && number(receipt, "consumedBytes") >= consumed, "original complete emitted member roster is incomplete");
    }

    static ArrayList<ObservedOrigin> rootOrigins(Map<String, Object> evidence, Runtime runtime, HelperManifest helper,
            BenchmarkReturnWorkBudgetBindingsIo.Result authenticated) {
        require("ROOT_NAMED_CLASS_ORIGINS_RECORDED".equals(evidence.get("state"))
                && "EXACT_REGISTERED_NAMES_INITIALIZE_FALSE".equals(evidence.get("resolution"))
                && "OWN_CLASS_RESOURCE_AGREES_WITH_ACTUAL_PROTECTION_DOMAIN_AND_REGISTERED_BYTES".equals(evidence.get("byteScope")), "original root origin observation is missing");
        var owner = object(evidence.get("runtime"));
        require(runtime.owner().name().equals(owner.get("owner")) && runtime.runId().equals(owner.get("runId"))
                && runtime.pid() == number(owner, "pid") && runtime.jvmStartTimeMillis() == number(owner, "jvmStartTimeMillis")
                && runtime.javaHome().equals(owner.get("javaHome")) && runtime.javaVersion().equals(owner.get("javaVersion")), "original root origin runtime differs");
        falseFlags(evidence); var expected = origins(authenticated.origins(), Owner.ROOT); var found = new ArrayList<ObservedOrigin>(); long bytes = 0;
        var rows = list(evidence.get("classes"), MAX_CLASSES);
        require(number(evidence, "classCount") == rows.size() && rows.size() == helper.classes().size(), "original root origin count differs");
        for (Object raw : rows) {
            var row = object(raw); String binary = text(row.get("binaryName"), 256); var registered = expected.remove(binary);
            require(registered != null && registered.container().equals(row.get("container")) && registered.entry().equals(row.get("entry"))
                    && registered.classBytes().equals(blob(row)), "original root origin is foreign, repeated or changed");
            text(row.get("loaderClass"), 256); signed(row.get("loaderIdentityHashCode"));
            var observed = new Origin(Owner.ROOT, registered.role(), binary, text(row.get("container"), 4096), text(row.get("entry"), 1024), blob(row), null);
            found.add(new ObservedOrigin(observed, runtime)); bytes = Math.addExact(bytes, observed.classBytes().bytes());
        }
        long upper = BenchmarkReturnRootOrigins.serviceRequestUpper(rows.size(), bytes);
        require(expected.isEmpty() && number(evidence, "consumedBytes") == bytes && number(evidence, "hashedBytes") == bytes
                && number(evidence, "serviceRequestUpper") == upper && number(evidence, "serviceRequests") <= upper, "original root origin byte or service totals differ");
        return found;
    }

    private static List<ObservedOrigin> producerOrigins(List<?> receipts, Runtime runtime, BenchmarkCausalClock.Identity identity,
            FileFact app, FileFact library, HelperManifest helper, BenchmarkReturnWorkBudgetBindingsIo.Result authenticated) {
        List<ObservedOrigin> result = null; Map<String, Object> nativeBefore = null;
        var compiled = classes(helper); var expected = origins(authenticated.origins(), Owner.PRODUCER);
        require(expected.size() == BenchmarkReturnProducerOrigins.ORIGINS, "registered producer class roster differs");
        for (Object value : receipts) {
            var receipt = object(value);
            require(number(receipt, "pid") == runtime.pid() && number(receipt, "jvmStartTimeMillis") == runtime.jvmStartTimeMillis(), "original producer cold runtime differs");
            var evidence = object(receipt.get("producerOrigins")); var parts = strings(evidence.get("rawParts"), BenchmarkReturnProducerOrigins.PARTS, BenchmarkReturnProducerOrigins.ORIGIN_BYTES);
            require(parts.size() == BenchmarkReturnProducerOrigins.PARTS, "original producer packet is incomplete");
            var parsed = BenchmarkReturnProducerOrigins.parse(parts.toArray(String[]::new), identity, app.path());
            require("RECORDED_UNQUALIFIED".equals(parsed.header().state()) && runtime.javaHome().equals(parsed.header().top().javaHome())
                    && runtime.javaVersion().equals(parsed.header().top().javaVersion()), "original producer packet runtime or state differs");
            var nativeFacts = BenchmarkNativeClockEvidence.parse(parsed.nativeRaw(), identity, library.path());
            require("NATIVE_RECORDED".equals(nativeFacts.get("state")), "original native part is not recorded");
            for (String boundary : List.of("before", "after")) {
                var snapshot = object(nativeFacts.get(boundary)); require(library.blob().sha256().equals(snapshot.get("loadedJniSha256")), "original native library bytes differ");
                var hashes = object(snapshot.get("classHashes"));
                for (String suffix : List.of("", "$Clock", "$JniAccess", "$NativeAccess")) {
                    var item = compiled.get("io.tapstate.adapters.pdk.PdkBenchmarkClock" + suffix);
                    require(item != null && item.blob().sha256().equals(hashes.get("PdkBenchmarkClock" + suffix)), "original native class bytes differ");
                }
                require(hashes.equals(object(receipt.get("classHashes"))), "retained four-class receipt differs from the exact native packet");
            }
            if (nativeBefore == null) nativeBefore = nativeFacts;
            else BenchmarkNativeClockEvidence.requireMatchingNative(nativeBefore, nativeFacts);
            var seen = new HashSet<String>(); var current = new ArrayList<ObservedOrigin>();
            for (var part : parsed.typedParts()) {
                var prior = expected.get(part.binaryName());
                require(prior != null && seen.add(part.binaryName()) && part.kind() == BenchmarkReturnProducerOrigins.Kind.BOOT_NESTED
                        && prior.container().equals(part.outer()) && prior.entry().equals(part.module() + "!/" + part.entry())
                        && prior.classBytes().sha256().equals(part.sha256()) && prior.classBytes().bytes() == part.bytes(), "original producer origin differs from authenticated application entries");
                current.add(new ObservedOrigin(new Origin(Owner.PRODUCER, prior.role(), part.binaryName(), part.outer(),
                        part.module() + "!/" + part.entry(), new Blob(part.sha256(), part.bytes()), app.blob().sha256()), runtime));
            }
            require(seen.equals(expected.keySet()), "original producer loaded origin coverage is incomplete");
            if (result == null) result = List.copyOf(current); else require(result.equals(current), "original producer before/after origin records differ");
        }
        require(result != null, "original producer origins are absent"); return result;
    }

    static List<String> classpath(String raw) {
        require(raw != null && raw.length() <= 512L * 4097 && !raw.isEmpty(), "actual ordered classpath is missing or oversized");
        String[] tokens = raw.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator), -1);
        require(tokens.length <= MAX_CLASSPATH + 1, "actual classpath token roster exceeds its source cap");
        var result = new ArrayList<String>(); var seen = new HashSet<String>();
        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].isEmpty()) { require(i == tokens.length - 1 && i > 0, "actual classpath has an interior empty token"); continue; }
            String item = path(tokens[i]).toString(); require(seen.add(item), "actual classpath has a repeated origin"); result.add(item);
        }
        require(!result.isEmpty() && result.size() <= MAX_CLASSPATH, "actual classpath origin roster differs"); return List.copyOf(result);
    }

    static List<FileFact> files(Object raw) {
        var result = new ArrayList<FileFact>(); var seen = new HashSet<BenchmarkReturnWorkBudgetProfile.FileRole>();
        for (Object value : list(raw, BenchmarkReturnWorkBudgetProfile.FileRole.values().length)) {
            var row = object(value); var role = BenchmarkReturnWorkBudgetProfile.FileRole.valueOf(text(row.get("role"), 128));
            require(seen.add(role), "registered file role is repeated"); result.add(new FileFact(role, path(text(row.get("path"), 4096)).toString(), blob(row)));
        }
        return List.copyOf(result);
    }
    private static FileFact file(List<FileFact> files, BenchmarkReturnWorkBudgetProfile.FileRole role) {
        return files.stream().filter(file -> file.role() == role).findFirst().orElseThrow(() -> new AssertionError("missing registered file role " + role));
    }
    private static Map<String, CompiledClass> classes(HelperManifest helper) {
        var result = new HashMap<String, CompiledClass>();
        for (var item : helper.classes()) require(result.put(item.binaryName(), item) == null, "registered class repeats"); return result;
    }
    private static Map<String, Origin> origins(List<Origin> origins, Owner owner) {
        var result = new HashMap<String, Origin>();
        for (var origin : origins) if (origin.owner() == owner) require(result.put(origin.binaryName(), origin) == null, "authenticated owned origin repeats"); return result;
    }
    private static void acquisitionCaps(Map<String, Object> caps) {
        Map<String, Long> fixed = Map.of("prospectiveBytes", (long) INPUT_BYTES, "helperManifestBytes", MAX_MANIFEST_BYTES,
                "jdkManifestBytes", MAX_MANIFEST_BYTES, "reportBytes", (long) REPORT_ACQUISITION_BYTES,
                "failsafeXmlBytes", (long) BenchmarkReturnAfterReceipt.XML_BYTES, "producerBodyBytes", 65536L, "companionBytes", 256L);
        require(caps.keySet().equals(fixed.keySet()), "acquisition material cap roster differs");
        fixed.forEach((key, value) -> require(number(caps, key) == value, "independent acquisition cap differs: " + key));
    }
    static Map<String, Object> json(byte[] bytes, BenchmarkReturnWorkBudgetDefinitions.OutputPlan prior) {
        require(bytes != null && bytes.length <= prior.encodedUpperBytes(), "acquired JSON exceeds its independently registered encoded bound");
        String raw = new String(bytes, StandardCharsets.UTF_8);
        require(Arrays.equals(bytes, raw.getBytes(StandardCharsets.UTF_8)), "acquired JSON is malformed UTF-8");
        BenchmarkReturnWorkBudgetDefinitions.requireJsonShape(raw, bytes.length, prior.depthUpper(),
                (int) Math.min(Integer.MAX_VALUE - 1L, prior.nodeUpper()), prior.leafUtf8Upper());
        var result = object(BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(JsonReader.parse(raw), prior).value());
        require(raw.equals(JsonWriter.write(result)), "acquired JSON is noncanonical or contains duplicate fields"); return result;
    }
    private static Map<String, Object> normalized(Object value, BenchmarkReturnWorkBudgetDefinitions.OutputPlan prior) {
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(value, prior).value();
        return json(JsonWriter.write(frozen).getBytes(StandardCharsets.UTF_8), prior);
    }
    private static BenchmarkReturnWorkBudgetDefinitions.OutputPlan producerPlan() {
        return new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(65536, 12, 1024, 65536, 128, 24);
    }
    private static BenchmarkReturnWorkBudget.Work vector(Object value) {
        var row = object(value); require(row.keySet().equals(UNITS), "source vector roster differs");
        return new BenchmarkReturnWorkBudget.Work(number(row, "calls"), number(row, "requests"), number(row, "records"), number(row, "frames"),
                number(row, "bytes"), number(row, "passes"), number(row, "elements"), number(row, "updates"));
    }
    private static void falseFlags(Map<String, Object> value) {
        for (var entry : value.entrySet()) if (entry.getKey().endsWith("Qualified") || entry.getKey().endsWith("Eligible") || entry.getKey().equals("formalPerformance"))
            require(Boolean.FALSE.equals(entry.getValue()), "original diagnostic receipt claimed qualification: " + entry.getKey());
    }
    @SuppressWarnings("unchecked") static Map<String, Object> object(Object value) {
        require(value instanceof Map<?, ?> && ((Map<?, ?>) value).keySet().stream().allMatch(String.class::isInstance), "bounded object is missing");
        return (Map<String, Object>) value;
    }
    static List<?> list(Object value, int upper) { require(value instanceof List<?> && ((List<?>) value).size() <= upper, "bounded list is missing or exceeds prior cap"); return (List<?>) value; }
    static List<String> strings(Object value, int count, int length) { var result = new ArrayList<String>(); for (Object item : list(value, count)) result.add(text(item, length)); return List.copyOf(result); }
    static long number(Map<String, Object> object, String field) {
        Object value = object.get(field); require((value instanceof Integer || value instanceof Long) && ((Number) value).longValue() >= 0, "exact nonnegative integer is missing: " + field);
        return ((Number) value).longValue();
    }
    private static void signed(Object value) { require((value instanceof Integer || value instanceof Long) && ((Number) value).longValue() >= Integer.MIN_VALUE && ((Number) value).longValue() <= Integer.MAX_VALUE, "actual defining loader identity is missing"); }
    static String text(Object value, int upper) { require(value instanceof String && ((String) value).length() <= upper && !((String) value).isBlank(), "bounded text is missing"); return (String) value; }
    private static String digest(String value) { require(value.matches("[a-f0-9]{64}"), "registered digest differs"); return value; }
    static Path path(String value) { text(value, 4096); Path path = Path.of(value); require(path.isAbsolute() && path.equals(path.normalize()), "exact absolute path is missing"); return path; }
    static Blob blob(Map<String, Object> value) { return new Blob(digest(text(value.get("sha256"), 64)), number(value, "bytes")); }
    private static MessageDigest sha() { try { return MessageDigest.getInstance("SHA-256"); } catch (Exception impossible) { throw new AssertionError(impossible); } }
    static String hash(byte[] bytes) { return HexFormat.of().formatHex(sha().digest(bytes)); }
    private static Map<String, Object> blobEvidence(Acquired value) { return Map.of("path", value.path().toString(), "sha256", value.blob().sha256(), "bytes", value.blob().bytes()); }
    static Map<String, Object> throwable(Throwable value) {
        var out = new LinkedHashMap<String, Object>(); out.put("type", value.getClass().getName());
        String message = value.getMessage(); out.put("message", message == null ? "" : message.substring(0, Math.min(message.length(), 4096)));
        var frames = new ArrayList<String>(); StackTraceElement[] stack = value.getStackTrace();
        for (int i = 0; i < Math.min(stack.length, 32); i++) { String frame = stack[i].toString(); frames.add(frame.substring(0, Math.min(frame.length(), 512))); }
        out.put("frames", List.copyOf(frames)); out.put("omittedFrames", Math.max(0, stack.length - frames.size()));
        out.put("suppressedTypes", Arrays.stream(value.getSuppressed()).limit(8).map(t -> t.getClass().getName()).toList());
        if (value.getCause() != null && value.getCause() != value) { out.put("causeType", value.getCause().getClass().getName()); String cause = value.getCause().getMessage(); out.put("causeMessage", cause == null ? "" : cause.substring(0, Math.min(cause.length(), 4096))); }
        return Map.copyOf(out);
    }
    private static void preserve(Path output, Acquired acquired, String filename) throws IOException {
        Path target = output.resolve(filename); Files.write(target, acquired.bytes(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        require(Files.size(target) == acquired.blob().bytes(), "acquired snapshot write is incomplete");
    }
    static void write(Path path, Map<String, Object> evidence) throws IOException {
        var prior = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(65536, 16, 65536, OUTPUT_BYTES, 256, 24);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(evidence, prior).value();
        byte[] bytes = JsonWriter.write(frozen).getBytes(StandardCharsets.UTF_8);
        require(bytes.length <= OUTPUT_BYTES, "external receipt exceeds its independent output acquisition cap");
        Files.write(path, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }
    static void require(boolean value, String message) { if (!value) throw new AssertionError("full method external acquisition " + message); }
}
