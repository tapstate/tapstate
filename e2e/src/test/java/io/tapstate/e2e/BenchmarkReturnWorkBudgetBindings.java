package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Compares typed registration and independently obtained byte/origin facts without performing IO. */
final class BenchmarkReturnWorkBudgetBindings {
    static final int SCHEMA_VERSION = 1, PRODUCER_WRITER_SCHEMA_VERSION = 2, MAX_CLASSES = 512, MAX_CLASSPATH = 512;
    static final long MAX_MANIFEST_BYTES = 2L * 1024 * 1024, MAX_MEMBER_BYTES = 1024 * 1024;

    enum Owner { ROOT, PRODUCER }
    enum Lifecycle { FAILSAFE_VERIFY, SUREFIRE_TEST }

    /** This roster covers named measurement source units, rather than every application dependency. */
    enum SourceRole {
        ROOT_BUDGET("BenchmarkReturnWorkBudget"), ROOT_ADMISSION("BenchmarkReturnWorkBudgetAdmission"),
        ROOT_DEFINITIONS("BenchmarkReturnWorkBudgetDefinitions"), ROOT_PROFILE("BenchmarkReturnWorkBudgetProfile"),
        ROOT_REPORT_PLAN("BenchmarkReturnWorkBudgetReportPlan"), ROOT_RUN("BenchmarkReturnWorkBudgetRun"),
        LOCAL_SOURCE_WORK("BenchmarkReturnLocalSourceWork"),
        ROOT_BINDINGS("BenchmarkReturnWorkBudgetBindings"), ROOT_SEAL("BenchmarkReturnWorkBudgetProducerSeal"),
        BINDINGS_IO("BenchmarkReturnWorkBudgetBindingsIo"), MANIFEST_READER("BenchmarkReturnBindingsManifestReader"),
        ROOT_ORIGINS("BenchmarkReturnRootOrigins"), PRODUCER_ORIGIN_READER("BenchmarkReturnProducerOrigins"),
        EMITTED_ROSTER("BenchmarkReturnEmittedRoster"),
        ROOT_ALLOCATIONS("BenchmarkReturnWorkBudgetProducerAllocations"), ROOT_CLOCK_WORK("BenchmarkReturnRootClockWork"),
        ROOT_SOURCE_TAILS("BenchmarkReturnWorkBudgetSourceTails"), ROOT_CPU("BenchmarkRootCpuEnvelope"),
        NATIVE_QUEUE("BenchmarkNativeQueueProbe"), NATIVE_BASELINE("BenchmarkNativeCounterBaseline"),
        ENVIRONMENT("BenchmarkForkEnvironment"), OWNED_PROCESS("BenchmarkOwnedProcessReceipt"),
        READER("BenchmarkWriteReturnReader"), PROCESS_PROBE("BenchmarkProcessProbe"),
        CAPTURE("BenchmarkWriteReturnCapture"), ASSEMBLY("BenchmarkWriteReturnAssembly"),
        LEDGER("BenchmarkWriteReturnLedger"), RETURN_SAMPLER("BenchmarkReturnClockSampler"),
        RESOURCE_SAMPLER("BenchmarkResourceSampler"), COMMAND_SAMPLER("BenchmarkMongoCommandSampler"),
        TARGET_SAMPLER("BenchmarkTargetClockSampler"), TARGET_CLOCK("BenchmarkTargetClock"),
        DELIVERY_OBSERVER("BenchmarkMongoDeliveryObserver"), TERMINAL_OBSERVER("BenchmarkTableTerminalObserver"),
        TABLE_CAPTURE("BenchmarkTableCaptureSet"), EXPECTATIONS("BenchmarkWriteReturnExpectations"),
        TIME_BOUNDS("BenchmarkReturnTimeBounds"), RESOURCE_BOUNDS("BenchmarkReturnResourceBounds"),
        COMMON_WINDOW("BenchmarkReturnCommonWindow"), NOMINAL_CLOCK("BenchmarkNativeNominalReturnClock"),
        NATIVE_CLOCK("BenchmarkNativeReturnClock"), NATIVE_EVIDENCE("BenchmarkNativeClockEvidence"),
        POINT_CLOCK("BenchmarkReturnPointClock"), CAUSAL_CLOCK("BenchmarkCausalClock"),
        NOMINAL_ENCLOSURES("BenchmarkNominalCounterEnclosures"), PHASE_EVIDENCE("BenchmarkWriteReturnPhaseEvidence"),
        WORKLOAD("BenchmarkWorkloadDefinitions"), EXPECTED_CHANGES("BenchmarkExpectedChanges"),
        BOUNDARY_WRITES("BenchmarkBoundaryWrites"), END_MARKERS("BenchmarkMeasuredEndMarkers"),
        TERMINAL_CHANGES("BenchmarkTerminalTargetChanges"), PREFLIGHT_WRITES("BenchmarkPreflightWrites"),
        ACK_ORACLE("BenchmarkAckOracle"), SOURCE_LINEAGE("BenchmarkSourceLineage"),
        TERMINAL_CAPTURE("BenchmarkTerminalCapture"), TERMINAL_METADATA("BenchmarkTerminalMetaReceipt"),
        UNREAD_SAMPLER("BenchmarkUnreadSampler"), POSITION_COVERAGE("BenchmarkConnectorPositionCoverage"),
        STEADY_WINDOW("BenchmarkSteadyOutputWindow"), FAILURE_RETENTION("BenchmarkReturnFailureRetention"),
        COST_STAGE_READER("BenchmarkWriteReturnCostStages"), HARNESS("PipelineBenchmarkHarness"),
        COMPARISON("PipelineBenchmarkComparison"), AWAIT("Await"), DRIVER("RealBenchmarkForkDriver"),
        DRIVER_ENTRY("RealBenchmarkForkDriverIT"),
        PDK_BUDGET("PdkWriteReturnWorkBudget", "adapter-pdk"), PDK_PROBE("PdkWriteReturnProbe", "adapter-pdk"),
        PDK_BEAN("PdkWriteReturnProbeMBean", "adapter-pdk"), PDK_SCOPE("PdkMongoWriteScope", "adapter-pdk"),
        PDK_CLOCK("PdkBenchmarkClock", "adapter-pdk"), PDK_WRITER("PdkSinkWriter", "adapter-pdk"),
        PDK_ORIGINS("PdkReturnOrigins", "adapter-pdk"),
        JSON_WRITER("JsonWriter", "core-common"), JSON_READER("JsonReader", "core-common"), JNI_SOURCE,
        SHARED_SOURCE_WORK("BenchmarkSharedSupportSourceWork"), SHARED_POSTGRES("SharedPostgres"), SHARED_MONGO("SharedMongo"),
        REAL_PROCESS_SERVER("RealProcessServer"), SERVER_HANDLE("ServerHandle"),
        DOCKER_GATE("DockerGate", "test-support"),
        RETURN_INTERVAL_COMPARISON("BenchmarkReturnIntervalComparison"),
        RETURN_INTERVAL_METRICS("BenchmarkReturnIntervalMetrics"),
        RETURN_METHOD_ADMISSION("BenchmarkReturnMethodAdmission");

        private final String binaryName, sourcePath, module;
        SourceRole(String name) {
            module = "e2e"; binaryName = "io.tapstate.e2e." + name;
            sourcePath = "e2e/src/test/java/" + binaryName.replace('.', '/') + ".java";
        }
        SourceRole(String name, String module) {
            this.module = module;
            binaryName = (module.equals("adapter-pdk") ? "io.tapstate.adapters.pdk."
                    : module.equals("test-support") ? "io.tapstate.testsupport." : "io.tapstate.core.common.") + name;
            sourcePath = (module.equals("adapter-pdk") ? "adapters/adapter-pdk"
                    : module.equals("test-support") ? "test-support" : "core/core-common")
                    + (module.equals("test-support") ? "/src/test/java/" : "/src/main/java/")
                    + binaryName.replace('.', '/') + ".java";
        }
        SourceRole() { module = "native"; binaryName = null; sourcePath = "e2e/src/test/resources/benchmark-clock/mach_clock_jni.c"; }
        String binaryName() { return binaryName; }
        String sourcePath() { return sourcePath; }
        String module() { return module; }
        Set<Owner> owners() { return module.equals("e2e") || module.equals("test-support") ? Set.of(Owner.ROOT) : Set.of(Owner.ROOT, Owner.PRODUCER); }
    }

    enum JdkRole {
        JAVA("bin/java"), JVM("lib/server/libjvm.dylib"), MODULES("lib/modules"), RELEASE("release"),
        CPU_PROVIDER("lib/libmanagement_ext.dylib");
        private final String relativePath;
        JdkRole(String path) { relativePath = path; }
        String relativePath() { return relativePath; }
    }

    record Blob(String sha256, long bytes) {
        Blob { digest(sha256); require(bytes > 0 && bytes < Long.MAX_VALUE, "positive bounded byte fact required"); }
    }
    record FileFact(BenchmarkReturnWorkBudgetProfile.FileRole role, String path, Blob blob) {
        FileFact { require(role != null && blob != null, "file role or material missing"); absolute(path); }
    }
    record Source(SourceRole role, String path, Blob blob) {
        Source { require(role != null && blob != null, "source role or material missing"); text(path, 512); }
    }
    record CompiledClass(SourceRole role, String binaryName, Blob blob) {
        CompiledClass { require(role != null && blob != null, "compiled role or material missing"); text(binaryName, 256); }
    }
    /** A separate producer writer does not replace the root compiler's writer or any common helper. */
    record ProducerWriterBinding(Owner owner, String sourceRoot, Source source, CompiledClass compiled) {
        ProducerWriterBinding {
            require(owner == Owner.PRODUCER && source != null && compiled != null, "producer writer owner or material differs");
            absolute(sourceRoot);
            require(source.role() == SourceRole.PDK_WRITER && source.path().equals(SourceRole.PDK_WRITER.sourcePath())
                    && compiled.role() == SourceRole.PDK_WRITER && compiled.binaryName().equals(SourceRole.PDK_WRITER.binaryName()),
                    "producer writer role binary or source path differs");
            require(source.blob().bytes() <= MAX_MEMBER_BYTES && compiled.blob().bytes() <= MAX_MEMBER_BYTES,
                    "producer writer exceeds prior member cap");
            require(Path.of(sourceRoot).resolve(source.path()).toString().length() <= 4096
                    && Path.of(sourceRoot).resolve("adapters/adapter-pdk/target/classes")
                    .resolve(compiled.binaryName().replace('.', '/') + ".class").toString().length() <= 4096,
                    "producer writer derived path exceeds its prior cap");
        }
        String compilerEntry() { return compiled.binaryName().replace('.', '/') + ".class"; }
        String compilerPath() { return Path.of(sourceRoot).resolve("adapters/adapter-pdk/target/classes").resolve(compilerEntry()).toString(); }
        Map<String, Object> evidence() {
            return Map.of("owner", owner.name(), "sourceRoot", sourceRoot, "role", source.role().name(), "sourcePath", source.path(),
                    "sourceSha256", source.blob().sha256(), "sourceBytes", source.blob().bytes(), "binaryName", compiled.binaryName(),
                    "classSha256", compiled.blob().sha256(), "classBytes", compiled.blob().bytes(), "compilerPath", compilerPath());
        }
    }
    record Origin(Owner owner, SourceRole role, String binaryName, String container, String entry,
                  Blob classBytes, String applicationSha256) {
        Origin {
            require(owner != null && role != null && classBytes != null, "class owner or bytes missing");
            text(binaryName, 256); absolute(container); text(entry, 1024);
            if (applicationSha256 != null) { digest(applicationSha256); }
        }
    }
    record Runtime(Owner owner, String runId, long pid, long jvmStartTimeMillis, String javaHome, String javaVersion) {
        Runtime {
            require(owner != null && pid > 0 && jvmStartTimeMillis > 0, "owned runtime missing");
            text(runId, 128); absolute(javaHome); text(javaVersion, 128);
        }
    }
    record OwnedRuntimes(Runtime root, Runtime producer) {
        OwnedRuntimes {
            require(root != null && producer != null && root.owner() == Owner.ROOT && producer.owner() == Owner.PRODUCER
                    && root.pid() != producer.pid(), "distinct root and producer identities required");
        }
    }
    record ObservedOrigin(Origin origin, Runtime runtime) {
        ObservedOrigin { require(origin != null && runtime != null, "actual class origin or owner missing"); }
    }
    record JdkFile(JdkRole role, String path, Blob blob) {
        JdkFile { require(role != null && blob != null, "JDK role or material missing"); absolute(path); }
    }
    record HelperManifest(int schemaVersion, String runId, String inputSha256, String buildId,
            String rootDefinitionId, String rootFormulaSha256, String producerFormulaSha256, Blob failsafeXml,
            List<String> failsafeClasspath, List<Source> sources, List<CompiledClass> classes, List<Origin> origins,
            ProducerWriterBinding producerWriter) {
        HelperManifest(int schemaVersion, String runId, String inputSha256, String buildId,
                String rootDefinitionId, String rootFormulaSha256, String producerFormulaSha256, Blob failsafeXml,
                List<String> failsafeClasspath, List<Source> sources, List<CompiledClass> classes, List<Origin> origins) {
            this(schemaVersion, runId, inputSha256, buildId, rootDefinitionId, rootFormulaSha256, producerFormulaSha256,
                    failsafeXml, failsafeClasspath, sources, classes, origins, null);
        }
        HelperManifest {
            text(runId, 128); digest(inputSha256); text(buildId, 128); text(rootDefinitionId, 128);
            digest(rootFormulaSha256); digest(producerFormulaSha256); require(failsafeXml != null, "Failsafe XML fact missing");
            failsafeClasspath = copy(failsafeClasspath, MAX_CLASSPATH); sources = copy(sources, SourceRole.values().length);
            classes = copy(classes, MAX_CLASSES); origins = copy(origins, 2 * MAX_CLASSES);
        }
    }
    record JdkManifest(int schemaVersion, String runId, String inputSha256, String javaHome, String javaVersion,
                       List<JdkFile> files) {
        JdkManifest { text(runId, 128); digest(inputSha256); absolute(javaHome); text(javaVersion, 128); files = copy(files, JdkRole.values().length); }
    }
    record ActualFacts(String runId, String inputSha256, String buildId, Lifecycle lifecycle, Blob failsafeXml,
            List<String> failsafeClasspath, List<FileFact> files, List<Source> sources, List<CompiledClass> classes,
            List<ObservedOrigin> origins, List<JdkFile> jdkFiles, Set<String> claimedQualifications,
            ProducerWriterBinding producerWriter) {
        ActualFacts(String runId, String inputSha256, String buildId, Lifecycle lifecycle, Blob failsafeXml,
                List<String> failsafeClasspath, List<FileFact> files, List<Source> sources, List<CompiledClass> classes,
                List<ObservedOrigin> origins, List<JdkFile> jdkFiles, Set<String> claimedQualifications) {
            this(runId, inputSha256, buildId, lifecycle, failsafeXml, failsafeClasspath, files, sources, classes,
                    origins, jdkFiles, claimedQualifications, null);
        }
        ActualFacts {
            text(runId, 128); digest(inputSha256); text(buildId, 128);
            require(lifecycle != null && failsafeXml != null && claimedQualifications != null, "actual provenance missing");
            failsafeClasspath = copy(failsafeClasspath, MAX_CLASSPATH); files = copy(files, BenchmarkReturnWorkBudgetProfile.FileRole.values().length);
            sources = copy(sources, SourceRole.values().length); classes = copy(classes, MAX_CLASSES);
            origins = copy(origins, 2 * MAX_CLASSES); jdkFiles = copy(jdkFiles, JdkRole.values().length);
            require(claimedQualifications.size() <= 16, "qualification claim roster exceeds bound");
            claimedQualifications = Set.copyOf(claimedQualifications);
        }
    }

    private record OriginKey(Owner owner, String binaryName) { }
    private BenchmarkReturnWorkBudgetBindings() { }

    static final class AuthenticatedProfile {
        private final BenchmarkReturnWorkBudgetProfile.Prepared prepared;
        private final BenchmarkReturnWorkBudget.Definition definition;
        private AuthenticatedProfile(BenchmarkReturnWorkBudgetProfile.Prepared prepared) {
            this.prepared = prepared; this.definition = prepared.requireDefinition();
        }
        BenchmarkReturnWorkBudgetProfile.Prepared prepared() { return prepared; }
        BenchmarkReturnWorkBudget.Definition definition() { return definition; }
    }

    /** Authenticate once before consuming a run; later fact comparisons do not rebuild its workload. */
    static AuthenticatedProfile authenticate(BenchmarkReturnWorkBudgetProfile.Prepared prepared) {
        require(prepared != null, "binding input missing");
        require(prepared.equals(BenchmarkReturnWorkBudgetProfile.inspect(prepared.inputs())), "source factory registration differs");
        return new AuthenticatedProfile(prepared);
    }

    static Map<String, Object> verify(BenchmarkReturnWorkBudgetProfile.Prepared prepared, HelperManifest helper,
            JdkManifest jdk, ActualFacts actual, OwnedRuntimes owners) {
        return verify(authenticate(prepared), helper, jdk, actual, owners);
    }

    static Map<String, Object> verify(AuthenticatedProfile authenticated, HelperManifest helper,
            JdkManifest jdk, ActualFacts actual, OwnedRuntimes owners) {
        require(authenticated != null && helper != null && jdk != null && actual != null && owners != null, "binding input missing");
        var prepared = authenticated.prepared();
        var definition = authenticated.definition();
        var inputs = prepared.inputs();
        require(supportedHelper(helper) && jdk.schemaVersion() == SCHEMA_VERSION, "manifest schema differs");
        require(java.util.Objects.equals(inputs.producerWriter(), helper.producerWriter())
                && java.util.Objects.equals(helper.producerWriter(), actual.producerWriter()), "actual producer writer source or compiled binding differs");
        require(inputs.runId().equals(helper.runId()) && inputs.runId().equals(jdk.runId()) && inputs.runId().equals(actual.runId())
                && inputs.inputSha256().equals(helper.inputSha256()) && inputs.inputSha256().equals(jdk.inputSha256())
                && inputs.inputSha256().equals(actual.inputSha256()), "registered input or run identity differs");
        require(definition.definitionId().equals(helper.rootDefinitionId()) && definition.sourceFormulaSha256().equals(helper.rootFormulaSha256())
                && BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256.equals(helper.producerFormulaSha256()), "source formula binding differs");
        require(actual.claimedQualifications().isEmpty(), "caller supplied a qualification claim");
        require(helper.buildId().equals(actual.buildId()) && actual.lifecycle() == Lifecycle.FAILSAFE_VERIFY
                && helper.failsafeXml().equals(actual.failsafeXml()), "fresh build or actual Failsafe provenance differs");
        require(helper.failsafeClasspath().equals(actual.failsafeClasspath()), "actual Failsafe classpath differs");
        var classpath = new java.util.HashSet<String>();
        for (String path : helper.failsafeClasspath()) { absolute(path); require(classpath.add(path), "classpath origin repeated"); }
        require(!classpath.isEmpty(), "actual Failsafe classpath missing");

        var files = fileFacts(inputs, actual.files());
        var sources = sources(helper.sources());
        require(sources.equals(sources(actual.sources())), "actual source bytes differ");
        var classes = classes(helper.classes());
        require(classes.equals(classes(actual.classes())), "actual compiled class bytes differ");
        var origins = origins(helper.origins(), classes, files, classpath, helper.producerWriter());
        var observed = new ArrayList<Origin>();
        for (var origin : actual.origins()) {
            var expected = origin.origin().owner() == Owner.ROOT ? owners.root() : owners.producer();
            require(origin.runtime().equals(expected), "actual class runtime identity differs");
            observed.add(origin.origin());
        }
        require(origins.equals(origins(observed, classes, files, classpath, helper.producerWriter())), "actual class origin differs");
        verifyJdk(inputs, jdk, actual, owners, files);
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("schemaVersion", SCHEMA_VERSION); evidence.put("state", "REGISTERED_BINDING_FACTS_MATCHED");
        evidence.put("runId", inputs.runId()); evidence.put("inputSha256", inputs.inputSha256()); evidence.put("buildId", helper.buildId());
        evidence.put("rootDefinitionId", definition.definitionId()); evidence.put("rootFormulaSha256", definition.sourceFormulaSha256());
        evidence.put("producerFormulaSha256", helper.producerFormulaSha256()); evidence.put("sourceUnits", sources.size());
        evidence.put("compiledMembers", classes.size()); evidence.put("ownedOrigins", origins.size()); evidence.put("jdkArtifacts", jdk.files().size());
        evidence.put("sourceScope", "FIXED_NAMED_SOURCE_UNITS_AND_REGISTERED_EMITTED_MEMBER_ROSTER");
        evidence.put("byteScope", "SUPPLIED_FRESH_SOURCE_COMPILED_APPLICATION_ENTRY_AND_RUNTIME_RESOURCE_FACTS");
        evidence.put("externalRequirement", "IO_FACT_AUTHENTICATION_EMITTED_ROSTER_COMPLETENESS_AND_VM_TRANSFORMATION_NOT_PROVEN_HERE");
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> evidence.put(flag, false));
        evidence.put("structuralWorkBudgetQualified", false); evidence.put("wholeMethodCostQualified", false);
        evidence.put("causalOverheadQualified", false); evidence.put("runtimeInstructionBytesQualified", false);
        if (helper.producerWriter() != null) { evidence.put("producerWriter", helper.producerWriter().evidence()); }
        return Collections.unmodifiableMap(evidence);
    }

    static boolean supportedHelper(HelperManifest helper) {
        return helper != null && (helper.schemaVersion() == SCHEMA_VERSION && helper.producerWriter() == null
                || helper.schemaVersion() == PRODUCER_WRITER_SCHEMA_VERSION && helper.producerWriter() != null);
    }

    static CompiledClass compiledForOwner(Map<String, CompiledClass> classes, Owner owner, String binary,
            ProducerWriterBinding producerWriter) {
        var root = classes.get(binary);
        if (producerWriter != null && owner == Owner.PRODUCER && SourceRole.PDK_WRITER.binaryName().equals(binary)) {
            require(root != null && root.role() == SourceRole.PDK_WRITER, "root writer compiled fact is missing");
            return producerWriter.compiled();
        }
        return root;
    }

    private static Map<BenchmarkReturnWorkBudgetProfile.FileRole, FileFact> fileFacts(
            BenchmarkReturnWorkBudgetProfile.Inputs inputs, List<FileFact> actual) {
        var expected = new EnumMap<BenchmarkReturnWorkBudgetProfile.FileRole, BenchmarkReturnWorkBudgetProfile.FileInput>(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        for (var file : inputs.files()) { require(expected.put(file.role(), file) == null, "registered file role repeated"); }
        var required = EnumSet.allOf(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        if (!inputs.flags().rootCpu()) { required.remove(BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY); }
        require(expected.keySet().equals(required), "registered file role roster differs");
        var facts = new EnumMap<BenchmarkReturnWorkBudgetProfile.FileRole, FileFact>(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        for (var fact : actual) {
            require(facts.put(fact.role(), fact) == null && expected.containsKey(fact.role()), "actual file role repeated or foreign");
            var prior = expected.get(fact.role());
            require(fact.blob().bytes() == prior.bytes() && fact.blob().sha256().equals(prior.sha256()), "actual file digest or prior size differs");
            if (fact.role() == BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST
                    || fact.role() == BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST) {
                require(fact.blob().bytes() <= MAX_MANIFEST_BYTES, "manifest exceeds prior material cap");
            }
        }
        require(facts.keySet().equals(required), "actual required file role missing");
        return facts;
    }

    private static Map<SourceRole, Source> sources(List<Source> list) {
        var facts = new EnumMap<SourceRole, Source>(SourceRole.class);
        long total = 0;
        for (var source : list) {
            require(source.path().equals(source.role().sourcePath()) && source.blob().bytes() <= MAX_MEMBER_BYTES, "source path or prior member cap differs");
            require(facts.put(source.role(), source) == null, "source role repeated"); total = Math.addExact(total, source.blob().bytes());
        }
        require(facts.keySet().equals(EnumSet.allOf(SourceRole.class)), "required source roster missing");
        require(total <= Math.multiplyExact(MAX_MEMBER_BYTES, SourceRole.values().length), "source aggregate cap exceeded");
        return facts;
    }

    private static Map<String, CompiledClass> classes(List<CompiledClass> list) {
        var classes = new HashMap<String, CompiledClass>();
        for (var item : list) {
            String prefix = item.role().binaryName();
            require(prefix != null && (item.binaryName().equals(prefix)
                    || item.binaryName().startsWith(prefix + "$") && item.binaryName().substring(prefix.length() + 1).matches("[A-Za-z0-9_$]+")),
                    "compiled class belongs to a foreign source owner");
            require(item.blob().bytes() <= MAX_MEMBER_BYTES, "compiled class exceeds prior member cap");
            require(classes.put(item.binaryName(), item) == null, "compiled class repeated");
        }
        for (var role : SourceRole.values()) {
            if (role.binaryName() != null) { require(classes.containsKey(role.binaryName()), "required compiled source owner missing"); }
        }
        for (String suffix : List.of("$Clock", "$JniAccess", "$NativeAccess")) {
            require(classes.containsKey(SourceRole.PDK_CLOCK.binaryName() + suffix), "required native clock member missing");
        }
        return classes;
    }

    private static Map<OriginKey, Origin> origins(List<Origin> list, Map<String, CompiledClass> classes,
            Map<BenchmarkReturnWorkBudgetProfile.FileRole, FileFact> files, Set<String> classpath, ProducerWriterBinding producerWriter) {
        var facts = new HashMap<OriginKey, Origin>();
        var application = files.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR);
        for (var origin : list) {
            var compiled = compiledForOwner(classes, origin.owner(), origin.binaryName(), producerWriter);
            require(compiled != null && compiled.role() == origin.role() && compiled.blob().equals(origin.classBytes())
                    && origin.role().owners().contains(origin.owner()), "class bytes or source owner differs");
            String member = origin.binaryName().replace('.', '/') + ".class";
            if (origin.owner() == Owner.ROOT) {
                require(classpath.contains(origin.container()) && origin.entry().equals(member) && origin.applicationSha256() == null,
                        "root class origin is outside actual Failsafe inputs");
            } else {
                String prefix = "BOOT-INF/lib/" + origin.role().module() + "-";
                require(origin.container().equals(application.path()) && application.blob().sha256().equals(origin.applicationSha256())
                        && origin.entry().startsWith(prefix) && origin.entry().endsWith(".jar!/" + member)
                        && origin.entry().substring(prefix.length(), origin.entry().indexOf(".jar!/")).matches("[A-Za-z0-9._-]+"),
                        "producer application entry or artifact owner differs");
            }
            require(facts.put(new OriginKey(origin.owner(), origin.binaryName()), origin) == null, "class origin repeated");
        }
        for (var compiled : classes.values()) {
            for (var owner : compiled.role().owners()) { require(facts.containsKey(new OriginKey(owner, compiled.binaryName())), "required owned class origin missing"); }
        }
        return facts;
    }

    private static void verifyJdk(BenchmarkReturnWorkBudgetProfile.Inputs inputs, JdkManifest registered, ActualFacts actual,
            OwnedRuntimes owners, Map<BenchmarkReturnWorkBudgetProfile.FileRole, FileFact> files) {
        require(registered.javaVersion().equals("21") || registered.javaVersion().startsWith("21."), "unsupported JDK version");
        for (var runtime : List.of(owners.root(), owners.producer())) {
            require(runtime.runId().equals(inputs.runId()) && runtime.javaHome().equals(registered.javaHome())
                    && runtime.javaVersion().equals(registered.javaVersion()), "owned JDK runtime differs");
        }
        var expected = jdkFiles(registered.javaHome(), registered.files(), inputs.flags().rootCpu());
        require(expected.equals(jdkFiles(registered.javaHome(), actual.jdkFiles(), inputs.flags().rootCpu())), "actual JDK artifact differs");
        if (inputs.flags().rootCpu()) {
            var cpu = files.get(BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY);
            var jdkCpu = expected.get(JdkRole.CPU_PROVIDER);
            require(cpu.path().equals(jdkCpu.path()) && cpu.blob().equals(jdkCpu.blob()), "CPU provider file role differs from owned JDK");
        }
    }
    private static Map<JdkRole, JdkFile> jdkFiles(String home, List<JdkFile> list, boolean cpu) {
        var files = new EnumMap<JdkRole, JdkFile>(JdkRole.class);
        var required = EnumSet.allOf(JdkRole.class); if (!cpu) { required.remove(JdkRole.CPU_PROVIDER); }
        for (var file : list) {
            require(file.path().equals(Path.of(home).resolve(file.role().relativePath()).toString()) && required.contains(file.role()),
                    "JDK path or role differs");
            require(files.put(file.role(), file) == null, "JDK artifact repeated");
        }
        require(files.keySet().equals(required), "required JDK artifact missing"); return files;
    }

    private static <T> List<T> copy(List<T> values, int bound) {
        require(values != null && values.size() <= bound, "typed roster missing or exceeds prior cap"); return List.copyOf(values);
    }
    private static void absolute(String value) {
        text(value, 4096); Path path = Path.of(value); require(path.isAbsolute() && path.equals(path.normalize()), "origin path is not absolute and normalized");
    }
    private static void text(String value, int bound) {
        require(value != null && !value.isBlank() && value.length() <= bound && value.chars().allMatch(c -> c >= 32 && c <= 126), "binding text missing or exceeds prior cap");
    }
    private static void digest(String value) { require(value != null && value.matches("[a-f0-9]{64}"), "binding digest missing or malformed"); }
    private static void require(boolean value, String reason) { if (!value) { throw new AssertionError("return work bindings " + reason); } }
}
