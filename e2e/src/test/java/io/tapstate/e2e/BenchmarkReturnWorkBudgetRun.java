package io.tapstate.e2e;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;

/** Binds a source profile to registered files before services and to the original child after exit. */
final class BenchmarkReturnWorkBudgetRun implements AutoCloseable {
    static final String PREFIX = "tapstate.e2e.benchmark.write-return-work-budget-registration.";
    private static final AtomicReference<BenchmarkReturnWorkBudgetRun> ACTIVE = new AtomicReference<>();
    private final BenchmarkReturnWorkBudgetProfile.Prepared profile;
    private final BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile validatedProfile;
    private final BenchmarkReturnWorkBudget.Definition definition;
    private final BenchmarkReturnWorkBudget.Scope scope;
    private final Map<BenchmarkReturnWorkBudgetProfile.FileRole, Path> paths;
    private final Path output, producerBody;
    private final String nonce;
    private final BenchmarkReturnWorkBudgetDefinitions.ProducerInput producerInput;
    private final BenchmarkWorkloadDefinitions.Workload canonicalWorkload;
    private final NativeClassPacketRoster nativeClassPacketRoster;
    private BenchmarkReturnWorkBudgetProducerSeal.Registration producerRegistration;
    private Map<String, Object> producerEvidence = Map.of("state", "NOT_BOUND");
    private final java.util.ArrayList<Capture> captures = new java.util.ArrayList<>();
    private boolean finished;
    record Capture(long epoch, String window, long calls, long rows) { }
    private BenchmarkReturnWorkBudgetAdmission.Runtime rootRuntime;
    private HelperManifest helperManifest;
    private JdkManifest jdkManifest;
    private BenchmarkReturnWorkBudgetBindingsIo.Result authenticated;
    private Map<String, Object> bindingEvidence = Map.of("state", "NOT_AUTHENTICATED");
    private Map<String, Object> classpathEvidence = Map.of("state", "NOT_READ");
    private List<String> rootClasspath = List.of();
    private List<ObservedOrigin> observedRootOrigins = List.of();
    private Map<String, Object> rootOriginEvidence = Map.of("state", "NOT_OBSERVED");
    private Path sourceRoot;
    private BenchmarkReturnEmittedRoster.Observation emittedRoster;
    private final java.util.ArrayList<Map<String, Object>> producerClassMetadata = new java.util.ArrayList<>();

    private TrustedPreflight trustedPreflight;
    private String earlyRootMetadata;
    private String earlyRootMetadataPhase;

    /** Issued only from this Run's acquired bytes, observed ROOT origins and actual cold getter. */
    static final class TrustedPreflight {
        private final BenchmarkReturnWorkBudgetRun owner;
        private final BenchmarkReturnMethodAdmission.Preflight facts;
        private TrustedPreflight(BenchmarkReturnWorkBudgetRun owner, BenchmarkReturnMethodAdmission.Preflight facts) {
            this.owner = owner; this.facts = facts;
        }
        BenchmarkReturnMethodAdmission.Preflight requireActiveFacts() {
            return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.RUN_TRUSTED_TOKEN,
                    () -> {
                        owner.requireActive();
                        require(owner.trustedPreflight == this && owner.earlyRootMetadata != null,
                                "trusted preflight was not issued by the active acquired Run");
                        return facts;
                    });
        }
    }

    static TrustedPreflight acquireTrustedPreflight() {
        var run = current();
        require(run != null, "trusted preflight requires an actual acquired Run");
        return run.acquireTrustedPreflightOnce();
    }

    private TrustedPreflight acquireTrustedPreflightOnce() {
        var entry = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.RUN_TRUSTED_ACQUIRE);
        try {
        requireActive();
        require(trustedPreflight == null && earlyRootMetadata == null && producerRegistration == null
                        && producerClassMetadata.isEmpty() && captures.isEmpty(),
                "trusted preflight is repeated or follows producer/phase acquisition");
        require(rootRuntime != null && authenticated != null && helperManifest != null && jdkManifest != null
                        && emittedRoster != null && !observedRootOrigins.isEmpty(),
                "trusted preflight requires acquired bytes and actual root origins");
            require(helperManifest.origins().size() <= 1024 && authenticated.origins().size() <= 1024
                            && observedRootOrigins.size() <= 512 && authenticated.classes().size() <= 512
                            && authenticated.sources().size() <= SourceRole.values().length && authenticated.files().size() <= 10,
                    "trusted preflight input roster exceeds its prior cap");
            require(java.util.Objects.equals(profile.inputs().producerWriter(), helperManifest.producerWriter())
                            && java.util.Objects.equals(profile.inputs().producerWriter(), authenticated.producerWriter()),
                    "trusted preflight producer writer tuple differs");
            var expected = new java.util.HashMap<String, Origin>();
            for (var origin : authenticated.origins()) if (origin.owner() == Owner.ROOT) {
                require(expected.put(origin.binaryName(), origin) == null, "authenticated ROOT origin is duplicated");
            }
            var runtime = new BenchmarkReturnWorkBudgetBindings.Runtime(Owner.ROOT, profile.inputs().runId(),
                    rootRuntime.pid(), rootRuntime.jvmStartTimeMillis(), jdkManifest.javaHome(), jdkManifest.javaVersion());
            for (var origin : observedRootOrigins) {
                require(runtime.equals(origin.runtime()) && origin.origin().owner() == Owner.ROOT
                                && origin.origin().equals(expected.remove(origin.origin().binaryName())),
                        "actual root origin or original runtime differs from authenticated input");
            }
            require(expected.isEmpty(), "actual ROOT origin roster is incomplete");
            // This is the first existing metadata invocation, moved ahead of configuration/fixtures.
            // It reads no counter. Its one-time phase reuse is explicitly an early recipe fact.
            String raw = rootClockMetadata();
            require(raw != null && raw.length() <= 8192 && raw.chars().allMatch(c -> c >= 32 && c <= 126),
                    "actual root cold metadata exceeds its prior scalar cap");
            var facts = new BenchmarkReturnMethodAdmission.Preflight(BenchmarkReturnMethodAdmission.METHOD,
                    BenchmarkReturnMethodAdmission.structuralRecipe(validatedProfile),
                    helperManifest, authenticated,
                    new BenchmarkCausalClock.Identity(rootRuntime.pid(), rootRuntime.jvmStartTimeMillis()),
                    paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toString(), raw);
            var result = new TrustedPreflight(this, facts);
            earlyRootMetadata = raw; trustedPreflight = result;
            BenchmarkReturnWorkBudget.done(entry);
            return result;
        } catch (RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(entry, failure); throw failure;
        }
    }

    /** No getter, clock, JMX or checkpoint is added; the first original phase invocation is consumed. */
    static String rootClockMetadataBefore(String phaseKey) {
        var run = current();
        if (run == null || run.trustedPreflight == null) { return rootClockMetadata(); }
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.RUN_ROOT_BEFORE, () -> {
        run.requireActive();
        require(phaseKey != null && !phaseKey.isEmpty() && phaseKey.length() <= 128
                        && phaseKey.chars().allMatch(c -> c >= 32 && c <= 126), "metadata phase key exceeds its prior cap");
        if (run.earlyRootMetadataPhase == null) {
            require(run.earlyRootMetadata != null, "early root metadata is missing");
            run.earlyRootMetadataPhase = phaseKey;
            return run.earlyRootMetadata;
        }
        require(!run.earlyRootMetadataPhase.equals(phaseKey), "early root metadata cannot be consumed twice for the same phase");
        // A distinct later phase retains the original fresh getter at its original site.
        return rootClockMetadata();
        });
    }

    static Map<String, Object> rootClockMetadataBeforeEvidence(Map<String, Object> value, String phaseKey) {
        var run = current();
        if (run == null || run.trustedPreflight == null) { return value; }
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.RUN_ROOT_WRAPPER, () -> {
        run.requireActive();
        BenchmarkReturnComponentSourceWork.guardColdWrapper(value);
        require(value != null && value.size() <= 128, "cold evidence wrapper exceeds its prior field cap");
        var annotated = new LinkedHashMap<String, Object>(value);
        annotated.put("rootBeforeObservationBasis", rootClockMetadataBeforeBasis(phaseKey));
        return Map.copyOf(annotated);
        });
    }

    /** Comparison only: input cannot issue or replace the privately acquired preflight. */
    static void requireEarlyRootMetadataMatchesAfter(String actualAfterRaw) {
        var run = current();
        if (run == null || run.trustedPreflight == null) { return; }
        BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.RUN_ROOT_COMPARE, () -> {
        run.requireActive();
        var facts = run.trustedPreflight.requireActiveFacts();
        BenchmarkReturnComponentSourceWork.guardNativeJson(facts.actualRootMetadata());
        BenchmarkReturnComponentSourceWork.guardNativeJson(actualAfterRaw);
        var before = BenchmarkNativeClockEvidence.parse(facts.actualRootMetadata(), facts.root(), facts.library());
        var after = BenchmarkNativeClockEvidence.parse(actualAfterRaw, facts.root(), facts.library());
        require("NATIVE_RECORDED".equals(before.get("state")) && "NATIVE_RECORDED".equals(after.get("state")),
                "early root recipe or actual post-window provider is UNKNOWN");
        require(BenchmarkNativeReturnClock.supportedRootSnapshot(before)
                        .equals(BenchmarkNativeReturnClock.supportedRootSnapshot(after)),
                "early root recipe differs from actual post-window route");
        return null;
        });
    }

    static String rootClockMetadataBeforeBasis(String phaseKey) {
        var run = current();
        if (run == null || run.trustedPreflight == null) { return "ORIGINAL_PHASE_ROOT_METADATA_GETTER"; }
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.RUN_ROOT_BASIS, () -> {
        require(phaseKey == null || phaseKey.length() <= 128, "metadata basis phase key exceeds its prior cap");
        return java.util.Objects.equals(run.earlyRootMetadataPhase, phaseKey)
                ? "EARLY_PREFLIGHT_ROOT_RECIPE_FACT_NOT_A_FRESH_PHASE_OBSERVATION"
                : "ORIGINAL_PHASE_ROOT_METADATA_GETTER";
        });
    }


    private BenchmarkReturnWorkBudgetRun(BenchmarkReturnWorkBudgetBindings.AuthenticatedProfile validatedProfile,
            Map<BenchmarkReturnWorkBudgetProfile.FileRole, Path> paths, Path output, Path body, String nonce) {
        this.validatedProfile = validatedProfile;
        this.profile = validatedProfile.prepared();
        var profile = this.profile;
        definition = profile.requireDefinition();
        canonicalWorkload = BenchmarkWorkloadDefinitions.steadyPilot(profile.inputs().workload().id());
        nativeClassPacketRoster = new NativeClassPacketRoster(canonicalWorkload);
        this.paths = Map.copyOf(paths); this.output = output; producerBody = body; this.nonce = nonce;
        long unlimited = Long.MAX_VALUE;
        producerInput = new BenchmarkReturnWorkBudgetDefinitions.ProducerInput(profile.inputs().runId(),
                profile.inputs().inputSha256(), new BenchmarkReturnWorkBudget.Work(unlimited, unlimited, unlimited,
                unlimited, unlimited, unlimited, unlimited, unlimited),
                BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES, 128, 2093056);
        scope = BenchmarkReturnWorkBudget.activate(definition, profile.inputs().runId());
    }

    static BenchmarkReturnWorkBudgetRun current() { return ACTIVE.get(); }

    /** Only a registered run and its captured scope select the extended owned cold getter. */
    static String coldProducerApplication(BenchmarkReturnWorkBudget.Scope captured, BenchmarkCausalClock.Identity identity) {
        var run = current();
        if (run == null || captured == null || captured != run.scope) { return null; }
        run.requireActive();
        require(run.producerRegistration != null && run.producerRegistration.runtime().equals(identity),
                "cold origin getter requires the registered owned producer");
        return run.paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).toString();
    }

    static void println(String value) {
        var run = current();
        if (run == null) { System.out.println(value); return; }
        run.requireActive();
        require(value != null && value.length() <= Math.addExact(run.profile.fieldsRegisteredOutputPlan().encodedUpperBytes(), 4096),
                "controlled stdout exceeds its prior report and prefix material bound");
        run.scope.add(BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.bytes(Math.multiplyExact(4L, value.length())));
        BenchmarkReturnWorkBudget.opaqueRequest(run.scope, BenchmarkReturnWorkBudget.Activity.REPORT_WRITE,
                () -> { System.out.println(value); return null; });
    }

    static void printf(String format, Object... values) {
        var run = current();
        if (run == null) { System.out.printf(format, values); return; }
        run.requireActive();
        require(format != null && format.length() <= 1024 && values != null && values.length <= 32,
                "controlled formatter exceeds its fixed argument or format bound");
        run.scope.add(BenchmarkReturnWorkBudget.Activity.BOOKKEEPING,
                new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, 4L * format.length(), 1, values.length, 0));
        BenchmarkReturnWorkBudget.opaqueRequest(run.scope, BenchmarkReturnWorkBudget.Activity.REPORT_WRITE,
                () -> { System.out.printf(format, values); return null; });
    }

    static String json(Object value) {
        var run = current();
        if (run == null) { return io.tapstate.core.common.JsonWriter.write(value); }
        run.requireActive();
        var plan = run.profile.fieldsRegisteredOutputPlan();
        var ticket = run.scope.begin(BenchmarkReturnWorkBudget.Activity.REPORT_SERIALIZE, BenchmarkReturnWorkBudget.Work.call());
        try {
            var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(value, plan);
            run.scope.add(BenchmarkReturnWorkBudget.Activity.COPY, new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0,
                    frozen.work().transientUtf8CopyUpperBytes(), 1, frozen.work().nodes(), 0));
            run.scope.issued(ticket);
            String encoded = io.tapstate.core.common.JsonWriter.write(frozen.value());
            long utf8Upper = Math.multiplyExact(4L, encoded.length());
            run.scope.complete(ticket, BenchmarkReturnWorkBudget.Work.bytes(utf8Upper), null);
            return encoded;
        } catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure; }
    }

    static String rootClockMetadata() {
        return BenchmarkReturnWorkBudget.request(BenchmarkReturnWorkBudget.current(),
                BenchmarkReturnWorkBudget.Activity.NATIVE_METADATA, io.tapstate.adapters.pdk.PdkBenchmarkClock::metadata);
    }

    static BenchmarkReturnWorkBudgetRun open(String workload) throws IOException {
        String requested = System.getProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY);
        if (requested == null) { return null; }
        RealBenchmarkForkDriver.workBudgetDiagnostics(requested,
                "true".equals(System.getProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY)));
        var files = new java.util.ArrayList<BenchmarkReturnWorkBudgetProfile.FileInput>();
        var paths = new EnumMap<BenchmarkReturnWorkBudgetProfile.FileRole, Path>(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        boolean cpu = "true".equals(System.getProperty(RealBenchmarkForkDriver.ROOT_CPU_DIAGNOSTICS_PROPERTY));
        for (var role : BenchmarkReturnWorkBudgetProfile.FileRole.values()) {
            if (role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY && !cpu) { continue; }
            String field = PREFIX + "file." + role.name() + ".";
            Path path = absolute(scalar(field + "path", 4096));
            long size;
            try { size = Long.parseLong(scalar(field + "bytes", 20)); }
            catch (NumberFormatException malformed) { throw new AssertionError("registered file byte scalar is malformed", malformed); }
            files.add(new BenchmarkReturnWorkBudgetProfile.FileInput(role, scalar(field + "sha256", 64), size));
            paths.put(role, path);
        }
        var conflicts = new java.util.LinkedHashSet<String>();
        for (String key : List.of("tapstate.e2e.benchmark.load-diagnostics", "tapstate.e2e.benchmark.compilation-diagnostics",
                "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark-smoke.jvm-gap-diagnostics",
                "tapstate.e2e.benchmark-smoke.paced-calibration", "tapstate.e2e.benchmark-smoke.cdc-settling-calibration",
                "tapstate.e2e.benchmark-smoke.full-cdc-settling-calibration", BenchmarkDualGcDiagnostics.ENABLED_PROPERTY,
                BenchmarkWitnessReadGate.PROPERTY, BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                RealBenchmarkForkDriver.WRITE_RETURN_METHOD_CONTROL_PROPERTY, RealBenchmarkForkDriver.WRITE_RETURN_COST_STAGES_PROPERTY)) {
            if (System.getProperty(key) != null) { conflicts.add(key); }
        }
        var flags = new BenchmarkReturnWorkBudgetProfile.Flags(System.getProperty("tapstate.e2e.benchmark-smoke.arm", "A"),
                System.getProperty("tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN"),
                RealBenchmarkForkDriver.writeReturnClockMode(System.getProperty(RealBenchmarkForkDriver.WRITE_RETURN_CLOCK_CONTROL_PROPERTY),
                        Boolean.getBoolean(RealBenchmarkForkDriver.WRITE_RETURN_DIAGNOSTICS_PROPERTY),
                        Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot")).name(),
                collectorMode(),
                Boolean.getBoolean("tapstate.e2e.benchmark-smoke.steady-pilot"),
                System.getProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY) != null,
                Boolean.getBoolean(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY),
                System.getProperty("tapstate.e2e.benchmark-smoke.fork-output") != null, cpu, conflicts);
        var inputs = new BenchmarkReturnWorkBudgetProfile.Inputs(scalar(PREFIX + "runId", 128),
                scalar(PREFIX + "inputSha256", 64), BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(workload), files, flags,
                producerWriterFromProperties());
        return open(BenchmarkReturnWorkBudgetProfile.inspect(inputs), paths,
                absolute(scalar("tapstate.e2e.benchmark-smoke.fork-output", 4096)),
                absolute(scalar(PREFIX + "producerBody", 4096)), scalar(PREFIX + "nonce", 64));
    }

    private static String collectorMode() {
        String calibration = System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY);
        if (calibration == null && "A".equals(System.getProperty("tapstate.e2e.benchmark-smoke.arm", "A"))
                && "true".equals(System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY))) { return "ON"; }
        return calibration;
    }

    private static ProducerWriterBinding producerWriterFromProperties() {
        String base = PREFIX + "producerWriter.";
        var keys = List.of("sourceRoot", "source.sha256", "source.bytes", "compiled.sha256", "compiled.bytes");
        boolean any = false; for (String key : keys) { any |= System.getProperty(base + key) != null; }
        if (!any) { return null; }
        try {
            return new ProducerWriterBinding(Owner.PRODUCER, scalar(base + "sourceRoot", 4096),
                    new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(),
                            new Blob(scalar(base + "source.sha256", 64), Long.parseLong(scalar(base + "source.bytes", 20)))),
                    new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(),
                            new Blob(scalar(base + "compiled.sha256", 64), Long.parseLong(scalar(base + "compiled.bytes", 20)))));
        } catch (NumberFormatException malformed) { throw new AssertionError("registered producer writer byte scalar is malformed", malformed); }
    }

    /**
     * Covers the bounded pre-scope property prelude, recipe append, repeated writer maps and
     * metadata projection of at most 32 fields; charged to every mandatory install entry.
     */
    static BenchmarkReturnWorkBudget.Work producerWriterRegistrationUpper() {
        long characters = 4L * 4096 + 2L * 512 + 2L * 256 + 4L * 64;
        return new BenchmarkReturnWorkBudget.Work(256, Math.addExact(256, Math.multiplyExact(16, characters)),
                2, 0, Math.multiplyExact(8, characters), 8, Math.multiplyExact(8, characters), 256);
    }

    static BenchmarkReturnWorkBudgetRun open(BenchmarkReturnWorkBudgetProfile.Prepared profile,
            Map<BenchmarkReturnWorkBudgetProfile.FileRole, Path> paths, Path output, Path body, String nonce) throws IOException {
        require(profile != null && paths != null && paths.keySet().equals(profile.inputs().files().stream()
                .map(BenchmarkReturnWorkBudgetProfile.FileInput::role).collect(java.util.stream.Collectors.toSet())),
                "registered file path roster differs");
        // Reuse the one existing pre-scope source validation; do not rebuild it in the factory.
        var validatedProfile = BenchmarkReturnWorkBudgetBindings.authenticate(profile);
        require(output != null && body != null && !output.equals(body) && !Path.of(body + ".seal").equals(output),
                "registered output and seal paths overlap");
        absolute(output.toString()); absolute(body.toString());
        require(nonce != null && !nonce.isEmpty() && nonce.length() <= 64
                && nonce.chars().allMatch(c -> c >= 32 && c <= 126), "registered nonce differs");
        require(ACTIVE.get() == null, "another source profile is active");
        var run = new BenchmarkReturnWorkBudgetRun(validatedProfile, paths, output, body, nonce);
        if (!ACTIVE.compareAndSet(null, run)) { run.scope.close(); throw new AssertionError("another source profile is active"); }
        try {
            run.bindFiles();
            return run;
        } catch (IOException | RuntimeException | Error failure) {
            run.scope.failed(failure);
            try { run.close(); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void bindFiles() throws IOException {
        if (profile.inputs().producerWriter() != null) {
            require("true".equals(BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ,
                    () -> System.getProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY)))
                    && BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ,
                    () -> System.getProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY)) == null,
                    "separate producer writer requires the native domain without collector calibration");
        }
        long start = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ,
                () -> ManagementFactory.getRuntimeMXBean().getStartTime());
        long pid = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.PROCESS_INFO,
                () -> ProcessHandle.current().pid());
        scope.bindRoot(pid, start);
        rootRuntime = new BenchmarkReturnWorkBudgetAdmission.Runtime(profile.inputs().runId(), pid, start);
        sourceRoot = absolute(scalar(PREFIX + "sourceRoot", 4096));
        Path actualSourceRoot = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, sourceRoot::toRealPath);
        require(sourceRoot.equals(actualSourceRoot) && BenchmarkReturnWorkBudget.external(scope,
                BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> Files.isDirectory(sourceRoot, LinkOption.NOFOLLOW_LINKS)),
                "actual source root is missing or noncanonical");
        for (var file : profile.inputs().files()) {
            String hash = fingerprint(paths.get(file.role()), file.bytes());
            require(hash.equals(file.sha256()), "actual registered file digest differs: " + file.role());
        }
        for (Path target : List.of(output, producerBody, Path.of(producerBody + ".seal"))) {
            Path parent = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> target.getParent().toRealPath());
            require(target.getParent().equals(parent), "output parent is not canonical");
            require(!BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> Files.exists(target, LinkOption.NOFOLLOW_LINKS)), "registered output already exists");
        }
        require(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).equals(
                absolute(scalar("tapstate.e2e.benchmark-smoke.jar", 4096))), "actual application path differs");
        require(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).equals(
                absolute(scalar(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY, 4096))), "actual native library path differs");
        authenticateManifests(sourceRoot, pid, start);
    }

    private void authenticateManifests(Path sourceRoot, long pid, long start) throws IOException {
        var install = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                BenchmarkReturnWorkBudget.Work.call());
        try {
            authenticateManifestDocuments(sourceRoot, pid, start);
            BenchmarkReturnWorkBudget.done(install);
        } catch (IOException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(install, failure);
            throw failure;
        }
    }

    private void authenticateManifestDocuments(Path sourceRoot, long pid, long start) throws IOException {
        var declared = new java.util.ArrayList<FileFact>();
        for (var file : profile.inputs().files()) declared.add(new FileFact(file.role(), paths.get(file.role()).toString(),
                new Blob(file.sha256(), file.bytes())));
        FileFact helperFile = declared.stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST).findFirst().orElseThrow();
        FileFact jdkFile = declared.stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST).findFirst().orElseThrow();
        helperManifest = BenchmarkReturnBindingsManifestReader.readHelper(Path.of(helperFile.path()), helperFile.blob());
        jdkManifest = BenchmarkReturnBindingsManifestReader.readJdk(Path.of(jdkFile.path()), jdkFile.blob());
        require(supportedHelper(helperManifest) && jdkManifest.schemaVersion() == SCHEMA_VERSION
                        && profile.inputs().runId().equals(helperManifest.runId()) && profile.inputs().runId().equals(jdkManifest.runId())
                        && profile.inputs().inputSha256().equals(helperManifest.inputSha256()) && profile.inputs().inputSha256().equals(jdkManifest.inputSha256()),
                "manifest run, input or schema differs");
        require(java.util.Objects.equals(profile.inputs().producerWriter(), helperManifest.producerWriter()),
                "producer writer declaration differs from independently registered scalars");
        require(definition.definitionId().equals(helperManifest.rootDefinitionId())
                        && definition.sourceFormulaSha256().equals(helperManifest.rootFormulaSha256())
                        && BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256.equals(helperManifest.producerFormulaSha256()),
                "manifest source definition differs");
        var actualClasspath = actualClasspath();
        require(helperManifest.failsafeClasspath().equals(actualClasspath), "current actual ordered classpath differs");
        rootClasspath = actualClasspath;
        String home = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> System.getProperty("java.home"));
        String version = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ, () -> System.getProperty("java.version"));
        require(home != null && version != null && (version.equals("21") || version.startsWith("21.")), "actual root JDK runtime is unavailable");
        Path actualHome = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, () -> absolute(home).toRealPath());
        require(actualHome.toString().equals(jdkManifest.javaHome()) && version.equals(jdkManifest.javaVersion()), "actual root JDK identity differs");
        var requiredJdk = java.util.EnumSet.allOf(JdkRole.class);
        if (!profile.inputs().flags().rootCpu()) requiredJdk.remove(JdkRole.CPU_PROVIDER);
        require(jdkManifest.files().stream().map(JdkFile::role).collect(java.util.stream.Collectors.toSet()).equals(requiredJdk), "JDK file role roster differs");
        authenticated = BenchmarkReturnWorkBudgetBindingsIo.authenticate(sourceRoot,
                paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR), declared, helperManifest, jdkManifest);
        require(java.util.Objects.equals(profile.inputs().producerWriter(), authenticated.producerWriter()),
                "actual producer writer source or compiled bytes were not authenticated");
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("state", "ACTUAL_DECLARED_BYTES_AND_ROOT_RUNTIME_RECORDED");
        evidence.put("runId", profile.inputs().runId()); evidence.put("inputSha256", profile.inputs().inputSha256());
        evidence.put("rootDefinitionId", definition.definitionId()); evidence.put("rootFormulaSha256", definition.sourceFormulaSha256());
        evidence.put("producerFormulaSha256", helperManifest.producerFormulaSha256()); evidence.put("actualIo", authenticated.evidence());
        evidence.put("actualRootRuntime", Map.of("pid", pid, "jvmStartTimeMillis", start,
                "javaHome", actualHome.toString(), "javaVersion", version));
        evidence.put("actualOrderedClasspath", actualClasspath); evidence.put("classpathSource", "CURRENT_SUREFIRE_TEST_CLASS_PATH_PROPERTY");
        evidence.put("classpathTokenEvidence", classpathEvidence);
        evidence.put("gaps", List.of("ACTUAL_LOADED_ROOT_CLASS_ORIGINS_NOT_OBSERVED", "EMITTED_MEMBER_COMPLETENESS_NOT_OBSERVED",
                "FRESH_FAILSAFE_XML_LIFECYCLE_NOT_INFERRED_FROM_DECLARED_DIGEST", "VM_TRANSFORMED_INSTRUCTION_BODY_NOT_CLAIMED"));
        evidence.put("runtimeOriginAuthenticationQualified", false); evidence.put("emittedMemberCompletenessQualified", false);
        evidence.put("runtimeInstructionBytesQualified", false); evidence.put("structuralWorkBudgetQualified", false);
        evidence.put("wholeMethodCostQualified", false); evidence.put("performanceAcceptanceEligible", false);
        if (authenticated.producerWriter() != null) {
            evidence.put("producerWriter", authenticated.producerWriter().evidence());
            evidence.put("producerWriterRoute", "A_PLAIN_PERIODIC_NATIVE_DOMAIN_COLLECTOR_ENABLED_CALIBRATION_ABSENT");
        }
        bindingEvidence = Map.copyOf(evidence);
    }

    private List<String> actualClasspath() throws IOException {
        String raw = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.RUNTIME_READ,
                () -> System.getProperty("surefire.test.class.path"));
        require(raw != null && !raw.isEmpty() && raw.length() <= 512L * 4097, "actual Failsafe classpath is missing or exceeds its bound");
        String[] entries = raw.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator), -1);
        require(entries.length <= Math.addExact(MAX_CLASSPATH, 1), "actual classpath token roster exceeds its bound");
        var paths = new java.util.ArrayList<String>(); var seen = new java.util.HashSet<String>();
        var absent = new java.util.ArrayList<String>(); int terminalEmpty = 0;
        for (int index = 0; index < entries.length; index++) {
            String entry = entries[index];
            if (entry.isEmpty()) {
                require(index == entries.length - 1 && index > 0, "actual classpath has an interior empty entry");
                terminalEmpty++; continue;
            }
            require(entry.length() <= 4096, "actual classpath has an oversized entry");
            require(paths.size() < MAX_CLASSPATH, "actual classpath origin roster exceeds its bound");
            Path path = absolute(entry);
            boolean exists = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ,
                    () -> Files.exists(path, LinkOption.NOFOLLOW_LINKS));
            if (exists) {
                Path real = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ, path::toRealPath);
                require(path.equals(real), "actual classpath has a linked origin");
            } else absent.add(entry);
            require(seen.add(entry), "actual classpath has a repeated origin"); paths.add(entry);
        }
        require(!paths.isEmpty(), "actual classpath has no declared nonempty origin");
        String hash;
        try {
            byte[] rawBytes = raw.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            scope.add(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, BenchmarkReturnWorkBudget.Work.bytes(rawBytes.length));
            hash = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(rawBytes)));
        } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        classpathEvidence = Map.of("rawPropertySha256", hash, "rawTokenCount", entries.length,
                "terminalEmptyTokens", terminalEmpty, "terminalEmptyTokenInterpretation", "RETAINED_NOT_RESOLVED_TO_CWD_RUNTIME_SEMANTICS_UNQUALIFIED",
                "absentDeclaredOrigins", List.copyOf(absent), "runtimeClasspathCompletenessQualified", false);
        return List.copyOf(paths);
    }

    Map<String, Object> bindingEvidence() { return bindingEvidence; }

    void observeRootOrigins() throws IOException, ClassNotFoundException {
        requireActive(); require(authenticated != null && observedRootOrigins.isEmpty(), "root origin observation is missing or repeated");
        var entry = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, BenchmarkReturnWorkBudget.Work.call());
        try {
            require(emittedRoster == null, "emitted compiler roster observation is repeated");
            emittedRoster = BenchmarkReturnEmittedRoster.observe(sourceRoot, helperManifest);
            var expected = new BenchmarkReturnWorkBudgetBindings.Runtime(Owner.ROOT, profile.inputs().runId(), rootRuntime.pid(), rootRuntime.jvmStartTimeMillis(),
                    jdkManifest.javaHome(), jdkManifest.javaVersion());
            var observed = BenchmarkReturnRootOrigins.observeRoot(helperManifest, rootClasspath, expected, scope);
            long declaredRoots = helperManifest.origins().stream().filter(origin -> origin.owner() == Owner.ROOT).count();
            require(declaredRoots > 0 && observed.observedOrigins().size() == declaredRoots,
                    "actual root origin roster differs from authenticated declarations");
            observedRootOrigins = List.copyOf(observed.observedOrigins()); rootOriginEvidence = observed.evidence();
            var evidence = new LinkedHashMap<String, Object>(bindingEvidence);
            evidence.put("actualRootOrigins", rootOriginEvidence);
            evidence.put("actualEmittedRoster", emittedRoster.evidence());
            evidence.put("gaps", List.of("PRODUCER_LOADED_ORIGINS_BEYOND_CLOCK_NOT_OBSERVED",
                    "FRESH_FAILSAFE_XML_LIFECYCLE_NOT_INFERRED_FROM_DECLARED_DIGEST", "VM_TRANSFORMED_INSTRUCTION_BODY_NOT_CLAIMED"));
            bindingEvidence = Map.copyOf(evidence);
            BenchmarkReturnWorkBudget.done(entry);
        } catch (IOException | ClassNotFoundException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(entry, failure); throw failure;
        }
    }

    /** Consumes already obtained cold metadata; it never queries the owned process or reads a clock. */
    void recordNativeClassMetadata(Map<String, Object> nativeEvidence) {
        requireActive();
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> {
            require(nativeClassPacketRoster.expectedPackets() == 2,
                    "multiple measured phases require explicit cold phase and cut");
            recordNativeClassMetadata(nativeEvidence, nativeClassPacketRoster.phaseKeys().getFirst(),
                    producerClassMetadata.isEmpty() ? NativeMetadataCut.BEFORE : NativeMetadataCut.AFTER);
            return null;
        });
    }

    void recordNativeClassMetadata(Map<String, Object> nativeEvidence, String phaseKey, NativeMetadataCut cut) {
        requireActive();
        var install = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                BenchmarkReturnWorkBudget.Work.call());
        try {
            consumeNativeClassMetadata(nativeEvidence, phaseKey, cut);
            BenchmarkReturnWorkBudget.done(install);
        } catch (RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(install, failure);
            throw failure;
        }
    }

    private void consumeNativeClassMetadata(Map<String, Object> nativeEvidence, String phaseKey, NativeMetadataCut cut) {
        require(authenticated != null && producerRegistration != null && nativeEvidence != null
                        && "NATIVE_RECORDED".equals(nativeEvidence.get("state")), "actual cold native class evidence is missing");
        require(producerClassMetadata.size() == nativeClassPacketRoster.recordedPackets(), "cold packet publication count differs");
        nativeClassPacketRoster.requireNext(phaseKey, cut, producerRegistration.runtime());
        Map<?, ?> metadata = map(nativeEvidence.get("ownedMetadata"));
        require(number(metadata, "pid") == producerRegistration.runtime().pid()
                        && number(metadata, "jvmStartTimeMillis") == producerRegistration.runtime().jvmStartTimeMillis(), "actual cold class runtime differs");
        var expected = new LinkedHashMap<String, Object>();
        for (String suffix : List.of("", "$Clock", "$JniAccess", "$NativeAccess")) {
            String binary = "io.tapstate.adapters.pdk.PdkBenchmarkClock" + suffix;
            var origin = authenticated.origins().stream().filter(item -> item.owner() == Owner.PRODUCER && item.binaryName().equals(binary)).findFirst().orElseThrow();
            expected.put("PdkBenchmarkClock" + suffix, origin.classBytes().sha256());
        }
        for (String boundary : List.of("before", "after")) {
            Map<?, ?> snapshot = map(metadata.get(boundary));
            require(expected.equals(map(snapshot.get("classHashes"))), "actual cold class hashes differ from authenticated application entries");
        }
        var origins = consumeProducerOrigins(nativeEvidence.get("producerOrigins"), nativeEvidence.get("raw"));
        var packet = Map.<String, Object>of("pid", producerRegistration.runtime().pid(),
                "jvmStartTimeMillis", producerRegistration.runtime().jvmStartTimeMillis(), "classHashes", Map.copyOf(expected),
                "producerOrigins", origins.evidence(), "phaseKey", phaseKey, "cut", cut.name());
        var nextPackets = new java.util.ArrayList<>(producerClassMetadata);
        nextPackets.add(packet);
        var bindings = new LinkedHashMap<String, Object>(bindingEvidence);
        bindings.put("actualProducerOrigins", List.copyOf(nextPackets));
        require(bindings.get("gaps") instanceof List<?> existing && existing.size() <= 8, "binding gap roster differs");
        bindings.put("gaps", ((List<?>) bindings.get("gaps")).stream()
                .filter(gap -> !"PRODUCER_LOADED_ORIGINS_BEYOND_CLOCK_NOT_OBSERVED".equals(gap)).toList());
        var published = Map.copyOf(bindings);
        producerClassMetadata.add(packet);
        nativeClassPacketRoster.record(phaseKey, cut, producerRegistration.runtime());
        bindingEvidence = published;
    }

    private BenchmarkReturnProducerOrigins.Parsed consumeProducerOrigins(Object value, Object nativeRaw) {
        Map<?, ?> evidence = map(value);
        require(evidence.get("rawParts") instanceof List<?> parts && parts.size() == 34,
                "actual producer origin raw roster is missing");
        var raw = new String[34];
        for (int i = 0; i < raw.length; i++) {
            Object part = ((List<?>) evidence.get("rawParts")).get(i);
            require(part instanceof String, "actual producer origin raw part is not text"); raw[i] = (String) part;
        }
        var parsed = BenchmarkReturnProducerOrigins.parse(raw, producerRegistration.runtime(),
                paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).toString());
        require(parsed.nativeRaw().equals(nativeRaw), "producer origin native part differs from the original cold read");
        require("RECORDED_UNQUALIFIED".equals(parsed.header().state()), "actual producer origin packet is incomplete");
        require(jdkManifest.javaHome().equals(parsed.header().top().javaHome())
                && jdkManifest.javaVersion().equals(parsed.header().top().javaVersion()), "actual producer origin JDK differs");
        var expected = new java.util.HashMap<String, Origin>();
        require(authenticated.origins().size() <= 2 * MAX_CLASSES, "authenticated origin roster exceeds its prior bound");
        for (var origin : authenticated.origins()) if (origin.owner() == Owner.PRODUCER) {
            require(expected.put(origin.binaryName(), origin) == null, "authenticated producer origin is duplicated");
        }
        require(expected.size() == BenchmarkReturnProducerOrigins.ORIGINS, "authenticated producer origin roster differs");
        for (var part : parsed.typedParts()) {
            var origin = expected.remove(part.binaryName());
            require(origin != null && part.kind() == BenchmarkReturnProducerOrigins.Kind.BOOT_NESTED
                    && origin.container().equals(part.outer())
                    && origin.entry().equals(part.module() + "!/" + part.entry())
                    && origin.classBytes().sha256().equals(part.sha256())
                    && origin.classBytes().bytes() == part.bytes(),
                    "actual producer origin differs from authenticated application entries");
        }
        require(expected.isEmpty(), "authenticated producer origin was not observed");
        return parsed;
    }

    /** Bounded maps and equality work around the separately ticketed packet decoder. */
    static BenchmarkReturnWorkBudget.Work producerOriginBindingUpper() {
        long origins = 2L * MAX_CLASSES, parts = 34;
        long text = Math.addExact(Math.multiplyExact(origins, 256), Math.multiplyExact(parts, 4096 + 2L * 1024 + 512 + 128 + 256 + 64));
        var original = new BenchmarkReturnWorkBudget.Work(Math.addExact(128, Math.multiplyExact(16, origins + parts)),
                Math.addExact(128, Math.multiplyExact(32, text)), parts, 0,
                Math.addExact(Math.multiplyExact(64, origins + parts), Math.multiplyExact(8, parts)), 8,
                Math.addExact(origins, Math.multiplyExact(8, parts)), Math.addExact(origins, parts));
        return original.plus(nativeClassPacketBindingUpper());
    }

    /** Added local phase/cut checks and bounded twelve-reference publication, per entered packet.
     * Includes the existing caller label's id/phase accessors and at most128-character concatenation;
     * the four old call sites use this label without another clock, metadata getter or JMX request.
     */
    static BenchmarkReturnWorkBudget.Work nativeClassPacketBindingUpper() {
        long packets = NativeClassPacketRoster.MAX_PACKETS, phases = NativeClassPacketRoster.MAX_PHASES;
        long characters = Math.multiplyExact(4, NativeClassPacketRoster.KEY_CHARS);
        long slots = Math.addExact(Math.addExact(Math.multiplyExact(7, packets), phases), characters);
        return new BenchmarkReturnWorkBudget.Work(1, Math.addExact(128, Math.multiplyExact(32, slots)), 0, packets,
                Math.addExact(Math.multiplyExact(128, packets + phases), Math.multiplyExact(4, characters)), 4,
                Math.multiplyExact(8, slots), Math.multiplyExact(4, slots));
    }

    /** One pre-scope constructor walks the already cached original phase roster; no factory is repeated. */
    static BenchmarkReturnWorkBudget.Work nativeClassPacketRosterFactoryUpper() {
        long phases = NativeClassPacketRoster.MAX_PHASES;
        long characters = Math.multiplyExact(phases, NativeClassPacketRoster.KEY_CHARS);
        long slots = Math.addExact(Math.multiplyExact(16, phases), characters);
        return new BenchmarkReturnWorkBudget.Work(1, Math.addExact(128, Math.multiplyExact(16, slots)), 0,
                NativeClassPacketRoster.MAX_PACKETS, Math.addExact(1024, Math.multiplyExact(8, characters)), 4,
                Math.multiplyExact(8, slots), Math.multiplyExact(4, slots));
    }

    /** Complete roster check and two scalar report annotations, apart from report serialization. */
    static BenchmarkReturnWorkBudget.Work nativeClassPacketAfterExitUpper() {
        long slots = Math.addExact(16, NativeClassPacketRoster.MAX_PACKETS);
        return new BenchmarkReturnWorkBudget.Work(1, Math.addExact(64, Math.multiplyExact(16, slots)), 0, 0,
                Math.addExact(256, Math.multiplyExact(32, slots)), 4,
                Math.multiplyExact(8, slots), Math.multiplyExact(4, slots));
    }

    enum NativeMetadataCut { BEFORE, AFTER }

    /** State is private to an original Run; a standalone projection grants no runtime authority. */
    static final class NativeClassPacketRoster {
        static final int MAX_PHASES = 6, MAX_PACKETS = 12, KEY_CHARS = 128;
        private final List<String> phaseKeys;
        private final int expectedPackets;
        private int recordedPackets;
        private BenchmarkCausalClock.Identity owner;

        NativeClassPacketRoster(BenchmarkWorkloadDefinitions.Workload original) {
            require(original != null && original.phases().size() <= MAX_PHASES, "original phase roster exceeds its prior cap");
            var keys = new java.util.ArrayList<String>(); var unique = new java.util.HashSet<String>();
            for (var phase : original.phases()) if (phase.measured()) {
                require(original.id() != null && phase.id() != null && !original.id().isEmpty() && !phase.id().isEmpty()
                                && Math.addExact(Math.addExact(original.id().length(), 1), phase.id().length()) <= KEY_CHARS,
                        "original measured phase key exceeds its prior cap");
                String key = original.id() + "/" + phase.id();
                require(key.chars().allMatch(c -> c >= 32 && c <= 126) && unique.add(key),
                        "original measured phase key is duplicated or malformed");
                keys.add(key);
            }
            require(!keys.isEmpty() && keys.size() <= MAX_PHASES, "original measured phase roster is missing");
            phaseKeys = List.copyOf(keys); expectedPackets = Math.multiplyExact(2, phaseKeys.size());
            require(expectedPackets <= MAX_PACKETS, "original cold packet roster exceeds its prior cap");
        }
        List<String> phaseKeys() { return phaseKeys; }
        int expectedPackets() { return expectedPackets; }
        int recordedPackets() { return recordedPackets; }
        void requireNext(String phaseKey, NativeMetadataCut cut, BenchmarkCausalClock.Identity actualOwner) {
            require(recordedPackets < expectedPackets, "cold native class receipt roster exceeds before/after bound");
            require(phaseKey != null && phaseKey.length() <= KEY_CHARS && cut != null && actualOwner != null,
                    "cold packet phase, cut or owner is missing");
            require(phaseKeys.get(recordedPackets / 2).equals(phaseKey)
                            && cut == (recordedPackets % 2 == 0 ? NativeMetadataCut.BEFORE : NativeMetadataCut.AFTER),
                    "cold packet phase or cut differs from the original measured roster");
            require(owner == null || owner.equals(actualOwner), "cold packet owner changed between measured phases");
        }
        void record(String phaseKey, NativeMetadataCut cut, BenchmarkCausalClock.Identity actualOwner) {
            requireNext(phaseKey, cut, actualOwner);
            if (owner == null) owner = actualOwner;
            recordedPackets = Math.incrementExact(recordedPackets);
        }
        void requireComplete() {
            require(owner != null && recordedPackets == expectedPackets,
                    "actual original measured before/after cold packet roster is incomplete");
        }
    }

    /** The ASCII transport guard consumes the fixed packet once before copying or decoding. */
    static BenchmarkReturnWorkBudget.Work coldPacketTransportUpper() {
        long bytes = 2080768;
        return new BenchmarkReturnWorkBudget.Work(1, Math.addExact(128 + 16L * 34, Math.multiplyExact(3, bytes)),
                0, 0, bytes, 1, bytes + 35, 0);
    }
    private static Map<?, ?> map(Object value) { require(value instanceof Map<?, ?>, "actual class metadata object is missing"); return (Map<?, ?>) value; }
    private static long number(Map<?, ?> value, String field) {
        Object raw = value.get(field); require(raw instanceof Long || raw instanceof Integer, "actual class runtime integer is missing");
        return ((Number) raw).longValue();
    }

    private String fingerprint(Path path, long priorBytes) throws IOException {
        require(path.equals(absolute(path.toString())), "registered input path is not normalized");
        require(BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                () -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)), "registered input is not a regular owned file");
        MessageDigest digest;
        try { digest = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                () -> MessageDigest.getInstance("SHA-256")); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        var input = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                () -> FileChannel.open(path, StandardOpenOption.READ));
        Throwable primary = null;
        try {
            var buffer = ByteBuffer.allocate(8192);
            long count = 0;
            while (true) {
                buffer.clear(); buffer.limit((int) Math.min(buffer.capacity(), Math.addExact(priorBytes - count, 1)));
                int read = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        () -> input.read(buffer));
                if (read < 0) { break; }
                require(read > 0, "registered file read made no progress");
                count = Math.addExact(count, read); require(count <= priorBytes, "registered input exceeds its prior byte scalar");
                buffer.flip();
                BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        () -> { digest.update(buffer); return null; });
                scope.add(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                        new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, read, 1, read, 0));
            }
            require(count == priorBytes, "registered input byte length differs");
            byte[] hash = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, digest::digest);
            return HexFormat.of().formatHex(hash);
        } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
        finally {
            try { BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> { input.close(); return null; }); }
            catch (IOException | RuntimeException | Error failure) {
                if (primary == null) { throw failure; }
                if (failure != primary) { primary.addSuppressed(failure); }
            }
        }
    }

    BenchmarkReturnWorkBudgetDefinitions.ProducerInput producerInput() { return producerInput; }

    void requireActive() {
        require(ACTIVE.get() == this && BenchmarkReturnWorkBudget.current() == scope && !finished,
                "trusted source profile is not active");
        if (profile.inputs().producerWriter() != null) {
            require(authenticated != null && profile.inputs().producerWriter().equals(authenticated.producerWriter()),
                    "separate producer writer is not authenticated");
        }
    }

    void requireDriver(BenchmarkWorkloadDefinitions.Workload workload, PipelineBenchmarkComparison.Arm arm,
                       int fork, Path application) {
        requireActive();
        require(workload != null && arm != null && application != null
                        && workload.equals(canonicalWorkload)
                        && arm.name().equals(profile.inputs().flags().arm())
                        && application.equals(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR))
                        && fork == Integer.parseInt(System.getProperty("tapstate.e2e.benchmark-smoke.fork", "1")),
                "actual driver workload, arm, fork or application differs from its registered source profile");
    }

    void registerProducer(BenchmarkWriteReturnReader reader, Map<String, Object> registration) {
        requireActive();
        require(registration != null && "RECORDED_ENABLED".equals(registration.get("state"))
                && registration.get("pid") instanceof Long && registration.get("jvmStartTimeMillis") instanceof Long,
                "existing owned producer registration is incomplete");
        var identity = new BenchmarkCausalClock.Identity((Long) registration.get("pid"), (Long) registration.get("jvmStartTimeMillis"));
        if (producerRegistration != null) {
            require(producerRegistration.runtime().equals(identity), "producer identity changed between phases");
            return;
        }
        String appHash = profile.inputs().files().stream().filter(file -> file.role() == BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR)
                .findFirst().orElseThrow().sha256();
        producerRegistration = new BenchmarkReturnWorkBudgetProducerSeal.Registration(profile.inputs().runId(), identity,
                profile.inputs().inputSha256(), nonce, appHash);
        require(reader.armWorkBudgetFinalReceipt(producerBody, nonce), "owned producer refused its once-only final receipt");
    }

    void afterExit(BenchmarkOwnedProcessReceipt actual) throws IOException {
        requireActive(); require(producerRegistration != null, "producer final receipt was never armed");
        require(!observedRootOrigins.isEmpty(), "actual root class origins were not observed before services");
        require(emittedRoster != null, "actual complete emitted compiler roster was not observed before services");
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> {
            nativeClassPacketRoster.requireComplete();
            require(producerClassMetadata.size() == nativeClassPacketRoster.expectedPackets(),
                    "actual before/after cold class metadata was not bound");
            return null;
        });
        var bound = BenchmarkReturnWorkBudgetProducerSeal.verifyAfterExit(producerBody, producerRegistration, actual);
        var arithmetic = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(bound.body());
        reconcileCaptures(bound.body(), captures);
        var facts = new LinkedHashMap<String, Object>();
        facts.put("state", "POST_EXIT_BYTES_AND_SOURCE_ARITHMETIC_BOUND"); facts.put("body", bound.body());
        facts.put("bodySha256", bound.bodySha256()); facts.put("companionSha256", bound.companionSha256());
        facts.put("allocation", arithmetic); facts.put("captureCountPairing", "MATCHED_ORIGINAL_FULL_WIRE_RECEIPTS");
        facts.put("actualInputBindings", bindingEvidence);
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> {
            facts.put("producerClassMetadata", List.copyOf(producerClassMetadata));
            facts.put("producerClassMetadataExpectedPhaseKeys", nativeClassPacketRoster.phaseKeys());
            facts.put("producerClassMetadataExpectedPackets", nativeClassPacketRoster.expectedPackets());
            return null;
        });
        facts.put("actualRootOrigins", rootOriginEvidence);
        facts.put("runtimeCoverageQualified", false);
        facts.put("structuralWorkBudgetQualified", false); facts.put("wholeMethodCostQualified", false);
        producerEvidence = Map.copyOf(facts);
    }

    void captured(BenchmarkWriteReturnCapture.Result result, Map<String, Object> retained) {
        requireActive(); require(result != null && result.summary() != null && retained != null
                && retained.get("epoch") instanceof Long && captures.size() < 16, "original capture evidence is missing or over its cap");
        long rows = 0;
        for (var call : result.calls()) { rows = Math.addExact(rows, call.totalRows()); }
        require(rows == result.summary().reportedRecords() && result.calls().size() == result.summary().completedCalls(),
                "original wire capture rows or calls differ");
        captures.add(new Capture((Long) retained.get("epoch"), result.summary().window(), result.calls().size(), rows));
        scope.add(BenchmarkReturnWorkBudget.Activity.ASSOCIATION,
                new BenchmarkReturnWorkBudget.Work(1, 0, result.calls().size(), 0, 0, 1, result.calls().size(), 0));
    }

    static void reconcileCaptures(Map<String, Object> body, List<Capture> captures) {
        require(body != null && captures != null && !captures.isEmpty() && captures.size() <= 16
                && body.get("epochs") instanceof List<?> epochs && epochs.size() == captures.size(), "producer epoch roster differs");
        long rows = 0, calls = 0;
        for (int i = 0; i < captures.size(); i++) {
            var capture = captures.get(i);
            require(capture.epoch() > 0 && capture.calls() > 0 && capture.rows() > 0 && capture.window() != null
                    && capture.window().length() <= 512, "original capture scalar is outside its source bound");
            require(((List<?>) body.get("epochs")).get(i) instanceof Map<?, ?> epoch
                    && number(epoch.get("epoch")) == capture.epoch()
                    && java.util.Base64.getEncoder().encodeToString(capture.window().getBytes(java.nio.charset.StandardCharsets.UTF_8))
                            .equals(epoch.get("windowBase64")), "producer epoch or window differs from the original wire capture");
            rows = Math.addExact(rows, capture.rows()); calls = Math.addExact(calls, capture.calls());
        }
        require(number(body.get("inputRowsAttempted")) == rows && body.get("activities") instanceof Map<?, ?>,
                "producer active input rows differ from the complete wire receipts");
        Map<?, ?> actors = (Map<?, ?>) body.get("activities");
        require(actorInvocations(actors, "IDENTITY_ROW") == rows && actorInvocations(actors, "ACTIVE_BEGIN") == calls
                && actorInvocations(actors, "RETURN_OBSERVATION") == calls && actorInvocations(actors, "COMPLETE") == calls,
                "producer active rows or return calls differ from the complete wire receipts");
    }

    private static long actorInvocations(Map<?, ?> actors, String actor) {
        require(actors.get(actor) instanceof Map<?, ?>, "producer activity pairing is missing");
        return number(((Map<?, ?>) actors.get(actor)).get("invocations"));
    }
    private static long number(Object value) {
        require(value instanceof Long || value instanceof Integer, "producer pairing scalar is malformed");
        long number = ((Number) value).longValue(); require(number >= 0, "producer pairing scalar is negative"); return number;
    }

    void writeReport(Path actualPath, Map<String, Object> report) throws Exception {
        requireActive(); require(output.equals(actualPath), "actual report path differs");
        var tree = new LinkedHashMap<String, Object>(report);
        tree.put("returnWorkBudgetProfile", profile.inputRegistrationInfo()); tree.put("returnWorkBudgetProducer", producerEvidence);
        tree.put("returnWorkBudgetPrefix", scope.snapshot());
        var plan = profile.fieldsRegisteredOutputPlan();
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(tree, plan);
        scope.add(BenchmarkReturnWorkBudget.Activity.COPY, new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0,
                frozen.work().transientUtf8CopyUpperBytes(), 1, frozen.work().nodes(), 0));
        var prepared = scope.prepareOutput("FULL_FORK_REPORT", new BenchmarkReturnWorkBudget.TreePlan(plan.nodeUpper(),
                plan.stringUtf8Upper(), plan.encodedUpperBytes(), plan.depthUpper()), frozen.value(), plan.sha256());
        scope.writeOutput(prepared, bytes -> Files.write(output, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        BenchmarkReturnWorkBudgetAdmission.verifyOutputFile(output, prepared, plan, scope.snapshot());
    }

    Map<String, Object> finish() {
        requireActive();
        Map<String, Object> receipt = scope.finish();
        BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, rootRuntime, receipt);
        finished = true;
        return receipt;
    }

    void failed(Throwable failure) { scope.failed(failure); }

    @Override public void close() {
        scope.close(); ACTIVE.compareAndSet(this, null);
    }

    private static String scalar(String key, int limit) {
        String value = System.getProperty(key);
        require(value != null && !value.isBlank() && value.length() <= limit
                && value.chars().allMatch(c -> c >= 32 && c <= 126), "registered scalar is missing or exceeds its source cap: " + key);
        return value;
    }
    private static Path absolute(String raw) {
        Path path = Path.of(raw);
        require(path.isAbsolute() && path.getFileName() != null && path.equals(path.normalize()), "registered path is not absolute and normalized");
        return path;
    }
    private static void require(boolean value, String reason) { if (!value) { throw new AssertionError("return work run " + reason); } }
}
