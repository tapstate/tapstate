package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Base64;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

import static io.tapstate.e2e.BenchmarkReturnExternalAcquirer.*;

/** Completed original method facts are authority only after independent bounded acquisition. */
final class BenchmarkReturnCompletedMethod {
    private BenchmarkReturnCompletedMethod() { }

    static CompletedFork verify(BenchmarkReturnExternalAcquirer.AcquiredCompletion acquired) {
        require(BenchmarkReturnWorkBudget.current() == null && BenchmarkReturnWorkBudgetRun.current() == null,
                "completed method consumer must remain outside the original Run");
        require(acquired != null, "actual acquired completion is missing");
        var registration = acquired.registration();
        var after = acquired.after();
        var closed = acquired.evidence();
        require("COLLECTION_METHOD_AFTER_FACTS_MATCHED_DIAGNOSTIC_ONLY".equals(after.evidence().get("state"))
                && "FINAL_RENDER_PRINTLN_METHOD_RETURN_AND_TRY_WITH_RESOURCES_RELEASE".equals(after.evidence().get("completionScope")),
                "normal full method completion is missing");
        require(after.sourceUpper().equals(vector(after.evidence().get("sourceUpper"))), "independent source sum differs");
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) require(Boolean.FALSE.equals(after.evidence().get(flag)),
                "diagnostic receipt qualification changed");
        var workload = BenchmarkWorkloadDefinitions.steadyPilot(registration.workload().id());
        var arm = PipelineBenchmarkComparison.Arm.valueOf(text(closed.get("arm"), 1));
        long ordinal = number(closed, "ordinal");
        require(ordinal >= 1 && ordinal <= 5, "original fork ordinal differs");
        String id = workload.id() + "-" + arm + "-" + ordinal;
        var measurement = object(acquired.report().get("measurement"));
        var interval = object(measurement.get("returnMethodFork"));
        require(id.equals(measurement.get("id")) && id.equals(interval.get("id"))
                && workload.id().equals(measurement.get("workload")) && workload.id().equals(interval.get("workload"))
                && arm.name().equals(measurement.get("arm")) && arm.name().equals(interval.get("arm"))
                && ordinal == number(interval, "ordinal")
                && BenchmarkReturnMethodAdmission.METHOD.equals(interval.get("method")), "original typed fork identity differs");
        var method = object(interval.get("methodAdmission"));
        require(BenchmarkReturnMethodAdmission.METHOD.equals(method.get("method"))
                && BenchmarkReturnMethodAdmission.ENDPOINT.equals(method.get("endpoint"))
                && Boolean.FALSE.equals(method.get("controlOnly"))
                && registration.root().pid() == number(method, "rootPid")
                && registration.root().jvmStartTimeMillis() == number(method, "rootJvmStartTimeMillis"),
                "original acquired method authority or root differs");
        var recipe = object(method.get("costRecipe"));
        require(registration.definition().definitionId().equals(recipe.get("definitionId"))
                && registration.definition().sourceFormulaSha256().equals(recipe.get("sourceFormulaSha256"))
                && strings(recipe.get("supportedWorkloads"), 3, 32).equals(List.of(workload.id())),
                "original cached source recipe differs");
        var expectedPhases = workload.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured)
                .map(BenchmarkWorkloadDefinitions.Phase::id).toList();
        require(strings(interval.get("phases"), 6, 128).equals(expectedPhases), "full measured phase population differs");
        var admitted = BenchmarkReturnMethodAdmission.admitCompleted(acquired);
        admitted.requireProductionAuthority();
        require(io.tapstate.core.common.JsonReader.parse(JsonWriter.write(admitted.evidence())).equals(method),
                "retained method admission differs from independently acquired native/source authority");
        var checked = verifyPhases(measurement, workload, admitted,
                new BenchmarkCausalClock.Identity(registration.root().pid(), registration.root().jvmStartTimeMillis()),
                registration.producer().runtime());
        var recomputed = checked.metrics();
        var count = object(interval.get("completedRows"));
        var duration = object(interval.get("durationNanos"));
        long countLower = number(count, "lower"), countUpper = number(count, "upper");
        long durationLower = number(duration, "lower"), durationUpper = number(duration, "upper");
        long cohort = number(interval, "fullCohortRows");
        long expectedCohort = 0;
        for (var phase : workload.phases()) if (phase.measured()) {
            for (var plan : BenchmarkExpectedChanges.forPhase(workload, phase)) {
                for (var batch : plan.batches()) for (var change : batch)
                    if (workload.inFixedCohort(change.key())) expectedCohort = Math.incrementExact(expectedCohort);
            }
        }
        require(cohort == expectedCohort && cohort <= 262144 && countLower >= 10000 && countUpper >= countLower
                && countUpper <= cohort && durationLower > 0 && durationUpper >= durationLower,
                "complete original interval population or common duration differs");
        var throughput = object(interval.get("throughput"));
        var p99 = object(interval.get("p99Nanos"));
        long p99Lower = signedLong(p99.get("lower")), p99Upper = signedLong(p99.get("upper"));
        require(p99Upper >= p99Lower, "retained p99 enclosure is reversed");
        // Preserve an uncertain/nonpositive p99 enclosure. Relative comparison separately refuses it.
        var rate = new BenchmarkReturnIntervalComparison.Bounds(decimal(throughput.get("lower")), decimal(throughput.get("upper")));
        requireMatchingInterval(interval, recomputed);
        var actualResources = object(measurement.get("resources"));
        long peakHeap = number(interval, "peakHeapBytes"), peakRss = number(interval, "peakRssBytes");
        require(peakHeap > 0 && peakRss > 0 && peakHeap == number(actualResources, "peakHeapBytes")
                && peakRss == number(actualResources, "peakRssBytes"), "retained memory observations differ");
        var correctness = correctness(measurement, workload, id);
        BenchmarkAckOracle.verify(List.of(correctness), PipelineBenchmarkHarness.expectedTerminals(workload));
        var proof = new LinkedHashMap<String, Object>();
        proof.put("state", "ORIGINAL_METHOD_COMPLETED_AND_INDEPENDENT_BYTES_MATCHED");
        proof.put("runId", registration.runId()); proof.put("inputSha256", registration.inputSha256());
        proof.put("rootRuntime", after.evidence().get("rootRuntime"));
        proof.put("producerRuntime", Map.of("pid", registration.producer().runtime().pid(),
                "jvmStartTimeMillis", registration.producer().runtime().jvmStartTimeMillis()));
        proof.put("definitionId", registration.definition().definitionId());
        proof.put("sourceFormulaSha256", registration.definition().sourceFormulaSha256());
        proof.put("producerFormulaSha256", BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256);
        proof.put("outputPlanSha256", registration.reportPlan().sha256());
        proof.put("registeredInputsSha256", acquired.registrationBytes().sha256());
        for (String field : List.of("reportSha256", "reportBytes", "finalStdoutSha256", "failsafeXmlSha256",
                "sourceUpper", "partitions", "completionScope", "rootOsTermination", "unresolvedCoverage"))
            proof.put(field, after.evidence().get(field));
        proof.put("sourceUpperIsActualWork", false);
        proof.put("intervalCorrespondence", "INDEPENDENT_RAW_CAPTURE_SOURCE_NATIVE_HELLO_COMMON_WINDOW_AND_FULL_COHORT_POOL_RECOMPUTED");
        proof.put("acceptanceScope", "STRUCTURAL_ENTERED_SOURCE_WORK_AND_ORIGINAL_NOMINAL_TIME_INTERVALS");
        proof.put("offeredLoad", checked.offeredLoad());
        proof.put("appCpuNanosPerCohortRow", Map.of("lower", BenchmarkReturnForkBridge.decimal(checked.cpuPerRow().lower()),
                "upper", BenchmarkReturnForkBridge.decimal(checked.cpuPerRow().upper()),
                "scope", "APP_PROCESS_CPU_OVER_EACH_PHASE_COMMON_RETURN_WINDOW_DIVIDED_BY_COHORT_ROWS"));
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> proof.put(flag, false));
        return new CompletedFork(id, workload.id(), arm, Math.toIntExact(ordinal), correctness, rate,
                new BenchmarkCausalClock.Interval(p99Lower, p99Upper), checked.cpuPerRow(), Math.toIntExact(cohort), peakHeap, peakRss,
                countLower, countUpper, durationLower, durationUpper, expectedPhases, Map.copyOf(proof), acquired.application());
    }

    private record CheckedPhases(BenchmarkReturnIntervalMetrics.Fork metrics, BenchmarkReturnIntervalComparison.Bounds cpuPerRow,
                                 List<Map<String, Object>> offeredLoad) { }

    private static CheckedPhases verifyPhases(Map<String, Object> measurement,
            BenchmarkWorkloadDefinitions.Workload workload, BenchmarkReturnMethodAdmission.AdmittedMethod admitted,
            BenchmarkCausalClock.Identity root, BenchmarkCausalClock.Identity producer) {
        var expected = workload.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured).toList();
        var windows = list(measurement.get("measuredPhaseWindows"), 6);
        require(windows.size() == expected.size(), "complete acquired measured phase windows differ");
        var phases = new ArrayList<BenchmarkReturnIntervalMetrics.Phase>();
        var cpu = new ArrayList<BenchmarkReturnCpuPerRow.Phase>();
        var offered = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < expected.size(); i++) {
            var phase = expected.get(i); var window = object(windows.get(i));
            require(phase.id().equals(window.get("id")), "acquired measured phase order differs");
            var clocks = object(window.get("targetClockQualification"));
            var retained = object(clocks.get("returnMethodPhase"));
            var batches = new ArrayList<BenchmarkForkEnvironment.BatchResult>();
            for (Object raw : list(retained.get("sourceBatches"), 960)) {
                var row = object(raw);
                batches.add(new BenchmarkForkEnvironment.BatchResult(Math.toIntExact(number(row, "index")),
                        number(row, "issuedAtNanos"), number(row, "completedAtNanos")));
            }
            var windowBatches = list(window.get("sourceBatches"), 960);
            require(windowBatches.size() == batches.size(), "retained source ledgers differ");
            for (int j = 0; j < batches.size(); j++) {
                var row = object(windowBatches.get(j)); var actual = batches.get(j);
                require(number(row, "index") == actual.index() && number(row, "issuedAtNanos") == actual.issuedAtNanos()
                        && number(row, "completedAtNanos") == actual.completedAtNanos(), "original source ledger projections differ");
            }
            var capture = reconstructCapture(retained, producer, workload.id() + "/" + phase.id());
            var hello = new ArrayList<BenchmarkTargetClock.Reading>();
            hello.add(reading(object(clocks.get("before"))));
            for (Object raw : list(object(clocks.get("sampledInterior")).get("readings"), BenchmarkTargetClock.MAX_INTERIOR_SAMPLES))
                hello.add(reading(object(raw)));
            hello.add(reading(object(clocks.get("after"))));
            var retainedHello = list(object(retained.get("helloSerialValidation")).get("readings"), BenchmarkTargetClock.MAX_INTERIOR_SAMPLES);
            require(retainedHello.size() == hello.size(), "serial hello projections differ");
            for (int j = 0; j < hello.size(); j++) require(hello.get(j).equals(reading(object(retainedHello.get(j)))),
                    "serial hello readings differ from original brackets and sampler");
            var verified = BenchmarkReturnMethodAdmission.verifyPhase(admitted, new BenchmarkReturnMethodAdmission.PhaseFacts(
                    workload, phase, batches, capture, root, producer, object(clocks.get("nativeClockBefore")),
                    object(clocks.get("nativeClockAfter")), hello));
            var actualCommon = verified.commonWindow(); var retainedCommon = object(retained.get("commonWindowCertificate"));
            require(io.tapstate.core.common.JsonReader.parse(JsonWriter.write(actualCommon)).equals(retainedCommon),
                    "retained common-window proof differs from actual raw/source reconstruction");
            requireMatchingClockContexts(retained, verified.evidence());
            for (String field : List.of("captureEpoch", "frameCount", "rawPageBytes", "pageCount", "callCount", "fullRows", "fixedCohortRows"))
                require(number(verified.evidence(), field) == number(retained, field), "retained capture totals differ: " + field);
            phases.add(verified.intervalMetrics());
            var load = BenchmarkReturnOfferedLoad.evidence(workload, phase, batches,
                    (Long) actualCommon.get("durationNanosLower"), (Long) actualCommon.get("durationNanosUpper"));
            require("PASS".equals(load.get("state")), "measured phase did not sustain its fixed offer: " + phase.id() + " " + load.get("reason"));
            offered.add(Map.of("phase", phase.id(), "evidence", load));
            cpu.add(BenchmarkReturnCpuPerRow.phase(object(window.get("resources")), verified.deliveries(),
                    object(retained.get("resourceBounds"))));
        }
        return new CheckedPhases(BenchmarkReturnIntervalMetrics.aggregate(phases), BenchmarkReturnCpuPerRow.perRow(cpu), List.copyOf(offered));
    }

    static void requireMatchingClockContexts(Map<String, Object> retained, Map<String, Object> reconstructed) {
        for (String field : List.of("sameDomainSerialCalibration", "helloSerialValidation")) {
            require(io.tapstate.core.common.JsonReader.parse(JsonWriter.write(reconstructed.get(field)))
                            .equals(object(retained.get(field))),
                    "retained calibration or hello context differs from actual reconstruction: " + field);
        }
    }

    static BenchmarkWriteReturnCapture.Result reconstructCapture(Map<String, Object> retained,
            BenchmarkCausalClock.Identity producer, String window) {
        require("PERIODIC".equals(retained.get("clockSamplingMode"))
                && "UNROUNDED_NOMINAL_COUNTER_ENCLOSURES".equals(retained.get("returnTimeCoordinate"))
                && !retained.containsKey("producerCostStages"), "original nominal periodic capture meaning differs");
        var summary = object(retained.get("summary"));
        require(window.equals(summary.get("window")), "acquired capture belongs to another original phase");
        var typed = new BenchmarkWriteReturnReader.Summary(window, text(summary.get("state"), 128),
                number(summary, "completedCalls"), number(summary, "failedCalls"), number(summary, "reportedRecords"),
                number(summary, "openCalls"), number(summary, "retainedBytes"));
        require(typed.completedCalls() <= BenchmarkReturnLocalSourceWork.MAX_STORED_CALLS && typed.reportedRecords() <= 262144 && typed.retainedBytes() <= 2097152,
                "acquired capture summary exceeds prior source material");
        var pages = strings(retained.get("pagesBase64"), 512, 4 * ((65536 + 2) / 3));
        require(!pages.isEmpty(), "complete original raw pages are missing");
        long bytes = 0;
        var assembly = new BenchmarkWriteReturnAssembly(window);
        for (String page : pages) {
            byte[] raw;
            try { raw = Base64.getDecoder().decode(page); }
            catch (IllegalArgumentException bad) { throw new AssertionError("invalid original raw page base64", bad); }
            bytes = Math.addExact(bytes, raw.length);
            require(raw.length <= 65536 && bytes <= 2097152 && Base64.getEncoder().encodeToString(raw).equals(page),
                    "original raw page exceeds its bound or is noncanonical");
            assembly.add(raw);
        }
        var calls = assembly.finish(typed.completedCalls(), typed.reportedRecords());
        var samples = new ArrayList<BenchmarkCausalClock.Sample>();
        for (Object raw : list(retained.get("clockSamples"), BenchmarkCausalClock.MAX_SAMPLES)) {
            var row = object(raw);
            var actual = new BenchmarkCausalClock.Identity(number(row, "pid"), number(row, "jvmStartTimeMillis"));
            require(producer.equals(actual), "original clock sample belongs to another producer");
            samples.add(new BenchmarkCausalClock.Sample(number(row, "sequence"), actual,
                    number(row, "driverBeforeNanos"), number(row, "driverAfterNanos"), number(row, "ownedNanos")));
        }
        var retainedOwner = object(retained.get("ownedRuntime"));
        require(producer.pid() == number(retainedOwner, "pid") && producer.jvmStartTimeMillis() == number(retainedOwner, "jvmStartTimeMillis"),
                "original capture owner differs from completed producer");
        return new BenchmarkWriteReturnCapture.Result(calls, samples, typed, pages, BenchmarkReturnClockSampler.Mode.PERIODIC);
    }
    private static BenchmarkTargetClock.Reading reading(Map<String, Object> value) {
        require(number(value, "resolutionMillis") == 1 && "SERVER_LOCAL_TIME_REQUEST_BRACKET".equals(value.get("scope")),
                "original hello coordinate meaning differs");
        return new BenchmarkTargetClock.Reading(text(value.get("primary"), 65536), text(value.get("processId"), 24),
                number(value, "serverWallMillis"), number(value, "startedAtNanos"), number(value, "completedAtNanos"),
                number(value, "localUtcBeforeMillis"), number(value, "localUtcAfterMillis"));
    }
    private static BigDecimal rateDecimal(double value, boolean upper) {
        require(Double.isFinite(value) && value >= 0.000001 && value < 300000000000000.0,
                "recomputed rate exceeds original count/duration source domain");
        return new BigDecimal(value).setScale(48, upper ? RoundingMode.CEILING : RoundingMode.FLOOR).stripTrailingZeros();
    }
    static void requireMatchingInterval(Map<String, Object> interval, BenchmarkReturnIntervalMetrics.Fork recomputed) {
        require(interval != null && interval.size() <= 64 && recomputed != null, "complete interval correspondence input is missing");
        var count = object(interval.get("completedRows")); var duration = object(interval.get("durationNanos"));
        var p99 = object(interval.get("p99Nanos")); var rate = object(interval.get("throughput"));
        require(number(count, "lower") == recomputed.completedLower() && number(count, "upper") == recomputed.completedUpper()
                && number(duration, "lower") == recomputed.durationLower() && number(duration, "upper") == recomputed.durationUpper()
                && number(interval, "fullCohortRows") == recomputed.fullCohortRows()
                && strings(interval.get("phases"), 6, 128).equals(recomputed.phases())
                && signedLong(p99.get("lower")) == recomputed.p99().lowerNanos() && signedLong(p99.get("upper")) == recomputed.p99().upperNanos()
                && decimal(rate.get("lower")).compareTo(rateDecimal(recomputed.throughput().lowerRecordsPerSecond(), false)) == 0
                && decimal(rate.get("upper")).compareTo(rateDecimal(recomputed.throughput().upperRecordsPerSecond(), true)) == 0,
                "retained pooled interval differs from independently reconstructed complete original phase facts");
    }

    /** A caller cannot promote a diagnostic Receipt, DTO or open-prefix token into this type. */
    static final class CompletedFork {
        private final String id, workload;
        private final PipelineBenchmarkComparison.Arm arm;
        private final int ordinal, cohort;
        private final BenchmarkAckOracle.Fork correctness;
        private final BenchmarkReturnIntervalComparison.Bounds rate;
        private final BenchmarkCausalClock.Interval p99;
        private final BenchmarkReturnIntervalComparison.Bounds cpuPerRow;
        private final long heap, rss, countLower, countUpper, durationLower, durationUpper;
        private final List<String> phases;
        private final Map<String, Object> proof;
        private final BenchmarkReturnWorkBudgetBindings.FileFact application;
        private CompletedFork(String id, String workload, PipelineBenchmarkComparison.Arm arm, int ordinal,
                BenchmarkAckOracle.Fork correctness, BenchmarkReturnIntervalComparison.Bounds rate,
                BenchmarkCausalClock.Interval p99, BenchmarkReturnIntervalComparison.Bounds cpuPerRow, int cohort, long heap, long rss,
                long countLower, long countUpper, long durationLower, long durationUpper,
                List<String> phases, Map<String, Object> proof, BenchmarkReturnWorkBudgetBindings.FileFact application) {
            this.id = id; this.workload = workload; this.arm = arm; this.ordinal = ordinal;
            this.correctness = correctness; this.rate = rate; this.p99 = p99; this.cpuPerRow = cpuPerRow; this.cohort = cohort;
            this.heap = heap; this.rss = rss; this.countLower = countLower; this.countUpper = countUpper;
            this.durationLower = durationLower; this.durationUpper = durationUpper; this.phases = List.copyOf(phases); this.proof = proof;
            this.application = application;
        }
        String id() { return id; }
        String workload() { return workload; }
        PipelineBenchmarkComparison.Arm arm() { return arm; }
        int ordinal() { return ordinal; }
        BenchmarkAckOracle.Fork correctness() { return correctness; }
        Map<String, Object> completionProof() { return proof; }
        BenchmarkReturnWorkBudgetBindings.FileFact application() { return application; }
        BenchmarkReturnIntervalComparison.Fork comparison() {
            require(p99.lowerNanos() > 0, "retained p99 enclosure is not positive for relative comparison");
            return new BenchmarkReturnIntervalComparison.Fork(arm, rate,
                    new BenchmarkReturnIntervalComparison.Bounds(BigDecimal.valueOf(p99.lowerNanos()), BigDecimal.valueOf(p99.upperNanos())),
                    cpuPerRow, cohort, heap, rss);
        }
        Map<String, Object> evidence() {
            return Map.ofEntries(Map.entry("id", id), Map.entry("workload", workload), Map.entry("arm", arm.name()),
                    Map.entry("ordinal", ordinal), Map.entry("phases", phases), Map.entry("fullCohortRows", cohort),
                    Map.entry("throughput", Map.of("lower", rate.lower().toString(), "upper", rate.upper().toString())),
                    Map.entry("p99Nanos", Map.of("lower", p99.lowerNanos(), "upper", p99.upperNanos())),
                    Map.entry("completedRows", Map.of("lower", countLower, "upper", countUpper)),
                    Map.entry("durationNanos", Map.of("lower", durationLower, "upper", durationUpper)),
                    Map.entry("peakHeapBytes", heap), Map.entry("peakRssBytes", rss), Map.entry("completedMethod", proof),
                    Map.entry("application", Map.of("path", application.path(), "sha256", application.blob().sha256(), "bytes", application.blob().bytes())));
        }
    }

    private static BenchmarkAckOracle.Fork correctness(Map<String, Object> report,
            BenchmarkWorkloadDefinitions.Workload workload, String id) {
        var chainDefinitions = new LinkedHashMap<String, BenchmarkWorkloadDefinitions.SourceChain>();
        for (var chain : workload.sourceChains()) chainDefinitions.put(chain.id(), chain);
        var chains = new ArrayList<BenchmarkAckOracle.SourceChain>();
        for (Object raw : list(report.get("sourceChains"), 16)) {
            var row = object(raw); String chainId = text(row.get("id"), 512);
            var registered = chainDefinitions.remove(chainId);
            require(registered != null, "independent original source chain is foreign or repeated");
            require("TABLE_ORDER".equals(row.get("ackProofKind")), "original independent table ACK proof is missing");
            var terminals = list(row.get("sourceTerminals"), 1); require(terminals.size() == 1, "original terminal is missing");
            var terminal = object(terminals.getFirst());
            String logical = text(terminal.get("logicalId"), 512), sourcePosition = text(terminal.get("sourcePosition"), 65536);
            require(registered.terminalLogicalId().equals(logical), "original terminal identity differs");
            var table = object(row.get("tableConfirmation"));
            require(logical.equals(table.get("terminalLogicalId")) && sourcePosition.equals(table.get("sourceTerminalToken"))
                    && registered.table().equals(table.get("table")), "table ACK identity differs");
            var cursorPlan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(4096, 16, 65536, 65536, 128, 24);
            var cursorTree = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(object(table.get("confirmedConsumer")), cursorPlan).value();
            String cursorJson = JsonWriter.write(cursorTree);
            require(cursorJson.length() <= 65536, "retained original consumer exceeds its prior bound");
            var cursor = Document.parse(cursorJson);
            String physical = text(table.get("physicalChainId"), 512);
            var binding = BenchmarkTableAckGate.bind(physical, registered.pipelineId(), registered.sourceId(), registered.table(), cursor);
            require(binding.consumer().equals(table.get("actualConsumerId"))
                    && binding.writers().equals(strings(table.get("expectedWriters"), 128, 512)), "original table writer binding differs");
            var marker = new BenchmarkTableTerminalObserver.Point(logical, text(table.get("ring"), 512),
                    number(table, "epoch"), number(table, "seq"), text(table.get("actualMarkerSourceToken"), 65536));
            var proof = new BenchmarkAckOracle.TableConfirmationProof(logical, sourcePosition, marker, binding, cursorJson);
            Object ack = row.get("authoritativeTargetAck");
            chains.add(new BenchmarkAckOracle.SourceChain(chainId, List.of(new BenchmarkAckOracle.TerminalEvent(logical, sourcePosition)),
                    ack == null ? null : text(ack, 65536), null, proof));
        }
        require(chainDefinitions.isEmpty(), "original source chains are incomplete");
        var coverage = object(report.get("logicalCoverage"));
        require(!coverage.isEmpty() && coverage.size() <= logicalCoverageUpper(workload),
                "original logical coverage exceeds its complete fork source bound");
        var counts = new LinkedHashMap<String, Long>();
        for (var entry : coverage.entrySet()) counts.put(text(entry.getKey(), 512), number(coverage, entry.getKey()));
        String checksum = text(report.get("correctnessChecksum"), 65536);
        require(checksum.equals(report.get("checksum")), "independent correctness and retained output checksums differ");
        return new BenchmarkAckOracle.Fork(id, chains, counts, checksum, number(report, "errorTotal"));
    }
    static long logicalCoverageUpper(BenchmarkWorkloadDefinitions.Workload workload) {
        require(workload != null && workload.phases().size() <= 6 && workload.sourceChains().size() <= 4,
                "original coverage recipe is missing");
        long rows = workload.sourceChains().size();
        for (var phase : workload.phases()) {
            if (phase.measured()) rows = Math.addExact(rows, phase.expectedLogicalOutputChanges());
            if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) for (var target : phase.targets()) {
                rows = Math.addExact(rows, BenchmarkTerminalTargetChanges.forTarget(workload, target).size());
                rows = Math.addExact(rows, BenchmarkTerminalTargetChanges.optionalForTarget(workload, target).size());
            }
        }
        return rows;
    }
    private static BenchmarkReturnWorkBudget.Work vector(Object value) {
        var row = object(value);
        require(row.keySet().equals(java.util.Set.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates")),
                "structural source vector differs");
        return new BenchmarkReturnWorkBudget.Work(number(row, "calls"), number(row, "requests"), number(row, "records"), number(row, "frames"),
                number(row, "bytes"), number(row, "passes"), number(row, "elements"), number(row, "updates"));
    }
    private static long signedLong(Object value) {
        require(value instanceof Integer || value instanceof Long, "exact signed integer is missing"); return ((Number) value).longValue();
    }
    private static BigDecimal decimal(Object value) {
        String raw = value instanceof String ? text(value, 160) : value instanceof Number ? value.toString() : null;
        require(raw != null && raw.length() <= 160, "bounded decimal enclosure is missing");
        try { return new BigDecimal(raw); } catch (NumberFormatException bad) { throw new AssertionError("nondecimal interval enclosure", bad); }
    }
}
