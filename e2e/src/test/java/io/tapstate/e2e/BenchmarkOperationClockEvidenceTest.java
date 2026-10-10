package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkOperationClockEvidenceTest {
    private static final Map<String, Object> MISSING = Map.of("status", "MISSING");

    @Test
    void formalEntryRejectsClockDiagnosticsBeforeAssumptionsFilesOrServices() {
        Map<String, String> prior = new LinkedHashMap<>();
        for (String property : List.of(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY,
                BenchmarkWitnessReadGate.PROPERTY, "tapstate.e2e.benchmark.compilation-diagnostics",
                "tapstate.e2e.benchmark.thread-point-diagnostics", "tapstate.e2e.benchmark.baseline-jar",
                "tapstate.e2e.benchmark.candidate-jar", "tapstate.e2e.benchmark.output",
                "tapstate.e2e.benchmark.gate", "tapstate.e2e.benchmark.target", "tapstate.e2e.benchmark.primary")) {
            prior.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        try {
            for (String enabled : List.of(BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY,
                    BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY)) {
                System.setProperty(enabled, "true");
                assertThatThrownBy(() -> new PipelineBenchmarkLiveRunIT()
                        .interleavedRealForksWriteEvidenceAndEnforceTheSelectedGate())
                        .isInstanceOf(AssertionError.class).hasMessageContaining("clock refusal evidence");
                System.clearProperty(enabled);
            }
        } finally {
            prior.forEach((property, value) -> {
                if (value == null) { System.clearProperty(property); }
                else { System.setProperty(property, value); }
            });
        }
    }

    @Test
    void nativeEvidenceWithoutDecodedClockModeRefusesBeforeFilesOrServices() {
        Map<String, String> prior = new LinkedHashMap<>();
        Map<String, String> requested = Map.of(
                BenchmarkMongoDeliveryObserver.NATIVE_OPERATION_WALL_EVIDENCE_PROPERTY, "true",
                BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY, "false",
                "tapstate.e2e.benchmark-smoke.jar", "unused.jar",
                "tapstate.e2e.benchmark-smoke.arm", "B",
                "tapstate.e2e.benchmark-smoke.capture-mode", "PLAIN");
        requested.forEach((property, value) -> prior.put(property, System.setProperty(property, value)));
        String output = "tapstate.e2e.benchmark-smoke.fork-output";
        prior.put(output, System.getProperty(output)); System.clearProperty(output);
        try {
            assertThatThrownBy(() -> new RealBenchmarkForkDriverIT().statelessForkUsesPgoutputAndItsOwnTerminalPosition())
                    .isInstanceOf(AssertionError.class).hasMessageContaining("requires decoded clock refusal evidence");
        } finally {
            prior.forEach((property, value) -> {
                if (value == null) { System.clearProperty(property); } else { System.setProperty(property, value); }
            });
        }
    }

    @Test
    void actualDocumentKeysRemainLiteralOrExplicitlyMissing() {
        var recorder = recorder(); var supplied = event("human key is independent", 30L, true);
        String literal = "{\"_id\": {\"$numberLong\": \"1\"}}";
        supplied.put("documentKey", literal); recorder.accepted(supplied);
        assertThat(metadata(recorder.evidence(), "previousAccepted")).containsEntry("documentKey", literal);
        var missing = event("another human key", 31L, true); missing.put("documentKey", MISSING);
        recorder.accepted(missing);
        assertThat(metadata(recorder.evidence(), "previousAccepted")).containsEntry("documentKey", MISSING);
    }

    @Test
    void acceptedMetadataFailureCannotOverrideTheAuthoritativeAcceptedEvent() {
        var recorder = recorder(); recorder.accepted(event("retained", 30L, true));
        var oversized = event("already accepted by clock gates", 31L, true);
        oversized.put("resumeToken", "x".repeat(16384));
        Throwable[] recordingFailure = new Throwable[1];
        assertThatCode(() -> recordingFailure[0] = BenchmarkMongoDeliveryObserver.recordAcceptedClockEvent(
                recorder, () -> oversized)).doesNotThrowAnyException();
        assertThat(recordingFailure[0]).isInstanceOf(AssertionError.class).hasMessageContaining("byte budget");
        assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED").containsEntry("acceptedEvents", 1L);
        assertThat(records(recorder.evidence())).hasSize(1);
        assertThat(metadata(recorder.evidence(), "previousAccepted")).containsEntry("key", "retained");
        assertThat(role(recorder.evidence(), "currentRejected")).containsEntry("status", "UNKNOWN_RECORDER_FAILED");
    }

    @Test
    void decodedMetadataConstructionFailureAlsoRemainsARecorderFailure() {
        var recorder = recorder();
        var original = new IllegalStateException("decoded resume token unavailable");
        Throwable[] recordingFailure = new Throwable[1];
        assertThatCode(() -> recordingFailure[0] = BenchmarkMongoDeliveryObserver.recordAcceptedClockEvent(
                recorder, () -> { throw original; })).doesNotThrowAnyException();
        assertThat(recordingFailure[0]).isSameAs(original);
        assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED").containsEntry("acceptedEvents", 0L);
        assertThat(records(recorder.evidence())).isEmpty();
        assertThat(role(recorder.evidence(), "currentRejected")).containsEntry("status", "UNKNOWN_RECORDER_FAILED");
    }

    @Test
    void aLaterClockRefusalKeepsTheFirstRecorderFailureAndOriginalAssertion() {
        var recorder = recorder(); recorder.accepted(event("retained", 30L, true));
        var originalRecording = new IllegalStateException("actual metadata construction failed");
        Throwable recording = BenchmarkMongoDeliveryObserver.recordAcceptedClockEvent(recorder,
                () -> { throw originalRecording; });
        var before = recorder.evidence();
        var originalClock = new AssertionError("actual authoritative three millisecond rejection");
        assertThatCode(() -> BenchmarkMongoDeliveryObserver.recordRejectedClockEvent(recorder, recording,
                () -> { throw new AssertionError("failed recorder must not construct another event"); }, originalClock))
                .doesNotThrowAnyException();
        assertThat(originalClock.getSuppressed()).containsExactly(originalRecording);
        assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED")
                .containsEntry("rejectionReason", originalClock.getMessage()).containsEntry("acceptedEvents", 1L);
        assertThat(recorder.evidence().get("records")).isEqualTo(before.get("records"));
        assertThat(before).containsEntry("rejectionReason", null);
        assertThat(role(recorder.evidence(), "currentRejected")).containsEntry("status", "UNKNOWN_RECORDER_FAILED");
    }

    @Test
    void rejectedMetadataConstructionFailureCannotReplaceOrInventTheClockRefusal() {
        var recorder = recorder(); recorder.accepted(event("retained", 30L, true));
        var recording = new IllegalStateException("actual rejected metadata unavailable");
        var originalClock = new AssertionError("actual authoritative three millisecond rejection");
        assertThatCode(() -> BenchmarkMongoDeliveryObserver.recordRejectedClockEvent(recorder, null,
                () -> { throw recording; }, originalClock)).doesNotThrowAnyException();
        assertThat(originalClock.getSuppressed()).containsExactly(recording);
        assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED")
                .containsEntry("rejectionReason", originalClock.getMessage()).containsEntry("acceptedEvents", 1L);
        assertThat(records(recorder.evidence())).hasSize(1);
        assertThat(role(recorder.evidence(), "currentRejected")).containsEntry("status", "UNKNOWN_RECORDER_FAILED");
    }

    @Test
    void retainedClockDiagnosticsCannotBecomeFormalAfterThePropertyIsCleared() {
        String property = BenchmarkMongoDeliveryObserver.CLOCK_REJECTION_EVIDENCE_PROPERTY;
        String prior = System.getProperty(property);
        System.clearProperty(property);
        try {
            var resources = new BenchmarkResourceSampler.Summary(1, 0, 1, 1, 2);
            var phase = new RealBenchmarkForkDriver.MeasuredPhase("cdc-update", 48000, 1, 2, 3, 96000, 96000, 96000,
                    new BenchmarkForkEnvironment.ClockAnchor(java.time.Instant.EPOCH, 1, 2), List.of(), resources,
                    java.util.Optional.empty(), java.util.Optional.empty(), true, java.util.Optional.empty(),
                    Map.of("operationClockRefusalEvidenceEnabled", true));
            var evidence = new RealBenchmarkForkDriver.Evidence("stateless-B-1",
                    BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B,
                    java.nio.file.Path.of("unused.jar"), List.of(phase), resources,
                    new BenchmarkMongoCommandSampler.Summary(Map.of(), Map.of(), 0), Map.of(), Map.of(), "unused", 0,
                    List.of(), java.util.Optional.empty());
            assertThatThrownBy(evidence::requireSteadyStateWindow).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("clock refusal evidence");
        } finally {
            if (prior == null) { System.clearProperty(property); } else { System.setProperty(property, prior); }
        }
    }

    @Test
    void actualRecorderReceiptsRefuseFormalQualificationEvenWhenTheFlagAndMarkerAreMissing() {
        var resources = new BenchmarkResourceSampler.Summary(1, 0, 1, 1, 2);
        var clock = Map.<String, Object>of("targetWitnessReadReceipts", List.of(Map.of("target", "owned-target",
                "readSchedule", Map.of("operationClockRefusalEvidence", recorder().evidence()))));
        var phase = new RealBenchmarkForkDriver.MeasuredPhase("cdc-update", 48000, 1, 2, 3, 96000, 96000, 96000,
                new BenchmarkForkEnvironment.ClockAnchor(java.time.Instant.EPOCH, 1, 2), List.of(), resources,
                java.util.Optional.empty(), java.util.Optional.empty(), true, java.util.Optional.empty(), clock);
        var evidence = new RealBenchmarkForkDriver.Evidence("stateless-B-1",
                BenchmarkWorkloadDefinitions.steadyPilot("stateless"), PipelineBenchmarkComparison.Arm.B,
                java.nio.file.Path.of("unused.jar"), List.of(phase), resources,
                new BenchmarkMongoCommandSampler.Summary(Map.of(), Map.of(), 0), Map.of(), Map.of(), "unused", 0,
                List.of(), java.util.Optional.empty());
        assertThatThrownBy(evidence::requireSteadyStateWindow).isInstanceOf(AssertionError.class)
                .hasMessageContaining("clock refusal evidence");
    }

    @Test
    void distinctHighWaterPreviousAndThreeMillisecondRefusalRetainAllExactFactsOnce() {
        var recorder = recorder();
        var high = event("high", 100005L, true);
        var previous = event("previous", 100004L, true);
        var rejected = event("rejected", 100002L, false);
        recorder.accepted(high); recorder.accepted(previous); recorder.rejected(rejected, "original two millisecond gate refused three milliseconds");
        var evidence = recorder.evidence();
        assertThat(records(evidence)).hasSize(3);
        assertThat(metadata(evidence, "highWater")).isEqualTo(high);
        assertThat(metadata(evidence, "previousAccepted")).isEqualTo(previous);
        assertThat(metadata(evidence, "currentRejected")).isEqualTo(rejected);
        assertThat(record(evidence, "currentRejected")).containsEntry("accepted", false);
        assertThat(evidence).containsEntry("acceptedEvents", 2L).containsEntry("state", "REJECTED")
                .containsEntry("clockQualification", "NOT_EVALUATED_BY_RECORDER").containsEntry("performanceAcceptanceEligible", false);
        assertThat(recorder.evidence()).isSameAs(evidence);
    }

    @Test
    void equalWallKeepsFirstHighWaterWhilePreviousTracksEveryAcceptedEvent() {
        var recorder = recorder(); recorder.accepted(event("first", 100L, true));
        var one = recorder.evidence(); assertThat(records(one)).hasSize(1);
        assertThat(role(one, "highWater").get("index")).isEqualTo(role(one, "previousAccepted").get("index"));
        recorder.accepted(event("tie", 100L, true)); recorder.accepted(event("one millisecond lower", 99L, true));
        var evidence = recorder.evidence();
        assertThat(metadata(evidence, "highWater").get("key")).isEqualTo("first");
        assertThat(metadata(evidence, "previousAccepted").get("key")).isEqualTo("one millisecond lower");
        assertThat(evidence).containsEntry("highWaterTiePolicy", "FIRST_ACCEPTED_MAX_WALL_TIME").containsEntry("acceptedEvents", 3L);
        assertThat(records(evidence)).hasSize(2);
    }

    @Test
    void recorderRetainsGivenGateResultsWithoutReimplementingNativeTimestampOrder() {
        var recorder = recorder(); var one = event("first", 10L, true); one.put("clusterTime", Map.of("seconds", 9L, "increment", 3L));
        recorder.accepted(one);
        var next = event("given accepted result", 11L, true); next.put("clusterTime", Map.of("seconds", 9L, "increment", 2L));
        recorder.accepted(next);
        assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(next);
        assertThat(recorder.evidence()).containsEntry("clockQualification", "NOT_EVALUATED_BY_RECORDER");
    }

    @Test
    void explicitMissingNativeFactsStayUnknownRatherThanInventingAZeroHighWater() {
        var recorder = recorder(); var absent = event("explicit absence", MISSING, true);
        absent.put("clusterTime", MISSING); absent.put("startedReadNanos", MISSING); absent.put("resumeToken", MISSING);
        recorder.accepted(absent);
        assertThat(role(recorder.evidence(), "highWater")).containsEntry("status", "UNKNOWN_NO_ACCEPTED_WALL");
        assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(absent);
        assertThat(role(recorder.evidence(), "highWater")).doesNotContainKey("index");
    }

    @Test
    void snapshotsDeepFreezeMutableMetadataAndSurviveARealPhaseReset() {
        var recorder = recorder(); var mutable = event("row", 20L, true);
        Map<String, Object> session = new LinkedHashMap<>(Map.of("id", "session-one")); mutable.put("session", session);
        recorder.accepted(mutable); var first = recorder.evidence(); session.put("id", "changed"); mutable.put("wallTime", 999L);
        assertThat(metadata(first, "highWater")).containsEntry("wallTime", 20L).containsEntry("session", Map.of("id", "session-one"));
        assertThatThrownBy(() -> metadata(first, "highWater").clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> records(first).clear()).isInstanceOf(UnsupportedOperationException.class);
        recorder.reset("db.orders", "owned-target", "phase-two");
        assertThat(records(recorder.evidence())).isEmpty(); assertThat(recorder.evidence()).containsEntry("acceptedEvents", 0L);
        assertThat(first.get("scope")).isEqualTo(Map.of("namespace", "db.orders", "targetId", "owned-target", "phaseId", "phase-one"));
        var newPhase = event("new", 21L, true); newPhase.put("phaseId", "phase-two"); recorder.accepted(newPhase);
        assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(newPhase);
        assertThat(metadata(first, "highWater").get("key")).isEqualTo("row");
    }

    @Test
    void malformedOrForeignEventsFailClosedWithoutCommittingTheirDataOrOrdinal() {
        for (String broken : List.of("foreign", "missing")) {
            var recorder = recorder(); var valid = event("retained", 30L, true); recorder.accepted(valid);
            var invalid = event("invalid", 31L, true);
            if (broken.equals("foreign")) { invalid.put("namespace", "another.orders"); } else { invalid.remove("clusterTime"); }
            assertThatThrownBy(() -> recorder.accepted(invalid)).isInstanceOf(AssertionError.class);
            assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED").containsEntry("acceptedEvents", 1L);
            assertThat(records(recorder.evidence())).hasSize(1);
            assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(valid);
            assertThat(role(recorder.evidence(), "currentRejected")).containsEntry("status", "UNKNOWN_RECORDER_FAILED");
            assertThatThrownBy(() -> recorder.accepted(valid)).isInstanceOf(AssertionError.class).hasMessageContaining("ended");
        }
    }

    @Test
    void oversizeMetadataIsRejectedAtomicallyAndNeverAppearsAsAnEmptyEvent() {
        var recorder = recorder(); var valid = event("retained", 30L, true); recorder.accepted(valid);
        var oversized = event("oversized", 31L, false); oversized.put("resumeToken", "x".repeat(16384));
        assertThatThrownBy(() -> recorder.rejected(oversized, "original clock refusal")).isInstanceOf(AssertionError.class).hasMessageContaining("byte budget");
        assertThat(recorder.evidence()).containsEntry("rejectionReason", "original clock refusal").containsEntry("state", "RECORDER_FAILED");
        assertThat(records(recorder.evidence())).hasSize(1); assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(valid);
    }

    @Test
    void fractionalClockFieldsAndAmbiguousMissingValuesAreNeverAccepted() {
        for (String field : List.of("wallTime", "clusterTime", "acceptedNanos", "observedNanos")) {
            var recorder = recorder(); var invalid = event("fraction", 30L, true);
            invalid.put(field, field.equals("clusterTime") ? Map.of("seconds", 20.5, "increment", 1L) : 30.5);
            assertThatThrownBy(() -> recorder.accepted(invalid)).isInstanceOf(AssertionError.class);
            assertThat(records(recorder.evidence())).isEmpty(); assertThat(recorder.evidence()).containsEntry("acceptedEvents", 0L);
        }
        var recorder = recorder(); var ambiguous = event("implicit absence", null, true);
        assertThatThrownBy(() -> recorder.accepted(ambiguous)).isInstanceOf(AssertionError.class);
    }

    @Test
    void malformedReadBracketsOrAcceptanceTimesFailClosedWhileNegativeNanosRemainValid() {
        for (String broken : List.of("backward", "observed", "accept", "overflow")) {
            var recorder = recorder(); var invalid = event("bad bracket", 30L, true);
            switch (broken) {
                case "backward" -> invalid.put("startedReadNanos", 201L);
                case "observed" -> invalid.put("observedNanos", 201L);
                case "accept" -> invalid.put("acceptedNanos", 199L);
                case "overflow" -> { invalid.put("startedReadNanos", Long.MIN_VALUE); invalid.put("completedReadNanos", Long.MAX_VALUE); invalid.put("observedNanos", Long.MAX_VALUE); }
                default -> throw new AssertionError("unhandled fixture");
            }
            assertThatThrownBy(() -> recorder.accepted(invalid)).isInstanceOf(AssertionError.class);
            assertThat(records(recorder.evidence())).isEmpty();
            assertThat(recorder.evidence()).containsEntry("state", "RECORDER_FAILED").containsEntry("acceptedEvents", 0L);
        }
        var recorder = recorder(); var negative = event("negative monotonic origin", 30L, true);
        negative.put("startedReadNanos", -100L); negative.put("completedReadNanos", -50L);
        negative.put("observedNanos", -50L); negative.put("acceptedNanos", -40L);
        recorder.accepted(negative); assertThat(metadata(recorder.evidence(), "previousAccepted")).isEqualTo(negative);
    }

    @Test
    void recordingFailureCannotReplaceTheOriginalClockAssertion() {
        var recorder = recorder(); recorder.accepted(event("prior", 30L, true));
        AssertionError original = new AssertionError("actual authoritative three millisecond rejection");
        var malformed = event("rejected", 27L, false); malformed.put("targetId", "foreign-target");
        try { recorder.rejected(malformed, original); throw new AssertionError("original failure was not propagated"); }
        catch (AssertionError actual) { assertThat(actual).isSameAs(original); }
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(recorder.evidence()).containsEntry("rejectionReason", original.getMessage()).containsEntry("state", "RECORDER_FAILED");
        assertThat(records(recorder.evidence())).hasSize(1);
    }

    @Test
    void manyAcceptedEventsRetainOnlyHighWaterPreviousAndOneRefusal() {
        var recorder = recorder(); recorder.accepted(event("maximum", 10000L, true));
        for (int i = 0; i < 200; i++) { recorder.accepted(event("accepted-" + i, 9999L, true)); }
        var refusal = event("last rejected", 9997L, false); refusal.put("acceptedNanos", MISSING);
        recorder.rejected(refusal, "gate refused");
        assertThat(records(recorder.evidence())).hasSize(3);
        assertThat(metadata(recorder.evidence(), "highWater").get("key")).isEqualTo("maximum");
        assertThat(metadata(recorder.evidence(), "previousAccepted").get("key")).isEqualTo("accepted-199");
        assertThat(metadata(recorder.evidence(), "currentRejected")).isEqualTo(refusal);
        assertThat(recorder.evidence()).containsEntry("acceptedEvents", 201L);
    }

    private static BenchmarkOperationClockEvidence recorder() { return new BenchmarkOperationClockEvidence("db.orders", "owned-target", "phase-one"); }
    private static Map<String, Object> event(String key, Object wall, boolean accepted) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("namespace", "db.orders"); out.put("targetId", "owned-target"); out.put("phaseId", "phase-one");
        out.put("key", key); out.put("operationType", "UPDATE"); out.put("clusterTime", Map.of("seconds", 20L, "increment", 7L));
        out.put("wallTime", wall); out.put("startedReadNanos", 100L); out.put("completedReadNanos", 200L); out.put("observedNanos", 200L);
        out.put("acceptedNanos", accepted ? 220L : null); out.put("resumeToken", "{\"_data\":\"exact-token\"}");
        out.put("transaction", Map.of("number", 9L)); out.put("session", Map.of("id", "exact-session")); return out;
    }
    @SuppressWarnings("unchecked") private static List<Map<String, Object>> records(Map<String, Object> evidence) { return (List<Map<String, Object>>) evidence.get("records"); }
    @SuppressWarnings("unchecked") private static Map<String, Object> role(Map<String, Object> evidence, String name) { return (Map<String, Object>) ((Map<?, ?>) evidence.get("roles")).get(name); }
    private static Map<String, Object> record(Map<String, Object> evidence, String role) { return records(evidence).get(((Number) role(evidence, role).get("index")).intValue()); }
    @SuppressWarnings("unchecked") private static Map<String, Object> metadata(Map<String, Object> evidence, String role) { return (Map<String, Object>) record(evidence, role).get("metadata"); }
}
