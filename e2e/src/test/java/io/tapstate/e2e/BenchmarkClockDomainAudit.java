package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Retains actual clock points and pair refusals without attributing their cause or qualifying time. */
final class BenchmarkClockDomainAudit {
    static final int TARGET_READS = 151;
    static final int MAX_READS = 160;
    record Owner(long pid, long jvmStartTimeMillis) { }
    record WallPoint(long wallMillis, long nanoBefore, long nanoAfter) {
        static WallPoint read() {
            long before = System.nanoTime(); long wall = System.currentTimeMillis();
            return new WallPoint(wall, before, System.nanoTime());
        }
        Map<String, Object> evidence() {
            return Map.of("wallMillis", wallMillis, "nanoBefore", nanoBefore, "nanoAfter", nanoAfter);
        }
    }
    record Exchange(int index, Owner owner, long threadId, WallPoint hostBefore,
                    BenchmarkTargetClock.Reading guest, WallPoint hostAfter) {
        Map<String, Object> evidence() {
            return Map.of("index", index, "rootPid", owner.pid(), "rootJvmStartTimeMillis", owner.jvmStartTimeMillis(),
                    "threadId", threadId, "hostBefore", hostBefore.evidence(), "hostAfter", hostAfter.evidence(),
                    "helloReadingAvailable", guest != null, "hello", guest == null ? Map.of() : guest.evidence());
        }
    }

    static Map<String, Object> facts() {
        return Map.of("purpose", "CLOCK_READ_FACTS_ONLY", "clockCause", "UNKNOWN",
                "performanceAcceptanceEligible", false, "formalPerformance", false, "costAcceptanceEligible", false,
                "samplingCostQualified", false, "continuousClockErrorBoundEstablished", false, "utcAccuracyQualified", false);
    }

    static List<Map<String, Object>> raw(List<Exchange> exchanges) {
        return exchanges.stream().map(Exchange::evidence).toList();
    }

    static Map<String, Object> evidence(List<Exchange> exchanges) {
        var rows = List.copyOf(exchanges);
        require(!rows.isEmpty() && rows.size() <= MAX_READS, "actual read roster is empty or exceeds its bound");
        var first = rows.getFirst();
        require(first.owner().pid() > 0 && first.owner().jvmStartTimeMillis() > 0, "root identity is missing");
        List<WallPoint> host = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i); var guest = row.guest();
            require(row.index() == i && row.owner().equals(first.owner()) && row.threadId() > 0,
                    "actual root identity or read index changed");
            require(guest != null && guest.primary().equals(first.guest().primary())
                    && guest.processId().equals(first.guest().processId()), "actual hello identity is missing or changed");
            ordered(row.hostBefore().nanoBefore(), row.hostBefore().nanoAfter());
            ordered(row.hostBefore().nanoAfter(), guest.startedAtNanos());
            ordered(guest.startedAtNanos(), guest.completedAtNanos());
            ordered(guest.completedAtNanos(), row.hostAfter().nanoBefore());
            ordered(row.hostAfter().nanoBefore(), row.hostAfter().nanoAfter());
            if (i > 0) { ordered(rows.get(i - 1).hostAfter().nanoAfter(), row.hostBefore().nanoBefore()); }
            ordered(first.hostBefore().nanoBefore(), row.hostAfter().nanoAfter());
            host.add(row.hostBefore()); host.add(row.hostAfter());
        }
        List<Map<String, Object>> guestChecks = new ArrayList<>(), hostChecks = new ArrayList<>();
        for (int i = 1; i < rows.size(); i++) {
            guestChecks.add(guestCheck(rows, i - 1, i, "ADJACENT"));
            guestChecks.add(guestCheck(rows, 0, i, "FIRST_TO_EACH"));
        }
        for (int i = 1; i < host.size(); i++) {
            hostChecks.add(hostCheck(host, i - 1, i, "ADJACENT"));
            hostChecks.add(hostCheck(host, 0, i, "FIRST_TO_EACH"));
        }
        var result = new LinkedHashMap<>(facts());
        result.put("state", "RECORDED"); result.put("readings", raw(rows)); result.put("helloCommands", rows.size());
        result.put("guestChecks", List.copyOf(guestChecks)); result.put("hostChecks", List.copyOf(hostChecks));
        result.put("guestRefusals", refusals(guestChecks)); result.put("hostRefusals", refusals(hostChecks));
        result.put("endpointToleranceMillis", BenchmarkTargetClock.ENDPOINT_RESOLUTION_ERROR_MILLIS);
        return Map.copyOf(result);
    }

    private static Map<String, Object> guestCheck(List<Exchange> rows, int from, int to, String kind) {
        var before = rows.get(from).guest(); var after = rows.get(to).guest();
        var result = pair(from, to, kind, before.serverWallMillis(), after.serverWallMillis(),
                before.startedAtNanos(), before.completedAtNanos(), after.startedAtNanos(), after.completedAtNanos());
        try { BenchmarkTargetClock.validate(before, after); result.put("state", "WITHIN_RECORDED_PAIR_TOLERANCE"); }
        catch (AssertionError refusal) { result.put("state", "REFUSED"); result.put("reason", refusal.getMessage()); }
        return Map.copyOf(result);
    }

    private static Map<String, Object> hostCheck(List<WallPoint> points, int from, int to, String kind) {
        var before = points.get(from); var after = points.get(to);
        var result = pair(from, to, kind, before.wallMillis(), after.wallMillis(),
                before.nanoBefore(), before.nanoAfter(), after.nanoBefore(), after.nanoAfter());
        long delta = (long) result.get("wallElapsedMillis"), lower = (long) result.get("minimumElapsedMillis"),
                upper = (long) result.get("maximumElapsedMillis");
        int tolerance = BenchmarkTargetClock.ENDPOINT_RESOLUTION_ERROR_MILLIS;
        result.put("state", delta < lower - tolerance || delta > upper + tolerance
                ? "REFUSED" : "WITHIN_RECORDED_PAIR_TOLERANCE");
        return Map.copyOf(result);
    }

    private static LinkedHashMap<String, Object> pair(int from, int to, String kind, long beforeWall, long afterWall,
            long beforeStart, long beforeEnd, long afterStart, long afterEnd) {
        var result = new LinkedHashMap<String, Object>();
        result.put("from", from); result.put("to", to); result.put("pairKind", kind);
        result.put("wallElapsedMillis", Math.subtractExact(afterWall, beforeWall));
        result.put("minimumElapsedMillis", Math.subtractExact(afterStart, beforeEnd) / 1_000_000L);
        result.put("maximumElapsedMillis", Math.subtractExact(afterEnd, beforeStart) / 1_000_000L);
        return result;
    }
    private static long refusals(List<Map<String, Object>> checks) {
        return checks.stream().filter(check -> "REFUSED".equals(check.get("state"))).count();
    }
    private static void ordered(long before, long after) { require(Math.subtractExact(after, before) >= 0, "actual root brackets are not serial"); }
    private static void require(boolean value, String message) { if (!value) { throw new AssertionError("clock audit " + message); } }
}
