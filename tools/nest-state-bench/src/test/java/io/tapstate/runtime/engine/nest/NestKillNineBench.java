package io.tapstate.runtime.engine.nest;

import io.tapstate.adapters.mongostore.KillRig;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * Kills a nest member with SIGKILL, or freezes it with SIGSTOP, part way through a commit group, on the
 * production transactional store, and reads back what the group left, whether the dead or frozen member's
 * generation can still write, how long a takeover waits, and what the new owner reads first.
 *
 * <p>Two members (separate JVMs) own disjoint key ranges of one namespace under one pipeline claim; the
 * claim owner's process also renews the lease. Each trial runs once with groups written the way the direct
 * store writes them today ({@code split}: one transaction per 256 keys, per release, per delete) and once
 * with the whole group in one transaction ({@code atomic}). Results are printed as JSON lines and appended
 * to {@code target/kill-nine.jsonl}.
 *
 * <pre>
 * mvn -f tools/nest-state-bench/pom.xml test -Dtest=NestKillNineBench [-Dkill.trials=8] [-Dkill.zombies=3]
 *     [-Dkill.mongo.url=mongodb://host:27017/?replicaSet=rs0]   (an existing replica set instead of Testcontainers)
 * </pre>
 */
class NestKillNineBench {

    private static final Duration LEASE = Duration.ofMillis(Long.getLong("kill.lease", 3_000L));
    private static final int KEYS = Integer.getInteger("kill.keys", 1000);
    private static final int PAD = Integer.getInteger("kill.pad", 1024);
    private static final Path OUT = Path.of("target", "kill-nine.jsonl");

    @Test
    void killNineMidGroup() throws Exception {
        String given = System.getProperty("kill.mongo.url");
        try (MongoDBContainer mongo = given != null ? null : new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            if (mongo != null) mongo.start();
            String url = given != null ? given : mongo.getReplicaSetUrl();
            for (String mode : List.of("split", "atomic")) {
                for (int trial = 0; trial < Integer.getInteger("kill.trials", 8); trial++) killTrial(url, mode, trial);
            }
            for (String mode : List.of("split", "atomic")) {
                for (int trial = 0; trial < Integer.getInteger("kill.zombies", 3); trial++) zombieTrial(url, mode, trial);
            }
            String replicaSet = System.getProperty("kill.rs.url");
            if (replicaSet != null) {
                for (String mode : List.of("split", "atomic")) {
                    for (int trial = 0; trial < Integer.getInteger("kill.primaries", 3); trial++) primaryTrial(replicaSet, mode, trial);
                }
            }
            if (Boolean.parseBoolean(System.getProperty("kill.contention", "true"))) {
                for (String mode : List.of("split", "atomic")) contention(url, mode);
            }
        }
    }

    /** SIGKILL the claim owner's member mid-group; the other member keeps running on the old generation. */
    private void killTrial(String url, String mode, int trial) throws Exception {
        WorkloadOwner a = new WorkloadOwner("node-a", UUID.randomUUID().toString());
        var prepared = KillRig.prepare(url, a, LEASE, Set.of("node-a", "node-b"));
        try (KillRig rig = prepared.rig()) {
            WorkloadClaimFence f1 = WorkloadClaimFence.from(prepared.claim());
            Member memberA = Member.start(url, rig.database(), mode, "a", "a", f1, 1, 0, true);
            Member memberB = Member.start(url, rig.database(), mode, "b", "b", f1, 1, 0, false);
            memberA.await(line -> line.startsWith("ACK 3 "), 120_000);
            long commitMs = memberA.meanCommitMillis();
            memberA.await(line -> line.startsWith("BEGIN ") && !line.startsWith("BEGIN 1 ")
                    && !line.startsWith("BEGIN 2 ") && !line.startsWith("BEGIN 3 "), 60_000);
            long delay = ThreadLocalRandom.current().nextLong(0, Math.max(1, commitMs + commitMs / 5));
            Thread.sleep(delay);
            long killedAt = System.currentTimeMillis();
            memberA.kill();
            long lastAck = memberA.lastAck();
            int openAfterKill = rig.openTransactions().size();

            Map<String, String> rows = rig.rows();
            Histogram aRows = Histogram.of(rows, "a");

            long[] takeover = takeOver(rig, prepared.claim(), killedAt);
            WorkloadClaim next = rig.current();
            WorkloadClaimFence f2 = WorkloadClaimFence.from(next);
            long advancedAt = takeover[1];

            Histogram bAtAdvance = Histogram.of(rig.rows(), "b");
            String bOutcome = memberB.awaitAny(line -> line.startsWith("REFUSED "), 60_000);
            memberB.kill();
            Histogram bAfter = Histogram.of(rig.rows(), "b");
            long bAcksAfterAdvance = memberB.acksAfter(advancedAt);

            Member adopter = Member.start(url, rig.database(), mode, "b2", "a,b", f2, lastAck + 1, 2, false, "-Dkill.read=true");
            adopter.awaitExit(120_000);
            String read = adopter.first(line -> line.startsWith("READ "));
            Map<String, String> finalRows = rig.rows();
            Histogram aFinal = Histogram.of(finalRows, "a");
            Histogram bFinal = Histogram.of(finalRows, "b");

            record("kill", mode, trial, Map.ofEntries(
                    Map.entry("commitMs", commitMs), Map.entry("killDelayMs", delay),
                    Map.entry("lastAckA", lastAck), Map.entry("aRowsByGroup", aRows.byGroup),
                    Map.entry("aTorn", aRows.byGroup.size() > 1), Map.entry("aDurable", aRows.min() >= lastAck),
                    Map.entry("aInFlightLanded", aRows.byGroup.getOrDefault(lastAck + 1, 0)),
                    Map.entry("openTxnsAfterKill", openAfterKill),
                    Map.entry("takeoverMs", takeover[0] - killedAt), Map.entry("takeoverAttempts", takeover[2]),
                    Map.entry("bOldGeneration", bOutcome == null ? "not refused in 60 s" : bOutcome),
                    Map.entry("bAcksAfterAdvance", bAcksAfterAdvance),
                    Map.entry("bRowsChangedAfterAdvance", !bAtAdvance.byGroup.equals(bAfter.byGroup)),
                    Map.entry("adopterRead", read == null ? "none" : read),
                    Map.entry("adopterExit", adopter.exit()),
                    Map.entry("newGeneration", f2.executionGeneration()),
                    Map.entry("converged", aFinal.byWriter.equals(Map.of("b2@" + f2.executionGeneration() + "#" + (lastAck + 2), KEYS))
                            && bFinal.byWriter.equals(Map.of("b2@" + f2.executionGeneration() + "#" + (lastAck + 2), KEYS)))));
        }
    }

    /**
     * SIGSTOP the claim owner's member mid-group: a member that is alive but not running, as a long pause
     * or a partition makes it. Its lease lapses, another node takes the claim and rewrites its keys, then
     * the frozen member resumes and tries to finish the group it was in.
     */
    private void zombieTrial(String url, String mode, int trial) throws Exception {
        WorkloadOwner a = new WorkloadOwner("node-a", UUID.randomUUID().toString());
        var prepared = KillRig.prepare(url, a, LEASE, Set.of("node-a", "node-b"));
        try (KillRig rig = prepared.rig()) {
            WorkloadClaimFence f1 = WorkloadClaimFence.from(prepared.claim());
            Member memberA = Member.start(url, rig.database(), mode, "a", "a", f1, 1, 0, true);
            memberA.await(line -> line.startsWith("ACK 3 "), 120_000);
            long commitMs = memberA.meanCommitMillis();
            memberA.await(line -> line.startsWith("BEGIN ") && !line.startsWith("BEGIN 1 ")
                    && !line.startsWith("BEGIN 2 ") && !line.startsWith("BEGIN 3 "), 60_000);
            long delay = ThreadLocalRandom.current().nextLong(0, Math.max(1, commitMs + commitMs / 5));
            Thread.sleep(delay);
            long stoppedAt = System.currentTimeMillis();
            memberA.signal("STOP");
            Thread.sleep(200);
            List<org.bson.Document> open = rig.openTransactions();
            Histogram atStop = Histogram.of(rig.rows(), "a");

            long[] takeover = takeOver(rig, prepared.claim(), stoppedAt);
            WorkloadClaimFence f2 = WorkloadClaimFence.from(rig.current());
            Member adopter = Member.start(url, rig.database(), mode, "b2", "a", f2, 1000, 1, false, "-Dkill.read=true");
            adopter.awaitExit(120_000);
            Histogram adopted = Histogram.of(rig.rows(), "a");

            long resumedAt = System.currentTimeMillis();
            memberA.signal("CONT");
            String outcome = memberA.awaitAny(line -> line.startsWith("REFUSED "), 150_000);
            memberA.kill();
            Histogram after = Histogram.of(rig.rows(), "a");

            record("zombie", mode, trial, Map.ofEntries(
                    Map.entry("commitMs", commitMs), Map.entry("stopDelayMs", delay),
                    Map.entry("openTxnsAtStop", open.size()),
                    Map.entry("aRowsAtStop", atStop.byGroup), Map.entry("tornAtStop", atStop.byGroup.size() > 1),
                    Map.entry("takeoverMs", takeover[0] - stoppedAt), Map.entry("takeoverAttempts", takeover[2]),
                    Map.entry("adopterExit", adopter.exit()),
                    Map.entry("adopterRead", String.valueOf(adopter.first(line -> line.startsWith("READ ")))),
                    Map.entry("adopted", adopted.byWriter),
                    Map.entry("zombieOutcome", outcome == null ? "no refusal in 150 s" : outcome),
                    Map.entry("zombieRefusalMs", outcome == null ? -1 : Long.parseLong(outcome.split(" ")[2]) - resumedAt),
                    Map.entry("zombieAcksAfterResume", memberA.acksAfter(resumedAt)),
                    Map.entry("zombieOverwrote", !after.byWriter.equals(adopted.byWriter))));
        }
    }

    /**
     * SIGKILL the MongoDB primary of a three-node replica set while a member is in a group: the member is
     * not killed, the store under it fails over. Each node runs in a container named {@code rs-<port>} on
     * the host network. Checks that every acknowledged group survives the election, that the member
     * finishes its run, and how long the failover stalls a commit.
     */
    private void primaryTrial(String url, String mode, int trial) throws Exception {
        WorkloadOwner a = new WorkloadOwner("node-a", UUID.randomUUID().toString());
        var prepared = KillRig.prepare(url, a, Duration.ofMinutes(10), Set.of("node-a"));
        try (KillRig rig = prepared.rig(); var client = com.mongodb.client.MongoClients.create(url)) {
            WorkloadClaimFence f1 = WorkloadClaimFence.from(prepared.claim());
            long groups = 12;
            Member memberA = Member.start(url, rig.database(), mode, "a", "a", f1, 1, groups, false);
            memberA.await(line -> line.startsWith("ACK 3 "), 120_000);
            long commitMs = memberA.meanCommitMillis();
            memberA.await(line -> line.startsWith("BEGIN 4 "), 60_000);
            long delay = ThreadLocalRandom.current().nextLong(0, Math.max(1, commitMs + commitMs / 5));
            Thread.sleep(delay);
            String primary = client.getDatabase("admin").runCommand(new org.bson.Document("hello", 1)).getString("primary");
            String container = "rs-" + primary.substring(primary.lastIndexOf(':') + 1);
            long killedAt = System.currentTimeMillis();
            long ackedBefore = memberA.lastAck();
            new ProcessBuilder("docker", "kill", "-s", "KILL", container).inheritIO().start().waitFor();
            memberA.awaitExit(300_000);
            long firstAckAfter = memberA.lines.stream().filter(line -> line.startsWith("ACK "))
                    .mapToLong(line -> Long.parseLong(line.split(" ")[2])).filter(at -> at > killedAt).min().orElse(-1);
            List<Long> latencies = memberA.commitMillis();
            Histogram rows = Histogram.of(rig.rows(), "a");
            new ProcessBuilder("docker", "start", container).inheritIO().start().waitFor();
            awaitHealthy(client);
            record("primary", mode, trial, Map.ofEntries(
                    Map.entry("killedPrimary", primary), Map.entry("killDelayMs", delay), Map.entry("commitMs", commitMs),
                    Map.entry("ackedBeforeKill", ackedBefore), Map.entry("memberExit", memberA.exit()),
                    Map.entry("lastAck", memberA.lastAck()),
                    Map.entry("stallMs", firstAckAfter < 0 ? -1 : firstAckAfter - killedAt),
                    Map.entry("maxCommitMs", latencies.stream().mapToLong(Long::longValue).max().orElse(-1)),
                    Map.entry("rowsByGroup", rows.byGroup),
                    Map.entry("allAtLastGroup", rows.byGroup.equals(Map.of(groups, KEYS)))));
        }
    }

    private static void awaitHealthy(com.mongodb.client.MongoClient client) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                var status = client.getDatabase("admin").runCommand(new org.bson.Document("replSetGetStatus", 1));
                long healthy = status.getList("members", org.bson.Document.class).stream()
                        .filter(member -> List.of(1, 2).contains(member.getInteger("state"))).count();
                if (healthy == 3) return;
            } catch (RuntimeException notYet) {
                // the set is still electing
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("replica set did not recover");
    }

    /** Commit latency for one member alone and for two committing at once under one pipeline fence. */
    private void contention(String url, String mode) throws Exception {
        for (int members : List.of(1, 2)) {
            WorkloadOwner a = new WorkloadOwner("node-a", UUID.randomUUID().toString());
            var prepared = KillRig.prepare(url, a, Duration.ofMinutes(5), Set.of("node-a", "node-b"));
            try (KillRig rig = prepared.rig()) {
                WorkloadClaimFence f1 = WorkloadClaimFence.from(prepared.claim());
                List<Member> running = new ArrayList<>();
                for (int m = 0; m < members; m++) {
                    running.add(Member.start(url, rig.database(), mode, "m" + m, "m" + m, f1, 1, 30, false,
                            "-Dkill.keys=256"));
                }
                List<Long> latencies = new ArrayList<>();
                for (Member member : running) {
                    member.awaitExit(300_000);
                    latencies.addAll(member.commitMillis().subList(5, member.commitMillis().size()));
                }
                Collections.sort(latencies);
                record("contention", mode, members, Map.of(
                        "members", members, "keysPerGroup", 256, "groups", latencies.size(),
                        "p50Ms", latencies.get(latencies.size() / 2),
                        "p95Ms", latencies.get((int) (latencies.size() * 0.95)),
                        "exits", running.stream().map(Member::exit).toList()));
            }
        }
    }

    /**
     * Takes the pipeline claim for another node as soon as the store allows it, then advances it to a
     * new execution generation: the controlled rebuild after a member loss. Returns the time the claim was
     * acquired, the time the new generation was in place, and how many attempts were refused first.
     */
    private static long[] takeOver(KillRig rig, WorkloadClaim old, long since) {
        WorkloadOwner b = new WorkloadOwner("node-b", UUID.randomUUID().toString());
        long attempts = 0;
        while (true) {
            WorkloadClaimAttempt attempt = rig.claims().acquirePipeline(KillRig.SCOPE, b, old.topologyRevision(), LEASE.multipliedBy(100));
            if (attempt.acquired()) {
                long acquired = System.currentTimeMillis();
                rig.claims().advanceExecution(attempt.claim(), old.topologyRevision(), Set.of("node-b")).orElseThrow();
                return new long[] {acquired, System.currentTimeMillis(), attempts};
            }
            attempts++;
            if (System.currentTimeMillis() - since > 300_000) throw new IllegalStateException("no takeover in 300 s");
            try {
                Thread.sleep(50);
            } catch (InterruptedException stop) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(stop);
            }
        }
    }

    private static void record(String scenario, String mode, int trial, Map<String, ?> values) throws Exception {
        StringBuilder json = new StringBuilder("{\"scenario\":\"").append(scenario).append("\",\"mode\":\"").append(mode)
                .append("\",\"trial\":").append(trial);
        new TreeMap<>(values).forEach((key, value) -> json.append(",\"").append(key).append("\":").append(jsonValue(value)));
        json.append('}');
        System.out.println("KILL-NINE " + json);
        Files.createDirectories(OUT.getParent());
        Files.writeString(OUT, json + "\n", StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
    }

    private static String jsonValue(Object value) {
        if (value instanceof Number || value instanceof Boolean) return value.toString();
        if (value instanceof Map<?, ?> map) {
            StringBuilder out = new StringBuilder("{");
            map.forEach((key, inner) -> out.append(out.length() > 1 ? "," : "").append('"').append(key).append("\":").append(jsonValue(inner)));
            return out.append('}').toString();
        }
        if (value instanceof List<?> list) {
            StringBuilder out = new StringBuilder("[");
            list.forEach(inner -> out.append(out.length() > 1 ? "," : "").append(jsonValue(inner)));
            return out.append(']').toString();
        }
        return '"' + String.valueOf(value).replace("\\", "\\\\").replace("\"", "'") + '"';
    }

    /** Rows of one key range, counted by the group that last wrote them and by writer. */
    private record Histogram(TreeMap<Long, Integer> byGroup, TreeMap<String, Integer> byWriter) {
        static Histogram of(Map<String, String> rows, String prefix) {
            TreeMap<Long, Integer> byGroup = new TreeMap<>();
            TreeMap<String, Integer> byWriter = new TreeMap<>();
            rows.forEach((stored, value) -> {
                String[] parts = value.split("\\|", 5);
                String key = parts.length > 3 ? parts[3] : stored;
                if (!key.startsWith(prefix) || !Character.isDigit(key.charAt(prefix.length()))) return;
                byGroup.merge(Long.parseLong(parts[2]), 1, Integer::sum);
                byWriter.merge(parts[0] + "@" + parts[1] + "#" + parts[2], 1, Integer::sum);
            });
            return new Histogram(byGroup, byWriter);
        }

        long min() {
            return byGroup.isEmpty() ? -1 : byGroup.firstKey();
        }
    }

    /** One member process and every line it has printed, with the controller's clock at arrival. */
    private static final class Member {
        private final Process process;
        private final List<String> lines = new CopyOnWriteArrayList<>();

        private Member(Process process) {
            this.process = process;
            Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line = in.readLine(); line != null; line = in.readLine()) {
                        lines.add(line);
                        if (Boolean.getBoolean("kill.echo")) System.out.println("  [member] " + line);
                    }
                } catch (java.io.IOException closed) {
                    // the process is gone
                }
            });
            reader.setDaemon(true);
            reader.start();
        }

        static Member start(String url, String db, String mode, String member, String prefixes, WorkloadClaimFence fence,
                long from, long groups, boolean renews, String... extra) throws Exception {
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx512m",
                    "-cp", System.getProperty("java.class.path"),
                    "-Dtapstate.nest.direct.group-commit=" + mode,
                    "-Dkill.url=" + url, "-Dkill.db=" + db, "-Dkill.member=" + member, "-Dkill.prefixes=" + prefixes,
                    "-Dkill.keys=" + KEYS, "-Dkill.pad=" + PAD, "-Dkill.from=" + from, "-Dkill.groups=" + groups,
                    "-Dkill.owner.node=" + fence.owner().nodeId(), "-Dkill.owner.boot=" + fence.owner().bootId(),
                    "-Dkill.claim=" + fence.claimGeneration(), "-Dkill.execution=" + fence.executionGeneration(),
                    "-Dkill.topology=" + fence.topologyRevision()));
            if (renews) command.add("-Dkill.renew.ttl=" + LEASE.toMillis());
            command.addAll(List.of(extra));
            command.add(KillMember.class.getName());
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            return new Member(process);
        }

        void await(Predicate<String> condition, long timeoutMillis) throws InterruptedException {
            if (awaitAny(condition, timeoutMillis) == null) {
                throw new IllegalStateException("member never printed the expected line; last lines: "
                        + lines.subList(Math.max(0, lines.size() - 20), lines.size()));
            }
        }

        String awaitAny(Predicate<String> condition, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            int seen = 0;
            while (System.currentTimeMillis() < deadline) {
                for (; seen < lines.size(); seen++) if (condition.test(lines.get(seen))) return lines.get(seen);
                if (!process.isAlive() && seen >= lines.size()) {
                    Thread.sleep(100);
                    for (; seen < lines.size(); seen++) if (condition.test(lines.get(seen))) return lines.get(seen);
                    return null;
                }
                Thread.sleep(5);
            }
            return null;
        }

        String first(Predicate<String> condition) {
            return lines.stream().filter(condition).findFirst().orElse(null);
        }

        void kill() throws InterruptedException {
            process.destroyForcibly();
            process.waitFor();
            Thread.sleep(100);
        }

        void signal(String signal) throws Exception {
            new ProcessBuilder("kill", "-" + signal, Long.toString(process.pid())).inheritIO().start().waitFor();
        }

        void awaitExit(long timeoutMillis) throws InterruptedException {
            if (!process.waitFor(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("member did not finish; last lines: "
                        + lines.subList(Math.max(0, lines.size() - 20), lines.size()));
            }
            Thread.sleep(100);
        }

        String exit() {
            if (process.isAlive()) return "running";
            String refused = first(line -> line.startsWith("REFUSED "));
            return process.exitValue() + (refused == null ? "" : " " + refused);
        }

        long lastAck() {
            return lines.stream().filter(line -> line.startsWith("ACK ")).mapToLong(line -> Long.parseLong(line.split(" ")[1]))
                    .max().orElse(0);
        }

        long acksAfter(long millis) {
            return lines.stream().filter(line -> line.startsWith("ACK ")).filter(line -> Long.parseLong(line.split(" ")[2]) > millis).count();
        }

        List<Long> commitMillis() {
            Map<String, Long> begun = new java.util.HashMap<>();
            List<Long> out = new ArrayList<>();
            for (String line : lines) {
                String[] parts = line.split(" ");
                if (parts[0].equals("BEGIN")) begun.put(parts[1], Long.parseLong(parts[2]));
                if (parts[0].equals("ACK") && begun.containsKey(parts[1])) out.add(Long.parseLong(parts[2]) - begun.get(parts[1]));
            }
            return out;
        }

        long meanCommitMillis() {
            return (long) commitMillis().stream().mapToLong(Long::longValue).average().orElse(100);
        }
    }
}
