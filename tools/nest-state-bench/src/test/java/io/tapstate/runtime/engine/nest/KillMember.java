package io.tapstate.runtime.engine.nest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.KillRig;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * One cluster member's nest vertex, as a process of its own: it owns a range of root keys and commits
 * them through {@link DirectNestStore} in groups, against the production transactional store, fenced by
 * the claim it is handed. The controller kills it (SIGKILL) or freezes it (SIGSTOP) part way through a
 * group, then reads the store to see what that group left behind.
 *
 * <p>Every group rewrites every key the member owns with a value naming the member, its execution
 * generation and the group number, so a reader can tell from the store alone whether a group landed
 * whole, in part, or not at all. Output lines, one per event, read by the controller:
 * {@code BEGIN g millis}, {@code ACK g millis}, {@code REFUSED g millis reason}, {@code READ ...}.
 *
 * <p>Configured by system properties: {@code kill.url}, {@code kill.db}, {@code kill.member},
 * {@code kill.prefixes} (comma-separated key ranges it owns), {@code kill.keys} (keys per range),
 * {@code kill.pad} (value bytes), {@code kill.from} (first group number), {@code kill.groups} (how many,
 * 0 = until killed), {@code kill.pause} (millis between groups), {@code kill.owner.node},
 * {@code kill.owner.boot}, {@code kill.claim}, {@code kill.execution}, {@code kill.topology}, and
 * {@code kill.renew.ttl} (millis; when set this process renews the claim, as the claim owner's node does).
 */
public final class KillMember {

    private KillMember() {
    }

    public static void main(String[] args) throws Exception {
        String url = System.getProperty("kill.url");
        String db = System.getProperty("kill.db");
        String member = System.getProperty("kill.member");
        List<String> prefixes = List.of(System.getProperty("kill.prefixes").split(","));
        int keys = Integer.getInteger("kill.keys", 1000);
        int pad = Integer.getInteger("kill.pad", 1024);
        long from = Long.getLong("kill.from", 1L);
        long groups = Long.getLong("kill.groups", 0L);
        long pause = Long.getLong("kill.pause", 0L);
        WorkloadClaimFence fence = new WorkloadClaimFence(KillRig.KEY,
                new WorkloadOwner(System.getProperty("kill.owner.node"), System.getProperty("kill.owner.boot")),
                Long.getLong("kill.claim"), Long.getLong("kill.execution"), Long.getLong("kill.topology"));
        long renewTtl = Long.getLong("kill.renew.ttl", 0L);

        try (KillRig rig = KillRig.attach(url, db); MongoClient client = registering(rig, member, url)) {
            if (renewTtl > 0) renewer(rig, fence, Duration.ofMillis(renewTtl)).start();
            NestStore<String> store = DirectNestStore.over(rig.store(client, fence), rig.mapName(), fence.executionGeneration());
            List<Object> owned = new ArrayList<>();
            for (String prefix : prefixes) for (int i = 0; i < keys; i++) owned.add(prefix + i);

            if (Boolean.getBoolean("kill.read")) read(store, owned);
            String padding = "x".repeat(pad);
            for (long g = from; groups == 0 || g < from + groups; g++) {
                for (Object key : owned) {
                    store.save(key, member + "|" + fence.executionGeneration() + "|" + g + "|" + key + "|" + padding);
                }
                say("BEGIN " + g + " " + System.currentTimeMillis());
                try {
                    store.commit();
                } catch (RuntimeException refused) {
                    say("REFUSED " + g + " " + System.currentTimeMillis() + " " + reason(refused));
                    System.exit(3);
                }
                say("ACK " + g + " " + System.currentTimeMillis());
                if (pause > 0) Thread.sleep(pause);
            }
        }
        System.exit(0);
    }

    /**
     * The member's client, which registers every server session it sees before that session's first
     * command is sent, so nothing can be left open under a session the takeover does not know about.
     */
    private static MongoClient registering(KillRig rig, String member, String url) {
        java.util.Set<org.bson.BsonDocument> known = java.util.concurrent.ConcurrentHashMap.newKeySet();
        com.mongodb.event.CommandListener listener = new com.mongodb.event.CommandListener() {
            @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                org.bson.BsonDocument lsid = event.getCommand().getDocument("lsid", null);
                if (lsid != null && known.add(lsid)) rig.registerSession(member, lsid);
            }
        };
        return MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(url)).addCommandListener(listener).build());
    }

    /** What a new owner finds for its keys on its first read, through the fenced store (read concern snapshot). */
    private static void read(NestStore<String> store, Collection<Object> owned) {
        Map<Object, String> loaded = store.loadAll(owned);
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        java.util.TreeMap<String, Integer> byWriter = new java.util.TreeMap<>();
        for (String value : loaded.values()) {
            String[] parts = value.split("\\|", 5);
            long g = Long.parseLong(parts[2]);
            min = Math.min(min, g);
            max = Math.max(max, g);
            byWriter.merge(parts[0] + "@" + parts[1] + "#" + parts[2], 1, Integer::sum);
        }
        say("READ found=" + loaded.size() + " of=" + owned.size() + " min=" + min + " max=" + max + " by=" + byWriter);
    }

    private static Thread renewer(KillRig rig, WorkloadClaimFence fence, Duration ttl) {
        Thread renew = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(Math.max(100, ttl.toMillis() / 3));
                    WorkloadClaim current = rig.current();
                    if (current.claimGeneration() != fence.claimGeneration()) return;
                    rig.claims().renew(current, ttl);
                } catch (InterruptedException stopped) {
                    return;
                } catch (RuntimeException failed) {
                    say("RENEW-FAILED " + System.currentTimeMillis() + " " + reason(failed));
                }
            }
        }, "claim-renewer");
        renew.setDaemon(true);
        return renew;
    }

    private static String reason(Throwable failure) {
        StringBuilder out = new StringBuilder();
        for (Throwable at = failure; at != null; at = at.getCause()) {
            if (out.length() > 0) out.append(" <- ");
            out.append(at.getClass().getSimpleName());
            if (at instanceof io.tapstate.core.common.TapstateException coded) out.append('[').append(coded.code().code()).append(']');
            else if (at.getMessage() != null) out.append(": ").append(at.getMessage().replace('\n', ' '));
        }
        return out.toString();
    }

    private static synchronized void say(String line) {
        System.out.println(line);
        System.out.flush();
    }
}
