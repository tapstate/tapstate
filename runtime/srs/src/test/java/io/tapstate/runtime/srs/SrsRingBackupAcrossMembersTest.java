package io.tapstate.runtime.srs;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.tapstate.runtime.srs.SrsClusterTestSupport.startMember;
import static org.assertj.core.api.Assertions.assertThat;

class SrsRingBackupAcrossMembersTest {

    @Test
    void abruptOwnerLossLeavesThePerChainPerTableRingOnItsSynchronousBackup() throws Exception {
        String cluster = "srs-backup-" + System.nanoTime();
        HazelcastInstance first = startMember(config(cluster));
        HazelcastInstance second = null;
        try {
            second = startMember(config(cluster), first);
            awaitMembers(first, 2, Duration.ofSeconds(10));
            String ringName = SrsRingbuffer.ringName("chain-a", "orders");
            long sequence = first.<SrsItem>getRingbuffer(ringName).add(new SrsItem(
                    new SourcePosition("binlog:12"), Op.INSERT, 1L, null, Map.of("id", 1), 0L));
            awaitSafe(first, Duration.ofSeconds(10));

            UUID owner = first.getPartitionService().getPartition(ringName).getOwner().getUuid();
            HazelcastInstance failed = first.getCluster().getLocalMember().getUuid().equals(owner) ? first : second;
            HazelcastInstance survivor = failed == first ? second : first;
            failed.getLifecycleService().terminate();

            awaitMembers(survivor, 1, Duration.ofSeconds(10));
            SrsItem recovered = awaitRead(survivor, ringName, sequence, Duration.ofSeconds(10));
            assertThat(recovered.srcPos()).isEqualTo(new SourcePosition("binlog:12"));
        } finally {
            if (first.getLifecycleService().isRunning()) {
                first.shutdown();
            }
            if (second != null && second.getLifecycleService().isRunning()) {
                second.shutdown();
            }
        }
    }

    private static Config config(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(SrsItem.class)
                .setImplementation(new SrsItemSerializer()));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(16)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(1));
        return config;
    }

    private static void awaitMembers(HazelcastInstance member, int expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (member.getCluster().getMembers().size() == expected) {
                return;
            }
            Thread.sleep(25);
        }
        assertThat(member.getCluster().getMembers()).hasSize(expected);
    }

    private static void awaitSafe(HazelcastInstance member, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (member.getPartitionService().isClusterSafe()) {
                return;
            }
            Thread.sleep(25);
        }
        assertThat(member.getPartitionService().isClusterSafe()).isTrue();
    }

    private static SrsItem awaitRead(
            HazelcastInstance member, String ringName, long sequence, Duration timeout) throws Exception {
        ExecutorService reader = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-srs-backup-reader");
            thread.setDaemon(true);
            return thread;
        });
        Future<SrsItem> read = reader.submit(
                () -> new SrsRingbuffer(member.getRingbuffer(ringName)).readOne(sequence));
        try {
            return read.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException missingBackup) {
            throw new AssertionError("the surviving member did not expose the backed-up ring item", missingBackup);
        } catch (ExecutionException failedRead) {
            if (failedRead.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw failedRead;
        } finally {
            read.cancel(true);
            reader.shutdownNow();
        }
    }
}
