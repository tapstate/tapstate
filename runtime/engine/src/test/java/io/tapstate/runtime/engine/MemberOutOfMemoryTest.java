package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What is written down about a member its own out-of-memory handling shuts down, and who is told.
 *
 * <p>Every member here is taken down, or left up, by the substrate's own handling: the error escapes one of its
 * threads and nothing in these cases shuts a member down directly, except the one case that is about a member
 * shut down on purpose. Whether a member is taken down stays the substrate's call. What is under test is only
 * what is written about it afterwards, and who hears.
 */
@DisplayName("what is written down about a member lost to out-of-memory, and who is told")
class MemberOutOfMemoryTest {

    private final List<HazelcastInstance> members = new ArrayList<>();

    @AfterEach
    void shutDownTheMembers() {
        members.forEach(HazelcastInstance::shutdown);
    }

    @Test
    void aWatchedMemberTheHandlingShutsDownIsWrittenDownAndItsWatcherIsToldOnce() {
        HazelcastInstance member = startMember();
        AtomicInteger told = new AtomicInteger();
        MemberOutOfMemory.watch(member, told::incrementAndGet);
        assertThat(MemberOutOfMemory.of(member)).as("a member that is still running has lost nothing").isEmpty();

        OutOfMemoryError error = OutOfMemoryOnAMemberThread.raise();

        assertThat(member.getLifecycleService().isRunning())
                .as("the substrate's own handling took the member down")
                .isFalse();
        assertThat(MemberOutOfMemory.of(member)).as("the error it was taken down over").containsSame(error);
        assertThat(told).as("its watcher hears of it once").hasValue(1);
    }

    /**
     * The handling is handed every member the process runs, and it takes down the ones nobody watches exactly
     * as it did before anything watched it. Only the watched member has anything written about it, and only its
     * watcher is told.
     */
    @Test
    void aMemberNobodyWatchesIsTakenDownAsBeforeAndNothingIsWrittenAboutIt() {
        HazelcastInstance watched = startMember();
        HazelcastInstance unwatched = startMember();
        AtomicInteger told = new AtomicInteger();
        MemberOutOfMemory.watch(watched, told::incrementAndGet);

        OutOfMemoryError error = OutOfMemoryOnAMemberThread.raise();

        assertThat(unwatched.getLifecycleService().isRunning())
                .as("whether a member is taken down is not up to its watcher")
                .isFalse();
        assertThat(MemberOutOfMemory.of(unwatched)).as("nothing is written about a member nobody watches").isEmpty();
        assertThat(MemberOutOfMemory.of(watched)).containsSame(error);
        assertThat(told).as("one watcher, told about its own member only").hasValue(1);
    }

    /**
     * The server shutting its member down on purpose is the everyday way a member stops, and it must not read as
     * one lost to memory: the engine fails every pipeline such a member carried.
     */
    @Test
    void aMemberShutDownOnPurposeWasNotLostToOutOfMemory() {
        HazelcastInstance member = startMember();
        AtomicInteger told = new AtomicInteger();
        MemberOutOfMemory.watch(member, told::incrementAndGet);

        member.shutdown();

        assertThat(MemberOutOfMemory.of(member)).isEmpty();
        assertThat(told).as("nobody is told about a shutdown that was asked for").hasValue(0);
    }

    private HazelcastInstance startMember() {
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        members.add(member);
        return member;
    }
}
