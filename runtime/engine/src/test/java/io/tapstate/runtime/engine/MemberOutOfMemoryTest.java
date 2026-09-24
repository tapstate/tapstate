package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What is written down about a member its own out-of-memory handling shuts down.
 *
 * <p>Every member here is taken down, or left up, by the substrate's own handling: the error escapes one of its
 * threads and nothing in these cases shuts a member down directly, except the one case that is about a member
 * shut down on purpose. Whether a member is taken down stays the substrate's call. What is under test is only
 * what is written about it afterwards.
 */
@DisplayName("what is written down about a member lost to out-of-memory")
class MemberOutOfMemoryTest {

    private final List<HazelcastInstance> members = new ArrayList<>();

    @AfterEach
    void shutDownTheMembers() {
        members.forEach(HazelcastInstance::shutdown);
    }

    @Test
    void aWatchedMemberTheHandlingShutsDownIsWrittenDown() {
        HazelcastInstance member = startMember();
        MemberOutOfMemory.watch(member);
        assertThat(MemberOutOfMemory.of(member)).as("a member that is still running has lost nothing").isEmpty();

        OutOfMemoryError error = OutOfMemoryOnAMemberThread.raise(member);

        assertThat(member.getLifecycleService().isRunning())
                .as("the substrate's own handling took the member down")
                .isFalse();
        assertThat(MemberOutOfMemory.of(member)).as("the error it was taken down over").containsSame(error);
    }

    /**
     * The handling is handed every member the process runs, once, and what is written is per member: no member's
     * record stands in for another's, and none keeps another's from being written.
     */
    @Test
    void everyWatchedMemberTheHandlingShutsDownIsWrittenDown() {
        HazelcastInstance first = startMember();
        HazelcastInstance second = startMember();
        MemberOutOfMemory.watch(first);
        MemberOutOfMemory.watch(second);

        OutOfMemoryError error = OutOfMemoryOnAMemberThread.raise(first, second);

        assertThat(MemberOutOfMemory.of(first)).containsSame(error);
        assertThat(MemberOutOfMemory.of(second)).containsSame(error);
    }

    /**
     * The handling is handed every member the process runs, and it takes down the ones nobody watches exactly
     * as it did before anything watched it. Only the watched member has anything written about it.
     */
    @Test
    void aMemberNobodyWatchesIsTakenDownAsBeforeAndNothingIsWrittenAboutIt() {
        HazelcastInstance watched = startMember();
        HazelcastInstance unwatched = startMember();
        MemberOutOfMemory.watch(watched);

        OutOfMemoryError error = OutOfMemoryOnAMemberThread.raise(watched, unwatched);

        assertThat(unwatched.getLifecycleService().isRunning())
                .as("whether a member is taken down is not up to whoever watches it")
                .isFalse();
        assertThat(MemberOutOfMemory.of(unwatched)).as("nothing is written about a member nobody watches").isEmpty();
        assertThat(MemberOutOfMemory.of(watched)).containsSame(error);
    }

    /**
     * The server shutting its member down on purpose is the everyday way a member stops, and it must not read as
     * one lost to memory: the engine fails every pipeline such a member carried, and the process reports itself
     * broken.
     */
    @Test
    void aMemberShutDownOnPurposeWasNotLostToOutOfMemory() {
        HazelcastInstance member = startMember();
        MemberOutOfMemory.watch(member);

        member.shutdown();

        assertThat(MemberOutOfMemory.of(member)).isEmpty();
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
