package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import io.tapstate.core.common.TapstateException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/** Transient writer replies, addressed to the dispatcher that issued the command in this execution. */
final class SinkLaneAcks implements AutoCloseable {
    record Ticket(UUID member, UUID mailbox, long sequence) implements Serializable {}

    private final HazelcastInstance member;
    private final UUID mailbox = UUID.randomUUID();
    private final Map<Long, Boolean> replies = new ConcurrentHashMap<>();
    private long next;

    SinkLaneAcks(HazelcastInstance member) {
        this.member = member;
        member.getUserContext().put(key(mailbox), this);
    }

    Ticket reserve() {
        replies.put(++next, false);
        return new Ticket(member.getCluster().getLocalMember().getUuid(), mailbox, next);
    }

    boolean take(Ticket ticket) {
        return replies.remove(ticket.sequence(), true);
    }

    static void acknowledge(HazelcastInstance member, Ticket ticket) {
        var receiver = member.getCluster().getMembers().stream().filter(m -> m.getUuid().equals(ticket.member()))
                .findFirst().orElseThrow(SinkLaneAcks::unconfirmed);
        if (receiver.localMember()) deliver(member, ticket);
        else {
            try {
                member.getExecutorService("tapstate-sink-lanes").submitToMember(new Delivery(ticket), receiver);
            } catch (com.hazelcast.core.HazelcastException | java.util.concurrent.RejectedExecutionException error) {
                throw new TapstateException(EngineError.KEY_MOVE_WRITE_UNCONFIRMED,
                        Map.of("expected", 1, "accepted", "unknown"), error);
            }
        }
    }

    static TapstateException unconfirmed() {
        return new TapstateException(EngineError.KEY_MOVE_WRITE_UNCONFIRMED,
                Map.of("expected", 1, "accepted", "unknown"), null);
    }

    private static String key(UUID id) { return SinkLaneAcks.class.getName() + "." + id; }

    private static boolean deliver(HazelcastInstance member, Ticket ticket) {
        return member.getUserContext().get(key(ticket.mailbox())) instanceof SinkLaneAcks inbox
                && inbox.replies.replace(ticket.sequence(), true) != null;
    }

    @Override public void close() {
        member.getUserContext().remove(key(mailbox), this);
        replies.clear();
    }

    private static final class Delivery implements Callable<Boolean>, HazelcastInstanceAware, Serializable {
        private final Ticket ticket;
        private transient HazelcastInstance member;
        Delivery(Ticket ticket) { this.ticket = ticket; }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Boolean call() { return deliver(member, ticket); }
    }
}
