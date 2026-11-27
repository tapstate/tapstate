package io.tapstate.runtime.engine.nest;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.jet.datamodel.Tuple2;
import io.tapstate.core.common.TapstateException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** A root handover returns credit to its original dispatcher after both owners finish. */
final class RootMoveAcks {
    private RootMoveAcks() {}

    record Ticket(UUID member, UUID mailbox, long sequence) implements Serializable {}

    static final class Inbox implements AutoCloseable {
        private final UUID address = UUID.randomUUID();
        private final ConcurrentMap<Long, Integer> live = new ConcurrentHashMap<>();
        private final HazelcastInstance member;
        private long sequence;

        Inbox(HazelcastInstance member) {
            this.member = member;
            member.getUserContext().put(key(address), this);
        }

        Ticket reserve() {
            live.put(++sequence, 0);
            return new Ticket(member.getCluster().getLocalMember().getUuid(), address, sequence);
        }

        boolean take(Ticket ticket) {
            return live.remove(ticket.sequence(), 3);
        }

        @Override public void close() {
            member.getUserContext().remove(key(address), this);
            live.clear();
        }
    }

    static TapstateException unconfirmed(NestVertex vertex) {
        return new TapstateException(NestError.ROOT_HANDOVER_UNCONFIRMED,
                Map.of("node", vertex.name()), null);
    }

    static Object payload(Object item) {
        return item instanceof Tuple2<?, ?> mail && mail.f0() instanceof Ticket
                ? mail.f1() : item;
    }

    private static String key(UUID id) { return RootMoveAcks.class.getName() + "." + id; }

    static void acknowledge(HazelcastInstance member, NestVertex vertex, Ticket ticket, boolean departure) {
        var delivery = new Delivery(ticket, departure ? 1 : 2);
        Member receiver = member.getCluster().getMembers().stream()
                .filter(candidate -> candidate.getUuid().equals(ticket.member())).findFirst().orElse(null);
        if (receiver == null) throw unconfirmed(vertex);
        if (receiver.localMember()) {
            delivery.setHazelcastInstance(member);
            delivery.call();
        } else {
            // Only receipt at the dispatcher's live mailbox releases the keys. Its deadline also
            // covers lost deliveries. The assembler can keep processing unrelated documents.
            try {
                member.getExecutorService("tapstate-root-handover").submitToMember(delivery, receiver);
            } catch (com.hazelcast.core.HazelcastException | java.util.concurrent.RejectedExecutionException failure) {
                throw new TapstateException(NestError.ROOT_HANDOVER_UNCONFIRMED,
                        Map.of("node", vertex.name()), failure);
            }
        }
    }

    private static final class Delivery implements Callable<Boolean>, HazelcastInstanceAware, Serializable {
        private static final long serialVersionUID = 1L;
        private final Ticket ticket;
        private final int side;
        private transient HazelcastInstance member;

        Delivery(Ticket ticket, int side) {
            this.ticket = ticket;
            this.side = side;
        }

        @Override public void setHazelcastInstance(HazelcastInstance value) { member = value; }

        @Override public Boolean call() {
            if (member.getUserContext().get(key(ticket.mailbox())) instanceof Inbox inbox) {
                inbox.live.computeIfPresent(ticket.sequence(), (id, received) -> received | side);
                return true;
            }
            return false;
        }
    }
}
