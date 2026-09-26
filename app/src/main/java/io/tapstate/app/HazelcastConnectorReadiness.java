package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MemberLeftException;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.common.TapstateException;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Asks every data member of the cluster, at once, to load a pipeline's sink connectors the way its sinks will:
 * through the connector provisioner bound on that member, which finds the connector's registration in the shared
 * registry and stages the artifact into the member's own plugins directory. Each member answers with the content
 * hash it loaded, or with why it could not.
 *
 * <p>A member that cannot load one, answers nothing in time, or leaves before answering refuses the start, named
 * by its stable id; so do members that load different artifacts for one connector. Loading stages and inspects the
 * artifact but opens no connection, so asking has no effect on any target.
 *
 * <p>The members asked are the data members at the time of asking. One joining or leaving between this and the
 * run's submission is caught where the run starts, which refuses to run on a member count other than its plan's.
 */
final class HazelcastConnectorReadiness implements ConnectorReadiness {

    /** The executor the question is asked through, on every member. */
    static final String EXECUTOR = "tapstate.connector-readiness";

    private final HazelcastInstance member;
    private final Duration timeout;

    HazelcastConnectorReadiness(HazelcastInstance member, Duration timeout) {
        this.member = Objects.requireNonNull(member, "member");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public Set<String> requireEveryMemberCanLoad(String pipelineId, Set<String> connectors) {
        if (connectors.isEmpty()) {
            return Set.of();
        }
        List<String> asked = List.copyOf(new TreeSet<>(connectors));
        Map<String, Member> dataMembers = new TreeMap<>();
        for (Member candidate : member.getCluster().getMembers()) {
            if (!candidate.isLiteMember()) {
                dataMembers.put(ClusterMembershipGate.stableIdOf(candidate), candidate);
            }
        }
        Map<Member, Future<List<Loaded>>> answers =
                member.getExecutorService(EXECUTOR).submitToMembers(new LoadConnectors(asked), dataMembers.values());
        long deadline = System.nanoTime() + timeout.toNanos();
        Map<String, Map<String, String>> loadedBy = new TreeMap<>();
        Set<String> shared = new TreeSet<>(asked);
        for (Map.Entry<String, Member> asking : dataMembers.entrySet()) {
            String memberId = asking.getKey();
            for (Loaded loaded : answerOf(pipelineId, memberId, asked, answers.get(asking.getValue()), deadline)) {
                if (loaded.failure() != null) {
                    throw unavailable(pipelineId, memberId, loaded.connector(), loaded.failure());
                }
                loadedBy.computeIfAbsent(loaded.connector(), ignored -> new TreeMap<>())
                        .put(memberId, loaded.contentHash());
                if (!loaded.shareSafe()) {
                    shared.remove(loaded.connector());
                }
            }
        }
        loadedBy.forEach((connector, hashByMember) -> {
            if (Set.copyOf(hashByMember.values()).size() > 1) {
                throw new TapstateException(ActuationError.CONNECTOR_DIFFERS_ACROSS_MEMBERS, Map.of(
                        "pipeline", pipelineId, "connector", connector,
                        "artifacts", hashByMember.entrySet().stream()
                                .map(loaded -> loaded.getKey() + "=" + loaded.getValue())
                                .collect(Collectors.joining(", "))), null);
            }
        });
        return Set.copyOf(shared);
    }

    /**
     * What {@code memberId} answered for every connector asked, waiting no later than {@code deadline}; a member
     * that answers nothing by then, leaves first, or fails to run the question refuses the start for all of them.
     */
    private List<Loaded> answerOf(String pipelineId, String memberId, List<String> asked,
            Future<List<Loaded>> answer, long deadline) {
        String connector = String.join(", ", asked);
        try {
            List<Loaded> loaded = answer.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            for (String expected : asked) {
                if (loaded.stream().noneMatch(one -> one.connector().equals(expected))) {
                    throw unavailable(pipelineId, memberId, expected, "the member did not answer for it");
                }
            }
            return loaded;
        } catch (TimeoutException slow) {
            answer.cancel(true);
            return List.of(Loaded.failed(connector, "no answer within " + timeout.toMillis() + "ms"));
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause() == null ? failed : failed.getCause();
            return List.of(Loaded.failed(connector, cause instanceof MemberLeftException
                    ? "the member left the cluster before answering"
                    : LoadConnectors.reasonOf(cause)));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return List.of(Loaded.failed(connector, "the start was interrupted while waiting for the member"));
        }
    }

    private static TapstateException unavailable(String pipelineId, String memberId, String connector, String why) {
        return new TapstateException(ActuationError.CONNECTOR_UNAVAILABLE_ON_MEMBER,
                Map.of("pipeline", pipelineId, "member", memberId, "connector", connector, "reason", why), null);
    }

    /**
     * One connector as a member loaded it: the content hash of the artifact it staged and whether that artifact is
     * certified to be shared by the writers on one member, or why it could not load it. Plain text rather than an
     * exception, because an error code does not cross members as an enum.
     */
    record Loaded(String connector, String contentHash, boolean shareSafe, String failure) implements Serializable {

        private static final long serialVersionUID = 1L;

        static Loaded of(ConnectorRef loaded, String connector) {
            return new Loaded(connector, loaded.contentHash(), loaded.shareSafe(), null);
        }

        static Loaded failed(String connector, String failure) {
            return new Loaded(connector, null, false, failure);
        }
    }

    /**
     * The question as it runs on each member: load every connector through the provisioner that member's sinks
     * load theirs through, and say what came of each.
     */
    static final class LoadConnectors implements Callable<List<Loaded>>, Serializable, HazelcastInstanceAware {

        private static final long serialVersionUID = 1L;

        private final List<String> connectors;
        private transient HazelcastInstance member;

        LoadConnectors(List<String> connectors) {
            this.connectors = List.copyOf(connectors);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance member) {
            this.member = member;
        }

        @Override
        public List<Loaded> call() {
            Object bound = member.getUserContext().get(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY);
            List<Loaded> loaded = new ArrayList<>();
            for (String connector : connectors) {
                if (!(bound instanceof ConnectorProvisioner provisioner)) {
                    loaded.add(Loaded.failed(connector,
                            "the member has no connector registry to load connectors from"));
                    continue;
                }
                try {
                    loaded.add(Loaded.of(provisioner.resolve(connector), connector));
                } catch (RuntimeException failed) {
                    loaded.add(Loaded.failed(connector, reasonOf(failed)));
                }
            }
            return loaded;
        }

        /**
         * A failure as one line of text: a coded refusal by its code and arguments, anything else by its type and
         * message, and the type and message of what it was caused by where there is a cause.
         */
        static String reasonOf(Throwable failure) {
            if (failure instanceof TapstateException coded) {
                return coded.getMessage();
            }
            String reason = failure.getClass().getSimpleName() + ": " + failure.getMessage();
            Throwable cause = failure.getCause();
            return cause == null || cause == failure ? reason
                    : reason + " (" + cause.getClass().getSimpleName() + ": " + cause.getMessage() + ")";
        }
    }
}
