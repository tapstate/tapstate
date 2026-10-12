package io.tapstate.runtime.srs;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import java.io.Serial;
import java.net.UnknownHostException;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Which member a source vertex's one instance runs on.
 *
 * <p>A source drains a member-local hand-off: the rows of an initial load, and on a source running with the
 * shared ring switched off every change it captures. The capture fills that hand-off on the member that
 * started it, and nothing carries it anywhere else. So the one instance has to run on that member, and the
 * engine does not know which one that is: left to itself it picks a member at random, and on a cluster the
 * pick is another member as often as not. The instance it starts there finds its hand-off empty, the rows
 * wait on the other member for a reader that never comes, and the job runs on with nothing thrown and
 * nothing crossing.
 *
 * <p>{@link #on(Address)} names the member. {@link #anyMember()} leaves the pick to the engine, which is
 * only right where there is one member to pick, and is written out at each place that relies on that
 * rather than being what a caller gets for saying nothing.
 */
public final class SourcePlacement {

    private static final SourcePlacement ANY_MEMBER = new SourcePlacement(null);

    private final Address member;

    private SourcePlacement(Address member) {
        this.member = member;
    }

    /** Wherever the engine puts it: right only when there is a single member to put it on. */
    public static SourcePlacement anyMember() {
        return ANY_MEMBER;
    }

    /** On {@code member}, which must be the member whose capture fills the hand-off the source drains. */
    public static SourcePlacement on(Address member) {
        return new SourcePlacement(Objects.requireNonNull(member, "member"));
    }

    /** {@code supplier}'s one instance, held to this placement. */
    ProcessorMetaSupplier place(ProcessorSupplier supplier) {
        return member == null
                ? ProcessorMetaSupplier.forceTotalParallelismOne(supplier)
                : new OnMember(supplier, member.getHost(), member.getPort());
    }

    @Override
    public String toString() {
        return member == null ? "any member" : "on " + member;
    }

    /**
     * One instance on the member at {@code host}:{@code port}, which is what the engine's own pinning does - carried
     * as the host and port rather than the engine's address.
     *
     * <p>That is the whole reason this exists. What the engine hands back for a member keeps the member's address,
     * and the address is written only by the engine's own serialization. The job carries a supplier the engine did
     * not make - one the source's changes wait behind, say - by plain Java serialization, and that takes everything
     * the supplier holds with it: an address inside it fails the submission of the whole job. The host and the port
     * travel anywhere, and the pinning is made again from them where the job runs.
     */
    private static final class OnMember implements ProcessorMetaSupplier {

        @Serial
        private static final long serialVersionUID = 1L;

        private final ProcessorSupplier supplier;
        private final String host;
        private final int port;
        private transient ProcessorMetaSupplier pinned;

        private OnMember(ProcessorSupplier supplier, String host, int port) {
            this.supplier = supplier;
            this.host = host;
            this.port = port;
        }

        private ProcessorMetaSupplier pinned() {
            if (pinned == null) {
                try {
                    pinned = ProcessorMetaSupplier.forceTotalParallelismOne(supplier, new Address(host, port));
                } catch (UnknownHostException e) {
                    throw new IllegalStateException("the member a source is placed on no longer resolves: "
                            + host + ":" + port, e);
                }
            }
            return pinned;
        }

        @Override
        public int preferredLocalParallelism() {
            return pinned().preferredLocalParallelism();
        }

        @Override
        public void init(Context context) throws Exception {
            pinned().init(context);
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            return pinned().get(addresses);
        }

        @Override
        public void close(Throwable error) throws Exception {
            pinned().close(error);
        }

        @Override
        public boolean isReusable() {
            return pinned().isReusable();
        }

        @Override
        public boolean initIsCooperative() {
            return pinned().initIsCooperative();
        }

        @Override
        public boolean closeIsCooperative() {
            return pinned().closeIsCooperative();
        }

        @Override
        public Permission getRequiredPermission() {
            return pinned().getRequiredPermission();
        }

        @Override
        public Map<String, String> getTags() {
            return pinned().getTags();
        }
    }
}
