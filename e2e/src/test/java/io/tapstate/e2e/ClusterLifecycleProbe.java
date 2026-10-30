package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/** A scoped observer of fixture actions; events carry hashes and owned identities only. */
final class ClusterLifecycleProbe {
    enum Phase { PREPARED, LAUNCHED, HEALTH_STARTED, HEALTH_PASSED, READY_STARTED, READY_PASSED,
        QUIT_REQUESTED, FORCE_REQUESTED, EXIT_CONFIRMED }

    record Event(Phase phase, String launch, String cluster, String node,
            int httpPort, int memberPort, int ownedLinkPort,
            long pid, String work, String output, String staging, String parameters) { }

    record Facts(Scope owner, String launch, String cluster, String node,
            int httpPort, int memberPort, int ownedLinkPort,
            String work, String output, String staging, String parameters) { }

    private static final ThreadLocal<Scope> ACTIVE = new ThreadLocal<>();

    private ClusterLifecycleProbe() { }

    static Scope install(Consumer<Event> listener) {
        Scope scope = new Scope(Thread.currentThread(), ACTIVE.get(), listener);
        ACTIVE.set(scope);
        return scope;
    }

    static Facts facts(int httpPort, Path work, Path output, Path staging, String storeUri,
            String operatorDatabase, String listenAddress, List<String> jvmOptions, List<String> extraArguments) {
        Scope scope = ACTIVE.get();
        if (scope == null) return null;
        try {
            List<String> parameters = new ArrayList<>(List.of(storeUri, operatorDatabase == null ? "<native-config>" : operatorDatabase, listenAddress));
            parameters.addAll(jvmOptions);
            parameters.addAll(extraArguments);
            String cluster = setting(extraArguments, "--tapstate.cluster.id=");
            String node = setting(extraArguments, "--tapstate.cluster.node-id=");
            String member = setting(extraArguments, "--tapstate.hz.member-port=");
            String link = setting(extraArguments, "--tapstate.hz.advertised-member-address=");
            // Only the owned link's numeric port leaves this setting, never its address.
            return new Facts(scope, UUID.randomUUID().toString(), hash(cluster), node, httpPort,
                    member.isEmpty() ? -1 : Integer.parseInt(member),
                    ownedLinkPort(link),
                    pathHash(work), pathHash(output),
                    pathHash(staging), hash(parameters));
        } catch (RuntimeException | Error failure) {
            scope.failed(failure);
            return null;
        }
    }

    static void record(Phase phase, Facts facts, long pid) {
        if (facts == null) return;
        Scope active = ACTIVE.get();
        if (active != facts.owner()) {
            facts.owner().failed(new IllegalStateException("fixture events left their owning probe scope"));
            return;
        }
        Event event = new Event(phase, facts.launch(), facts.cluster(), facts.node(),
                facts.httpPort(), facts.memberPort(), facts.ownedLinkPort(), pid, facts.work(), facts.output(),
                facts.staging(), facts.parameters());
        try { active.listener.accept(event); }
        catch (RuntimeException | Error failure) { active.failed(failure); }
    }

    static String hash(String value) {
        return hash(List.of(value));
    }

    private static String pathHash(Path path) {
        return hash(path.toAbsolutePath().normalize().toString());
    }

    private static String setting(List<String> arguments, String prefix) {
        return arguments.stream().filter(argument -> argument.startsWith(prefix))
                .map(argument -> argument.substring(prefix.length())).findFirst().orElse("");
    }

    private static int ownedLinkPort(String advertised) {
        if (advertised.isEmpty()) return -1;
        try {
            int port = Integer.parseInt(advertised.substring(advertised.lastIndexOf(':') + 1));
            if (port < 1 || port > 65535) throw new NumberFormatException();
            return port;
        } catch (NumberFormatException failure) {
            // This observation failure must not retain a raw command parameter in its cause.
            throw new IllegalArgumentException("the fixture link port is not observable");
        }
    }

    private static String hash(List<String> values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                digest.update(new byte[] {(byte) (bytes.length >>> 24), (byte) (bytes.length >>> 16),
                        (byte) (bytes.length >>> 8), (byte) bytes.length});
                digest.update(bytes);
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }

    static final class Scope implements AutoCloseable {
        private final Thread owner;
        private final Scope previous;
        private final Consumer<Event> listener;
        private final List<Throwable> failures = new ArrayList<>();
        private boolean closed;

        private Scope(Thread owner, Scope previous, Consumer<Event> listener) {
            this.owner = owner;
            this.previous = previous;
            this.listener = java.util.Objects.requireNonNull(listener, "listener");
        }

        private synchronized void failed(Throwable failure) {
            failures.add(failure);
        }

        @Override
        public synchronized void close() {
            if (Thread.currentThread() != owner) {
                throw new IllegalStateException("only the installing thread may close the fixture probe");
            }
            if (closed) return;
            if (ACTIVE.get() != this) {
                throw new IllegalStateException("fixture probe scopes must close in reverse order");
            }
            closed = true;
            if (previous == null) ACTIVE.remove();
            else ACTIVE.set(previous);
            if (!failures.isEmpty()) {
                Throwable first = failures.getFirst();
                failures.stream().skip(1).filter(failure -> failure != first).forEach(first::addSuppressed);
                if (first instanceof RuntimeException failure) throw failure;
                throw (Error) first;
            }
        }
    }
}
