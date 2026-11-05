package io.tapstate.e2e;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/** One lexical store owner; no process is borrowed from another case or found by PID. */
final class StoreProcessScope {
    private static final ThreadLocal<Deque<StoreProcessScope>> ACTIVE =
            ThreadLocal.withInitial(ArrayDeque::new);

    private final Thread owner = Thread.currentThread();
    private final Set<String> hostUris = new HashSet<>();
    private final List<Process> launched = new ArrayList<>();
    private final IdentityHashMap<Process, Boolean> known = new IdentityHashMap<>();
    private final IdentityHashMap<Process, BootOutput> outputs = new IdentityHashMap<>();
    private boolean sealed;
    private boolean cleaned;

    private StoreProcessScope() { }

    static StoreProcessScope open() {
        StoreProcessScope scope = new StoreProcessScope();
        ACTIVE.get().push(scope);
        return scope;
    }

    /** Binds exactly an address this owner actually returned; aliases are never guessed. */
    String rememberHostUri(String uri) {
        requireOwner();
        if (sealed || !ACTIVE.get().contains(this)) {
            throw new IllegalStateException("the store process scope is no longer open");
        }
        hostUris.add(uri);
        return uri;
    }

    static StoreProcessScope matching(String uri) {
        Deque<StoreProcessScope> active = ACTIVE.get();
        if (active.isEmpty()) {
            ACTIVE.remove();
            return null;
        }
        StoreProcessScope found = null;
        for (StoreProcessScope scope : active) {
            if (!scope.hostUris.contains(uri)) continue;
            if (found != null) {
                throw new IllegalStateException("a store URI has more than one lexical owner");
            }
            found = scope;
        }
        return found;
    }

    /** A prepared launch cannot create a child after its store owner's closing began. */
    void beforeLaunch() {
        requireOwner();
        if (sealed) throw new IllegalStateException("the store process scope is no longer open");
    }

    /** Called only after the launch plan owns the actual Process returned by ProcessBuilder. */
    void launched(Process process) {
        beforeLaunch();
        if (known.put(process, Boolean.TRUE) == null) launched.add(process);
    }

    /** Retain the handle before any metadata read can fail. Output already belongs to this launch. */
    void launched(Process process, java.nio.file.Path output, String token) {
        launched(process);
        var binding = new BootOutput(output,token,process.pid());
        outputs.put(process,binding);
        binding.started = process.info().startInstant().orElseThrow(
                () -> new AssertionError("the original child process start time is unavailable"));
    }

    private static final class BootOutput {
        final java.nio.file.Path output;final String token;final long pid;
        java.time.Instant started;
        BootOutput(java.nio.file.Path output,String token,long pid){this.output=output;this.token=token;this.pid=pid;}
    }

    List<String> deadBootIds() {
        requireOwner();
        var boots = new HashSet<String>();
        for(var process:launched){
            if(process.isAlive())throw new AssertionError("an owned client process is still alive");
            process.exitValue();
            var original=outputs.get(process);
            if(original==null || original.started==null)throw new AssertionError("the original process boot attribution is unavailable");
            try {boots.add(bootFromOutput(java.nio.file.Files.readString(original.output),original.token,original.pid,original.started));}
            catch(java.io.IOException failure){throw new java.io.UncheckedIOException(failure);}
        }
        return List.copyOf(boots);
    }

    static String bootFromOutput(String output,String token,long pid,java.time.Instant started) {
        var pattern=java.util.regex.Pattern.compile("\\[launch="+java.util.regex.Pattern.quote(token)
                +"\\][^\\r\\n]*?Process boot identity \\[boot_id=([0-9a-f-]{36}), pid=([0-9]+), started_at=([^\\]]+)\\]");
        var found=new HashSet<String>();var matches=pattern.matcher(output);
        while(matches.find()){
            String boot=matches.group(1);
            if(!java.util.UUID.fromString(boot).toString().equals(boot) || Long.parseLong(matches.group(2))!=pid
                    || !started.equals(java.time.Instant.parse(matches.group(3))))
                throw new AssertionError("the child boot output does not match its original process");
            found.add(boot);
        }
        if(found.size()!=1)throw new AssertionError("the original process must have exactly one attributed boot identity");
        return found.iterator().next();
    }

    /** Refusal detaches the lexical scope, but this instance can retry after its children retire. */
    void closeWith(Runnable storeCleanup) {
        requireOwner();
        if (cleaned) return;
        if (!sealed) {
            Deque<StoreProcessScope> active = ACTIVE.get();
            if (active.peek() != this) {
                throw new IllegalStateException("store process scopes must close in lexical order");
            }
            active.pop();
            sealed = true;
            if (active.isEmpty()) ACTIVE.remove();
        }
        List<Throwable> unknown = new ArrayList<>();
        for (Process process : launched) {
            try {
                if (process.isAlive()) {
                    unknown.add(new AssertionError("one owned store client process is still alive"));
                } else {
                    // A cleanup exception is not an exit result; ask the same Process independently.
                    process.exitValue();
                }
            } catch (RuntimeException | Error failure) {
                unknown.add(failure);
            }
        }
        if (!unknown.isEmpty()) {
            AssertionError retained = new AssertionError(
                    "the owned store is retained because not every client process has a confirmed exit");
            unknown.forEach(retained::addSuppressed);
            throw retained;
        }
        storeCleanup.run();
        cleaned = true;
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("the store process scope belongs to another thread");
        }
    }
}
