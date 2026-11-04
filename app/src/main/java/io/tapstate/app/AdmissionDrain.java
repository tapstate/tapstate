package io.tapstate.app;

import java.util.concurrent.TimeUnit;

/** Tracks admitted calls independently of monitors that those calls may hold during store IO. */
final class AdmissionDrain {
    private volatile boolean stopped;
    private int running;
    private final ThreadLocal<Integer> local = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<Long> deadline = new ThreadLocal<>();

    synchronized boolean enter() {
        if (stopped) return false;
        if(local.get()==0)deadline.set(ShutdownAwait.deadline());
        running++;local.set(local.get()+1);return true;
    }
    synchronized void leave() {
        running--;int remaining=local.get()-1;
        if(remaining==0){local.remove();deadline.remove();}else local.set(remaining);
        notifyAll();
    }
    synchronized void stop() { stopped=true;notifyAll(); }
    boolean current() { Long limit=deadline.get();return !stopped&&limit!=null&&limit-System.nanoTime()>0; }
    synchronized void await(long deadline) {
        if(local.get()>0)throw StorageRuntime.unavailable();
        while(running!=0) {
            try { TimeUnit.NANOSECONDS.timedWait(this,ShutdownAwait.remaining(deadline)); }
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw StorageRuntime.unavailable();}
        }
    }
}
