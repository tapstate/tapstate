package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/** One bounded local invocation. The host ledger owns the one-attempt budget and success deduplication. */
public final class SdkPlaceholderChannel implements NotificationChannel {
    private static final AlertChannelPolicy.Capability CAPABILITY = new AlertChannelPolicy.Capability("cloud-sdk", 1, AlertChannelPolicy.Family.CLOUD_SDK);
    private final TargetBinding binding;
    private final Consumer<Request> publish;
    private final Clock clock;
    private final ThreadPoolExecutor worker;
    public SdkPlaceholderChannel(AlertChannelPolicy.Deployment deployment, TargetBinding binding, Consumer<Request> publish, Clock clock) {
        this(deployment, binding, publish, clock, boundedWorker(deployment));
    }
    SdkPlaceholderChannel(AlertChannelPolicy.Deployment deployment, TargetBinding binding, Consumer<Request> publish, Clock clock, ThreadPoolExecutor worker) {
        AlertChannelPolicy.requireDeliverable(deployment, CAPABILITY);
        this.binding = Objects.requireNonNull(binding, "binding");
        this.publish = Objects.requireNonNull(publish, "publish");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.worker = Objects.requireNonNull(worker, "worker");
        if (worker.getMaximumPoolSize() != 1 || !(worker.getQueue() instanceof SynchronousQueue<?>)) {
            throw new IllegalArgumentException("local SDK worker must be bounded to one unqueued call");
        }
    }
    private static ThreadPoolExecutor boundedWorker(AlertChannelPolicy.Deployment deployment) {
        AlertChannelPolicy.requireDeliverable(deployment, CAPABILITY);
        return new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "tapstate-alert-sdk-placeholder"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    @Override public AlertChannelPolicy.Capability capability() { return CAPABILITY; }
    @Override public Result send(Request request) throws InterruptedException {
        if (request.purpose() != Purpose.BUSINESS || !binding.matches(request) || !request.destination().channelType().equals(typeId()) || !request.contentType().equals("application/json") || request.payload().length > 64 * 1024) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", "sdkPlaceholderRequest", "reason", "invalid-frozen-request"), null);
        }
        long nanos = remaining(request);
        if (nanos <= 0) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE); }
        final java.util.concurrent.atomic.AtomicBoolean returnedNormally = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.Future<?> future;
        try {
            future = worker.submit(() -> {
                if (!clock.instant().isBefore(request.deadline())) { return false; }
                publish.accept(request);
                returnedNormally.set(true);
                return true;
            });
        } catch (RejectedExecutionException ex) { return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY); }
        try {
            nanos = remaining(request);
            if (nanos <= 0) { future.cancel(true); return completion(returnedNormally.get()); }
            Object invoked = future.get(nanos, TimeUnit.NANOSECONDS);
            return result(returnedNormally.get() && Boolean.TRUE.equals(invoked) ? Outcome.ACCEPTED : Outcome.NOT_ACCEPTED, Action.STOP, Phase.LOCAL);
        } catch (TimeoutException ex) {
            future.cancel(true); return completion(returnedNormally.get());
        } catch (java.util.concurrent.CancellationException ex) {
            return completion(returnedNormally.get());
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof TapstateException) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.LOCAL); }
            if (cause instanceof RuntimeException bug) { throw bug; }
            if (cause instanceof Error bug) { throw bug; }
            throw new IllegalStateException("unexpected local publisher failure", cause);
        } catch (InterruptedException ex) { future.cancel(true); throw ex; }
    }
    public int activeCalls() { return worker.getActiveCount(); }
    @Override public void close() { worker.shutdownNow(); }
    private long remaining(Request request) {
        Duration value = Duration.between(clock.instant(), request.deadline());
        if (value.isNegative() || value.isZero()) { return 0; }
        if (value.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", "sdkPlaceholderDeadline", "reason", "over-five-minutes"), null);
        }
        return value.toNanos();
    }
    private static Result completion(boolean returnedNormally) {
        return result(returnedNormally ? Outcome.ACCEPTED : Outcome.UNKNOWN, Action.STOP, returnedNormally ? Phase.LOCAL : Phase.DEADLINE);
    }
    private static Result result(Outcome outcome, Action action, Phase phase) {
        return new Result(outcome, action, null, phase, 0, "sdk-placeholder");
    }
}
