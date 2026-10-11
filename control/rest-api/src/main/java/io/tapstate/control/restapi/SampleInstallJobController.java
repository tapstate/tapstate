package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.SampleSourceService;
import io.tapstate.core.common.TapstateException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Installs selected samples in the background and exposes actual stage transitions. */
@RestController
final class SampleInstallJobController implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SampleInstallJobController.class);
    private static final long JOB_RETENTION_MS = 24L * 60 * 60 * 1000;
    private static final int MAX_RETAINED_JOBS = 64;
    record Request(List<String> ids) { }
    record Item(String id, String stage, String errorCode, String errorDetail) { }
    record Status(String id, List<Item> items, boolean finished) { }

    private final SampleSourceService samples;
    private final Map<String, Job> jobs = new LinkedHashMap<>();
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(16), Thread.ofVirtual().name("sample-install-", 0).factory(),
            new ThreadPoolExecutor.AbortPolicy());
    private boolean closed;

    SampleInstallJobController(SampleSourceService samples) {
        this.samples = Objects.requireNonNull(samples, "samples");
    }

    @Verb("sample-source.install")
    @PostMapping("/sample-sources/installations")
    Status start(@RequestBody Request request) {
        List<String> ids = request == null ? null : request.ids();
        if (ids == null || ids.isEmpty() || ids.size() > 10 || ids.stream().anyMatch(Objects::isNull)
                || ids.stream().distinct().count() != ids.size()) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "select one or more distinct sample sources"), null);
        }
        List<String> available = samples.catalog().stream().filter(item -> item.available())
                .map(item -> item.id()).toList();
        if (!available.containsAll(ids)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "a selected sample source is unavailable"), null);
        }
        Job job = new Job(UUID.randomUUID().toString(), AuthenticatedCaller.subject(), ids);
        Status accepted = job.snapshot();
        synchronized (jobs) {
            if (closed) throw unavailable();
            jobs.entrySet().removeIf(entry -> entry.getValue().expired());
            if (jobs.size() >= MAX_RETAINED_JOBS) {
                var completed = jobs.entrySet().iterator();
                while (completed.hasNext()) {
                    if (completed.next().getValue().finished()) {
                        completed.remove();
                        break;
                    }
                }
            }
            if (jobs.size() >= MAX_RETAINED_JOBS) throw unavailable();
            jobs.put(job.id, job);
            try {
                executor.execute(() -> run(job));
            } catch (RejectedExecutionException rejected) {
                jobs.remove(job.id);
                throw unavailable();
            }
        }
        return accepted;
    }

    @Verb("sample-source.list")
    @GetMapping("/sample-sources/installations/{jobId}")
    Status status(@PathVariable("jobId") String jobId) {
        Job job;
        synchronized (jobs) { job = jobs.get(jobId); }
        if (job == null || !job.principal.equals(AuthenticatedCaller.subject())) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample installation was not found"), null);
        }
        return job.snapshot();
    }

    private void run(Job job) {
        try {
            for (String id : job.ids) {
                job.checkRunning();
                try {
                    samples.installSelected(job.principal, id, job::stage);
                } catch (TapstateException error) {
                    // Arguments and causes can contain connection settings; only the canonical code is public.
                    job.fail(id, error.code().code());
                }
            }
        } catch (RuntimeException | Error defect) {
            // Preserve programmer defects for the owner's next GET and the existing global error boundary.
            LOG.error("Sample installation job {} failed unexpectedly", job.id, defect);
            job.defect(defect);
        } finally {
            job.finish();
        }
    }

    private static TapstateException unavailable() {
        return new TapstateException(ControlError.UNREACHABLE, Map.of(), null);
    }

    @Override
    public void close() {
        synchronized (jobs) {
            closed = true;
            jobs.values().forEach(Job::cancel);
            jobs.clear();
            executor.shutdownNow();
        }
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class Job {
        private final String id;
        private final String principal;
        private final List<String> ids;
        private final Map<String, Item> items = new LinkedHashMap<>();
        private boolean finished;
        private boolean cancelled;
        private long finishedAt;
        private Throwable defect;

        private Job(String id, String principal, List<String> ids) {
            this.id = id;
            this.principal = principal;
            this.ids = List.copyOf(ids);
            for (String sourceId : ids) items.put(sourceId, new Item(sourceId, "QUEUED", null, null));
        }

        private synchronized void stage(String sourceId, String stage) {
            checkRunning();
            items.put(sourceId, new Item(sourceId, stage, null, null));
        }

        private synchronized void fail(String sourceId, String code) {
            items.put(sourceId, new Item(sourceId, "FAILED", code, null));
        }

        private synchronized void checkRunning() {
            if (cancelled || Thread.currentThread().isInterrupted()) throw new CancellationException();
        }

        private synchronized void defect(Throwable failure) { defect = failure; }

        private synchronized void cancel() { cancelled = true; }

        private synchronized void finish() {
            finishedAt = System.currentTimeMillis();
            finished = true;
        }

        private synchronized boolean finished() { return finished; }

        private synchronized Status snapshot() {
            if (defect instanceof RuntimeException runtime) throw runtime;
            if (defect instanceof Error error) throw error;
            return new Status(id, new ArrayList<>(items.values()), finished);
        }

        private synchronized boolean expired() {
            return finished && System.currentTimeMillis() - finishedAt > JOB_RETENTION_MS;
        }
    }
}
