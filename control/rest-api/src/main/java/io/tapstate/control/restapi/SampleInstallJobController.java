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
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Installs selected samples in the background and exposes actual stage transitions. */
@RestController
final class SampleInstallJobController {
    private static final long JOB_RETENTION_MS = 24L * 60 * 60 * 1000;
    record Request(List<String> ids) { }
    record Item(String id, String stage, String errorCode, String errorDetail) { }
    record Status(String id, List<Item> items, boolean finished) { }

    private final SampleSourceService samples;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(SampleInstallJobController.class);

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
        String jobId = UUID.randomUUID().toString();
        jobs.entrySet().removeIf(entry -> entry.getValue().expired());
        Job job = new Job(jobId, AuthenticatedCaller.subject(), ids);
        jobs.put(jobId, job);
        Thread.startVirtualThread(() -> run(job));
        return job.snapshot();
    }

    @Verb("sample-source.list")
    @GetMapping("/sample-sources/installations/{jobId}")
    Status status(@PathVariable String jobId) {
        Job job = jobs.get(jobId);
        if (job == null || !job.principal.equals(AuthenticatedCaller.subject())) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "sample installation was not found"), null);
        }
        return job.snapshot();
    }

    private void run(Job job) {
        for (String id : job.ids) {
            try {
                samples.installSelected(job.principal, id, job::stage);
            } catch (TapstateException error) {
                job.fail(id, error.code().code(), String.valueOf(error.args().getOrDefault("reason", "")));
            } catch (RuntimeException error) {
                LOG.error("Unexpected sample installation failure for {}", id, error);
                job.fail(id, ControlError.UNREACHABLE.code(), "Unexpected server error");
            }
        }
        job.finish();
    }

    private static final class Job {
        private final String id;
        private final String principal;
        private final List<String> ids;
        private final Map<String, Item> items = new LinkedHashMap<>();
        private final long createdAt = System.currentTimeMillis();
        private boolean finished;

        private Job(String id, String principal, List<String> ids) {
            this.id = id;
            this.principal = principal;
            this.ids = List.copyOf(ids);
            for (String sourceId : ids) items.put(sourceId, new Item(sourceId, "QUEUED", null, null));
        }

        private synchronized void stage(String sourceId, String stage) {
            items.put(sourceId, new Item(sourceId, stage, null, null));
        }

        private synchronized void fail(String sourceId, String code, String detail) {
            items.put(sourceId, new Item(sourceId, "FAILED", code, detail));
        }

        private synchronized void finish() { finished = true; }

        private synchronized Status snapshot() {
            return new Status(id, new ArrayList<>(items.values()), finished);
        }

        private synchronized boolean expired() {
            return finished && System.currentTimeMillis() - createdAt > JOB_RETENTION_MS;
        }
    }
}
