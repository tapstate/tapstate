package io.tapstate.control.restapi;

import io.tapstate.control.core.SampleSourceService;
import java.util.List;
import java.util.Objects;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** Public metadata and authenticated installation for deployment-configured samples. */
@RestController
class SampleSourceController {
    private final SampleSourceService samples;

    SampleSourceController(SampleSourceService samples) {
        this.samples = Objects.requireNonNull(samples, "samples");
    }

    @Verb("sample-source.list")
    @GetMapping("/sample-sources")
    SampleList list() {
        return new SampleList(samples.available());
    }

    @Verb("sample-source.install")
    @PostMapping("/sample-sources:install")
    SampleSourceService.Installation install() {
        return samples.install(AuthenticatedCaller.subject());
    }

    record SampleList(List<SampleSourceService.Descriptor> sources) { }
}
