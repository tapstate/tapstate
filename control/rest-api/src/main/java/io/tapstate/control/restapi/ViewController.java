package io.tapstate.control.restapi;

import io.tapstate.control.core.ViewCatalogService;
import java.util.Objects;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** One read for every collection maintained by a pipeline. */
@RestController
class ViewController {

    private final ViewCatalogService views;

    ViewController(ViewCatalogService views) {
        this.views = Objects.requireNonNull(views, "views");
    }

    @Verb("view.list")
    @GetMapping("/views")
    ViewList list() {
        return new ViewList(views.list());
    }
}
