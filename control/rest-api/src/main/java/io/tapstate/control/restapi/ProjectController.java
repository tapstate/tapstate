package io.tapstate.control.restapi;

import io.tapstate.control.core.ProjectService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The projects face: which projects this server holds, read off the label their resources carry, and the
 * removal of one project as a whole. The Default project is always listed and never removable.
 */
@RestController
@RequestMapping("/api")
class ProjectController {

    /** The project list the HTTP face returns. */
    record ProjectList(List<ProjectService.ProjectSummary> items) {
        ProjectList {
            items = List.copyOf(items);
        }
    }

    private final ProjectService projects;

    ProjectController(ProjectService projects) {
        this.projects = projects;
    }

    @Verb("project.list")
    @GetMapping("/projects")
    ProjectList list() {
        return new ProjectList(projects.list());
    }

    @Verb("project.remove")
    @DeleteMapping("/projects/{id}")
    ResponseEntity<Void> remove(@PathVariable("id") String id) {
        projects.remove(AuthenticatedCaller.subject(), id);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
