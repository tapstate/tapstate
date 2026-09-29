package io.tapstate.cli;

import java.util.List;

/**
 * The outcome of a remote {@code GET /api/projects}: the projects the server holds, the Default project
 * first; a coded refusal; or no answer at all. Sealed so the caller renders each branch without try/catch.
 */
sealed interface ProjectListOutcome {

    /** One resource a project holds. */
    record Member(String id, String kind) {
    }

    /** One project: its id, the name it is shown under, whether it can be removed, and what it holds. */
    record Project(String id, String title, boolean removable, List<Member> resources) {
        public Project {
            resources = List.copyOf(resources);
        }
    }

    /** The server answered with its projects. */
    record Listed(List<Project> projects) implements ProjectListOutcome {
        public Listed {
            projects = List.copyOf(projects);
        }
    }

    /** The server refused with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements ProjectListOutcome {
    }

    /** The server could not be reached. */
    record Unreachable() implements ProjectListOutcome {
    }
}
