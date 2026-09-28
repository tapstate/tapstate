package io.tapstate.core.model;

import java.util.Objects;

/**
 * The identity a project file declares: the project's id and its optional annotations. A project is
 * a directory of resource files brought up as one unit; this file is what names it, so that the name
 * does not depend on what the directory happens to be called.
 *
 * <p>It is not a resource. It is never sent to a server, and nothing on a server is stored under it:
 * a server knows a project only through the {@code project} label its resources carry.
 */
@Doc("A project file: names the directory it sits in as one project. It carries the project's id and "
        + "optional metadata, and nothing about where the project runs.")
public record ProjectManifest(
        @Doc(value = "The project's id: what the server groups this project's resources under.", required = true)
        String id,
        @Doc("Optional labels and free-text description.")
        Metadata metadata) {

    /** The kind a project file declares. */
    public static final String KIND = "project";

    /**
     * The project every server has from its first start, and the one a resource with no project label
     * belongs to. No project file may declare it: a resource joins it by carrying no label at all.
     */
    public static final String DEFAULT = "default";

    /** The name the Default project is shown under. */
    public static final String DEFAULT_TITLE = "Default project";

    /** The file a project root carries. */
    public static final String FILE_NAME = "project.tap.yml";

    public ProjectManifest {
        Objects.requireNonNull(id, "id");
    }
}
