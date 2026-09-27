package io.tapstate.cli;

import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.ProjectManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whatever {@code new} writes, it leaves a named project behind: every recipe in the catalog writes a
 * well-formed {@code project.tap.yml}, and a directory that already is a project keeps its own. The
 * answers each recipe needs are listed per recipe; one added to the catalog without an entry here
 * fails the coverage check below rather than going unexamined.
 */
class EveryRecipeWritesAProjectFileTest {

    private static final List<String> MYSQL = List.of(
            "--connector", "mysql", "--set", "host=db", "--set", "username=u", "--set", "password=s");

    /** The flags that let each recipe run without asking anything. */
    private static final Map<String, List<String>> ANSWERS = Map.of(
            "sample", List.of(),
            "blank", List.of(),
            "mirrored-table", concat(MYSQL, List.of("--table", "orders", "--view", "orders_view")),
            "reshaped-table", concat(MYSQL, List.of("--table", "orders", "--view", "orders_view",
                    "--keep", "id,amount")),
            "nested-json", concat(MYSQL, List.of("--root", "orders", "--child", "shipments:order_id=id")),
            "consolidated-table", List.of("--table", "orders",
                    "--db", "mysql,host=db1,username=u,password=s1",
                    "--db", "mysql,host=db2,username=u,password=s2"));

    @Test
    void everyRecipeInTheCatalogIsCovered() {
        assertThat(ANSWERS.keySet())
                .as("a recipe in the catalog with no answers here is a recipe nobody checked for a project file")
                .containsExactlyInAnyOrderElementsOf(Recipe.CATALOG.stream().map(Recipe::id).toList());
    }

    @Test
    void everyRecipeWritesAWellFormedProjectFile(@TempDir Path home, @TempDir Path parent) throws IOException {
        for (Recipe recipe : Recipe.CATALOG) {
            Path ws = Files.createDirectory(parent.resolve("ws-" + recipe.id()));
            NewRecipeTest.Run r = run(home, recipe.id(), ws);

            assertThat(r.code()).as("%s: %s", recipe.id(), r.all()).isZero();
            Path file = ws.resolve(ProjectManifest.FILE_NAME);
            assertThat(file).as("%s leaves a project file behind", recipe.id()).isRegularFile();
            ProjectManifest manifest = new DslParser().parseProject(Files.readString(file));
            if (recipe.id().equals("sample")) {
                assertThat(manifest.id()).as("the sample is the demo, named as the demo names it")
                        .isEqualTo("order_demo");
            } else {
                assertThat(manifest.id()).as("%s names the project after its directory", recipe.id())
                        .isEqualTo("ws-" + recipe.id());
            }
            assertThat(r.out()).contains(ProjectManifest.FILE_NAME + "  project " + manifest.id());
        }
    }

    @Test
    void aDirectoryThatIsAlreadyAProjectKeepsItsName(@TempDir Path home, @TempDir Path ws) throws IOException {
        String own = "version: tapstate/v1\nkind: project\nid: payments\n";
        Files.writeString(ws.resolve(ProjectManifest.FILE_NAME), own);

        for (String recipe : List.of("sample", "mirrored-table")) {
            assertThat(run(home, recipe, ws, "--force").code()).isZero();
            assertThat(Files.readString(ws.resolve(ProjectManifest.FILE_NAME))).as(recipe).isEqualTo(own);
        }
    }

    private static NewRecipeTest.Run run(Path home, String recipe, Path ws, String... extra) {
        List<String> args = new ArrayList<>(List.of("new", recipe, "--yes"));
        args.addAll(ANSWERS.get(recipe));
        args.addAll(List.of("-w", ws.toString()));
        args.addAll(List.of(extra));
        return NewRecipeTest.run(home, new ScriptedPrompter(), args.toArray(String[]::new));
    }

    private static List<String> concat(List<String> a, List<String> b) {
        List<String> all = new ArrayList<>(a);
        all.addAll(b);
        return all;
    }
}
