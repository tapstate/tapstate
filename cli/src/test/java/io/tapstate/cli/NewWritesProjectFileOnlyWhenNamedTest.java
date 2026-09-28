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
 * {@code new} writes a project file only when told which project the files belong to: without one the
 * directory is in the Default project, where a first run belongs. {@code sample} is the exception - it is
 * the demo, and it names the demo's project. Every recipe in the catalog is run both ways; one added
 * without an entry here fails the coverage check rather than going unexamined.
 */
class NewWritesProjectFileOnlyWhenNamedTest {

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
                .as("a recipe in the catalog with no answers here is a recipe nobody checked")
                .containsExactlyInAnyOrderElementsOf(Recipe.CATALOG.stream().map(Recipe::id).toList());
    }

    @Test
    void withoutANameNoRecipeButSampleWritesAProjectFile(@TempDir Path home, @TempDir Path parent) throws IOException {
        for (Recipe recipe : Recipe.CATALOG) {
            Path ws = Files.createDirectory(parent.resolve("unnamed-" + recipe.id()));
            NewRecipeTest.Run r = run(home, recipe.id(), ws);

            assertThat(r.code()).as("%s: %s", recipe.id(), r.all()).isZero();
            Path file = ws.resolve(ProjectManifest.FILE_NAME);
            if (recipe.id().equals("sample")) {
                assertThat(declared(file)).as("the sample names the demo's project").isEqualTo("order_demo");
            } else {
                assertThat(file).as("%s wrote a project file nobody asked for", recipe.id()).doesNotExist();
            }
        }
    }

    @Test
    void withANameEveryRecipeWritesThatProject(@TempDir Path home, @TempDir Path parent) throws IOException {
        for (Recipe recipe : Recipe.CATALOG) {
            Path ws = Files.createDirectory(parent.resolve("named-" + recipe.id()));
            NewRecipeTest.Run r = run(home, recipe.id(), ws, "--project", "bank_c360");

            assertThat(r.code()).as("%s: %s", recipe.id(), r.all()).isZero();
            assertThat(declared(ws.resolve(ProjectManifest.FILE_NAME))).as(recipe.id()).isEqualTo("bank_c360");
            assertThat(r.out()).contains(ProjectManifest.FILE_NAME + "  project bank_c360");
        }
    }

    @Test
    void theDefaultProjectCannotBeNamed(@TempDir Path home, @TempDir Path ws) {
        NewRecipeTest.Run r = run(home, "mirrored-table", ws, "--project", "default");

        assertThat(r.code()).isNotZero();
        assertThat(r.all()).contains("dsl.illegal-value");
        assertThat(ws.resolve(ProjectManifest.FILE_NAME)).doesNotExist();
    }

    @Test
    void aDirectoryThatIsAlreadyAProjectKeepsItsName(@TempDir Path home, @TempDir Path ws) throws IOException {
        String own = "version: tapstate/v1\nkind: project\nid: payments\n";
        Files.writeString(ws.resolve(ProjectManifest.FILE_NAME), own);

        assertThat(run(home, "sample", ws, "--force").code()).isZero();
        assertThat(run(home, "mirrored-table", ws, "--force", "--project", "payments").code()).isZero();
        assertThat(Files.readString(ws.resolve(ProjectManifest.FILE_NAME))).isEqualTo(own);
        assertThat(run(home, "mirrored-table", ws, "--force", "--project", "billing").all())
                .contains("already project 'payments'");
    }

    private static String declared(Path file) throws IOException {
        return new DslParser().parseProject(Files.readString(file)).id();
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
