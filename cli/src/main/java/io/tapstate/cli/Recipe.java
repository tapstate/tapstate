package io.tapstate.cli;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One entry of the guided first-run catalog: a recipe is what {@code new <id>} takes, worded by the
 * outcome it produces rather than the mechanism that produces it.
 *
 * <p>The catalog is a fixed list in code, on purpose. The CLI ships as a native image, so nothing
 * here is discovered by scanning a classpath or read from a resource at run time; and the text and
 * machine-readable renderings of {@code new --list} are both generated from this one list, so the
 * wording cannot drift between them. The list is the one on {@code docs/first-run/README.md}, in
 * that order, and a test holds the two together.
 *
 * @param id       what the user ends up with, {@code <past-participle>-<noun>}; never an internal type name
 * @param title    the wording the picker shows
 * @param runnable whether the workspace it writes can be brought up as it stands; only {@code blank}
 *                 is not, because it writes nothing to run
 * @param uses     the transform types the generated pipeline relies on, empty when none
 */
record Recipe(String id, String title, boolean runnable, List<String> uses) {

    /** Every recipe, in picker order; {@code blank} sits last because it means "none of the above". */
    static final List<Recipe> CATALOG = List.of(
            new Recipe("sample", "Try it with sample data", true, List.of()),
            new Recipe("mirrored-table", "Mirror one table, as it changes", true, List.of("cdc")),
            new Recipe("reshaped-table", "Mirror a table, renamed / filtered / trimmed", true,
                    List.of("cdc", "map", "filter")),
            new Recipe("nested-json", "Assemble several tables into one object", true, List.of("nest")),
            new Recipe("consolidated-table", "Consolidate the same table from several databases", true,
                    List.of("union")),
            new Recipe("blank", "Nothing generated - I will write it myself", false, List.of()));

    /** The recipe with this id, or empty when the catalog has none — the caller says how that is refused. */
    static Optional<Recipe> byId(String id) {
        return CATALOG.stream().filter(recipe -> recipe.id().equals(id)).findFirst();
    }

    /**
     * The catalog as the ordered tree the machine writers take: a top-level {@code recipes} array whose
     * entries carry {@code id}, {@code title}, {@code runnable}, {@code uses} in that order. Keys are a
     * stable contract — new ones may be added, existing ones are never renamed.
     */
    static Map<String, Object> catalogTree() {
        List<Map<String, Object>> recipes = new ArrayList<>();
        for (Recipe recipe : CATALOG) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", recipe.id());
            entry.put("title", recipe.title());
            entry.put("runnable", recipe.runnable());
            entry.put("uses", recipe.uses());
            recipes.add(entry);
        }
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("recipes", recipes);
        return tree;
    }
}
