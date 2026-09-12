package io.tapstate.cli;

import java.io.PrintWriter;
import java.util.List;

/**
 * The guided first run's one question ({@code docs/first-run/README.md}): which outcome this workspace
 * is for. It is skipped when a recipe id on the command line already names one, and never asked at all
 * without a prompter, which is what {@code --yes} promises a script.
 *
 * <p>An opening line goes first because {@code new} on its own is ambiguous - new what? - so the noun
 * is said before the question that assumes it. The question itself is worded by outcome, never by
 * resource type: which of those a recipe writes is the recipe's business, not the author's.
 *
 * <p><b>Nothing here touches a server.</b> Scaffolding is a local act: no probe, no process, no sign-in
 * and no binding. Everything that reaches a server belongs to {@code up} and lives in
 * {@link ServerBinding}.
 */
final class GuidedNew {

    static final String OPENING_LINE =
            "Building a workspace: a directory of .tap.yml files you can read and edit.";
    static final String RECIPE_QUESTION = "What is this workspace for?";

    private final Prompter prompter;
    private final PrintWriter prose;

    /**
     * @param prompter what asks the question, or null to never ask (the recipe must then be named)
     * @param prose    where the lines a person reads go, or null when nobody is reading them
     */
    GuidedNew(Prompter prompter, PrintWriter prose) {
        this.prompter = prompter;
        this.prose = prose;
    }

    /**
     * The recipe to run: the one already named, or the one picked from the catalog.
     *
     * @param recipeId the recipe already named on the command line, or null to ask
     */
    String chooseRecipe(String recipeId) {
        say(OPENING_LINE);
        return recipeId != null ? recipeId : askRecipe();
    }

    private String askRecipe() {
        List<String> titles = Recipe.CATALOG.stream().map(Recipe::title).toList();
        String title = prompter.choose(RECIPE_QUESTION, titles, titles.get(0));
        return Recipe.CATALOG.stream()
                .filter(recipe -> recipe.title().equals(title))
                .map(Recipe::id)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("prompter returned an option it was not offered: " + title));
    }

    private void say(String line) {
        if (prose != null) {
            prose.println(line);
            prose.flush();
        }
    }
}
