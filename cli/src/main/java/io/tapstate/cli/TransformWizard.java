package io.tapstate.cli;

import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;

/**
 * The interactive standalone {@code transform} flow: ask the id and the transform type, then collect
 * that type's body via {@link TransformBodyPrompter}, and build a canonical {@code kind: transform}
 * definition — pure logic with no {@code from:} wiring (X19); a pipeline supplies the wiring at the use
 * site. It collects answers through a {@link Prompter}, never a terminal directly.
 */
final class TransformWizard {

    private final Prompter prompter;
    private final TransformBodyPrompter bodyPrompter;

    TransformWizard(Prompter prompter) {
        this.prompter = prompter;
        this.bodyPrompter = new TransformBodyPrompter(prompter);
    }

    TransformResource run() {
        String id = WizardPrompts.askId(prompter, "Transform id", "transform");
        TransformBody body;
        do {
            // a map with no fields cannot round-trip -> askBody returns null and the type is re-asked;
            // every other type always yields a body, and join (never null) is the exhausted default
            body = bodyPrompter.askBody(prompter.choose("Transform type?", TransformBodyPrompter.TYPES));
        } while (body == null);
        return new TransformResource(id, null, body, null);
    }
}
