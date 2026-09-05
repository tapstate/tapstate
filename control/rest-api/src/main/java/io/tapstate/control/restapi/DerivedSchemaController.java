package io.tapstate.control.restapi;

import io.tapstate.control.core.DerivedSchemas;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The derived-output-columns read and its one write, projected onto HTTP.
 *
 * <p>The read is what a person looks at when a start was refused because a join no longer produces the
 * columns it was recorded producing: what was recorded, what it produces now, and what the table it
 * writes into actually holds. The write is how they say to carry on, and it is a verb of its own rather
 * than a flag on the start - a flag is typed once and then lives in a script, and the whole value of
 * this check is the one moment somebody looks.
 *
 * <p>Both are thin pass-throughs; the comparison, the audit and the record all live behind the port.
 */
@RestController
class DerivedSchemaController {

    private final DerivedSchemas derivedSchemas;

    DerivedSchemaController(DerivedSchemas derivedSchemas) {
        this.derivedSchemas = derivedSchemas;
    }

    @Verb("pipeline.derived-schema")
    @GetMapping("/pipelines/{id}/derived-schema")
    List<DerivedSchemas.StepReport> derivedSchema(@PathVariable("id") String id) {
        return derivedSchemas.compare(id);
    }

    @Verb("pipeline.accept-derived-schema")
    @PostMapping("/pipelines/{id}:accept-derived-schema")
    List<DerivedSchemas.StepReport> acceptDerivedSchema(@PathVariable("id") String id) {
        derivedSchemas.accept(AuthenticatedCaller.subject(), id);
        // The report is returned after accepting, not before: what a caller wants back is the shape now
        // on record, and returning nothing would leave a scripted accept unable to say what it took.
        return derivedSchemas.compare(id);
    }
}
