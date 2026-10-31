package io.tapstate.core.validation;

import java.util.List;

/** Opaque, completed local delegation of one subtree to an independent Nest checker. */
public sealed interface ValidationNestCheckedSubtree
        permits ValidationNestArrayFieldCheck.Checked, ValidationNestObjectFieldCheck.Checked {
    /** Reject reuse with any other observation, object coordinate, row instance or root staging. */
    List<String> require(ValidationObservation parent, List<Object> prefix, ValidationValues.Row source,
                         ValidationValues.Row target, ValidationRootCheck.Builder root);
}
