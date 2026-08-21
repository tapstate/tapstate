package io.tapstate.cli;

import java.util.Map;

/** An explicit, version-specific conversion into the current context configuration schema. */
interface ContextConfigMigration {

    int sourceVersion();

    ContextConfig migrate(Map<String, Object> document);
}
