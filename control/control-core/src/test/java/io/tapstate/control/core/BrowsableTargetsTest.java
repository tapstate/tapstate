package io.tapstate.control.core;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every MongoDB-family connector a pipeline can write to is one the data browser reads rows through. A
 * start looks at its targets through that face, so a write target missing from it is one whose start can
 * never ask before a full load goes into it -- silently, because the check reports it cannot tell and lets
 * the start through.
 */
class BrowsableTargetsTest {

    @Test
    void everyMongoFamilyWriteTargetIsBrowsable() {
        List<String> family = OfficialConnectors.IDS_BY_DATABASE_KIND.get("mongodb");
        Set<String> writableFamily = TapstateCatalog.load().all().stream()
                .filter(entry -> entry.sink().capable())
                .map(ConnectorCatalogEntry::id)
                .filter(family::contains)
                .collect(Collectors.toSet());

        assertThat(writableFamily)
                .as("the cloud's only write target is among them, so this compares something")
                .contains("mongodb-atlas");
        assertThat(DataBrowserService.browsableConnectors()).containsAll(writableFamily);
    }
}
