package io.tapstate.app;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class InstallationAutomaticRestartTest {
    @Test void oneSuccessfulHandoffClosesBeforeRequestingExactlyOneReplacement() throws Exception {
        var order = new CopyOnWriteArrayList<String>();
        var exited = new CountDownLatch(1);
        var restart = new InstallationAutomaticRestart(() -> order.add("closed"), code -> {
            order.add("exit:" + code); exited.countDown();
        });
        restart.run(); restart.run();
        assertThat(exited.await(6, TimeUnit.SECONDS)).isTrue();
        assertThat(order).containsExactly("closed", "exit:75");
    }
}
