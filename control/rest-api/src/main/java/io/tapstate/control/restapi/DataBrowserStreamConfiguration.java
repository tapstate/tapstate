package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.DataBrowserService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;

/**
 * Mounts the followed-collection stream at {@code /api/data-browser/{source}/{collection}/tail}.
 *
 * <p>Its own configuration rather than a third entry beside the two pipeline streams, and the reason
 * is not tidiness. This channel needs the read service, and putting it there would have made every
 * context that mounts a status watch also have to supply one — which is exactly what happened: two
 * unrelated contexts failed to start, neither of which has any interest in reading data. A stream
 * family that carries rows and one that carries pipeline observations have no reason to be assembled
 * together, and keeping them apart is what lets each be present only where it is wanted.
 *
 * <p>It also shares no scheduler with them, which is the load-bearing half. Those two hand their
 * polling to a small shared pool; a follow is a blocking read that never returns, so a couple of
 * follows on that pool would leave every status watch and log follow in the process frozen with
 * nothing reporting it. A follow instead rides the thread its subscription brought.
 */
@Configuration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableWebSocket
public class DataBrowserStreamConfiguration {
    static final String TAIL_PATH = "/api/data-browser/*/*/tail";

    /**
     * Held as a bean so its live follows are released when the context goes. What a follow holds is a
     * connector instance and a place in the host-wide ceiling, so a context that closed without
     * stopping them would leave both held until the process died.
     */
    @Bean(destroyMethod = "closeAll")
    DataBrowserTailHandler dataBrowserTailHandler(DataBrowserService browser) {
        return new DataBrowserTailHandler(browser);
    }

    @Bean
    WebSocketConfigurer dataBrowserStreamEndpoint(
            DataBrowserTailHandler handler, CredentialAuthenticator credentials) {
        // A follow invents no operation of its own, so its handshake is gated by the grade of the read
        // it streams. Bound to the row read rather than to anything of its own: what crosses here is
        // rows, and whoever may read them a page at a time is exactly whoever may watch them change.
        // Missing this interceptor is not a visible failure -- the endpoint would upgrade, and an
        // anonymous caller would be handed every change to the collection. No projection gate covers a
        // websocket path and no arch rule requires one carry an interceptor, so nothing else would say.
        return registry -> registry.addHandler(handler, TAIL_PATH)
                .addInterceptors(new PipelineStreamHandshakeInterceptor(
                        credentials, ControlOperations.DATA_BROWSER_FIND));
    }
}
