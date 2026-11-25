package io.tapstate.app;

import io.tapstate.core.alerting.*;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertMailLinksTest {
    static AlertIntent.Member member(long episode) {
        var scope=new AlarmKey.Scope("org","cluster");
        var identity=AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"rule");
        var key=new AlarmKey(scope,new AlarmKey.Resource("source","erp","source-life"),"source.connection-failed",identity.incarnation());
        var rule=new AlertRule(key,1,1,"producer",Duration.ZERO,Duration.ZERO,Duration.ofSeconds(60),Duration.ofSeconds(5));
        return new AlertIntent.Member(rule,episode,identity,1);
    }
    @Test void selectedAlertLinksUseTheConfiguredOriginAndOriginalEpisode() {
        var links=new AlertMailLinks("https://tapstate.example",null,false);
        var member=member(3);var url=links.details(List.of(member),false);
        assertThat(url.toASCIIString()).isEqualTo("https://tapstate.example/alerts?view=active&key="+AlertHttpModel.key(member.evaluation().key())+"&episode=3");
        assertThat(links.details(List.of(member),true).getRawQuery()).startsWith("view=history&");
    }
    @Test void localHttpLinksNeedTheExistingExplicitLoopbackOptIn() {
        assertThatThrownBy(()->new AlertMailLinks("http://localhost:64120",null,false)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(new AlertMailLinks("http://localhost:64120",null,true).details(List.of(member(1)),false).getScheme()).isEqualTo("http");
        for(String bad:List.of("http://localhost.evil.example","http://127.0.0.2","https://user:pass@tapstate.example","https://tapstate.example/path","https://tapstate.example?token=private","https://tapstate.example#private"))
            assertThatThrownBy(()->new AlertMailLinks(bad,null,true)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
    @Test void ambiguousOriginsHaveNoLinkAndGroupsUseTheAlertCenter() {
        assertThat(AlertMailLinks.none().details(List.of(member(1)),false)).isNull();
        assertThat(new AlertMailLinks(null,"https://one.example,https://two.example",false).details(List.of(member(1)),false)).isNull();
        var links=new AlertMailLinks(null,"https://tapstate.example",false);
        assertThat(links.details(List.of(member(1),member(2)),false).toASCIIString()).isEqualTo("https://tapstate.example/alerts");
        assertThat(links.details(List.of(member(0)),false).getRawQuery()).isNull();
        assertThat(links.details(List.of(member(Long.MAX_VALUE)),false).getRawQuery()).isNull();
    }
}
