package fluentd;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FluentdListenerFactory implements EventListenerFactory {

    public String getName() {
        return "presto-fluentd-logging";
    }

    public EventListener create(Map<String, String> map) {
        String fluentdHost = requireNonNull(map.get("event-listener.fluentd-host"), "event-listener.fluentd-host is null");
        String fluentdPort = requireNonNull(map.get("event-listener.fluentd-port"), "event-listener.fluentd-port is null");
        String fluentdTag = requireNonNull(map.get("event-listener.fluentd-tag"), "event-listener.fluentd-tag is null");
        try {
            return new FluentdListener(Fluency.defaultFluency(fluentdHost, Integer.parseInt(fluentdPort)), fluentdTag);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
