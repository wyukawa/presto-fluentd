package fluentd;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import com.google.common.collect.ImmutableList;

public class FluentdPlugin implements Plugin {

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return ImmutableList.of(new FluentdListenerFactory());
    }
}
