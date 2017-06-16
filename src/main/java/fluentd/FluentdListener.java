package fluentd;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import io.airlift.log.Logger;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FluentdListener implements EventListener {

    private static final Logger log = Logger.get(FluentdListener.class);

    private String fluentdTag;

    private Fluency fluency;

    public FluentdListener(Fluency fluency, String fluentdTag) {
        this.fluency = fluency;
        this.fluentdTag = fluentdTag;
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        Map<String, Object> event = new HashMap<>();
        event.put("queryId", queryCompletedEvent.getMetadata().getQueryId());
        event.put("query", queryCompletedEvent.getMetadata().getQuery());
        event.put("uri", queryCompletedEvent.getMetadata().getUri().toString());
        event.put("state", queryCompletedEvent.getMetadata().getQueryState());
        event.put("cpuTime", queryCompletedEvent.getStatistics().getCpuTime().toMillis());
        event.put("createTime", queryCompletedEvent.getCreateTime().toEpochMilli());
        event.put("endTime", queryCompletedEvent.getEndTime().toEpochMilli());
        event.put("executionStartTime", queryCompletedEvent.getExecutionStartTime().toEpochMilli());
        event.put("queuedTime", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
        event.put("peakMemoryBytes", queryCompletedEvent.getStatistics().getPeakMemoryBytes());
        event.put("totalBytes", queryCompletedEvent.getStatistics().getTotalBytes());
        event.put("totalRows", queryCompletedEvent.getStatistics().getTotalRows());
        event.put("remoteClientAddress", queryCompletedEvent.getContext().getRemoteClientAddress());
        event.put("user", queryCompletedEvent.getContext().getUser());
        event.put("userAgent", queryCompletedEvent.getContext().getUserAgent());
        event.put("source", queryCompletedEvent.getContext().getSource());
        event.put("catalog", queryCompletedEvent.getContext().getCatalog());
        event.put("schema", queryCompletedEvent.getContext().getSchema());

        if(queryCompletedEvent.getFailureInfo().isPresent()) {
            QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();
            event.put("errorCode", queryFailureInfo.getErrorCode());
            event.put("failureHost", queryFailureInfo.getFailureHost().orElse(""));
            event.put("failureMessage", queryFailureInfo.getFailureMessage().orElse(""));
            event.put("failureTask", queryFailureInfo.getFailureTask().orElse(""));
            event.put("failureType", queryFailureInfo.getFailureType().orElse(""));
            event.put("failuresJson", queryFailureInfo.getFailuresJson());
        }

        try {
            fluency.emit(fluentdTag, event);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

    }
}
