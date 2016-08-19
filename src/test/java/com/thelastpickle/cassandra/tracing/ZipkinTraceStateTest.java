
package com.thelastpickle.cassandra.tracing;

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.FixedSampleRateTraceFilter;
import com.github.kristofa.brave.LoggingSpanCollector;
import com.github.kristofa.brave.SpanId;
import java.util.Collections;
import java.util.UUID;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;




public final class ZipkinTraceStateTest {

    @Test
    public void test_trace() {
        System.out.println("traceImpl");
        System.setProperty("cassandra.custom_tracing_class", ZipkinTracing.class.getName());
        ZipkinTracing tracing = (ZipkinTracing) Tracing.instance;

        tracing.brave = new Brave
            .Builder("test_trace")
            .spanCollector(new LoggingSpanCollector())
            .traceFilters(Collections.singletonList(new FixedSampleRateTraceFilter(ZipkinTracing.SAMPLE_RATE)))
            .build();

        UUID sessionId = tracing.newSession(Tracing.TraceType.QUERY);
        assert null != sessionId;
        assert null != tracing.get(sessionId);
        assert null != tracing.get();
        ZipkinTraceState state = (ZipkinTraceState) tracing.begin("begin-test", Collections.emptyMap());
        assert state.sessionId == sessionId;
        assert FBUtilities.getLocalAddress().equals(state.coordinator);
        assert Tracing.TraceType.QUERY == state.traceType;
        Tracing.trace("test-0");
        assert 1 == state.openSpans.size();
        assert state.openSpans.getFirst().getName().startsWith("test-0");
        Tracing.trace("test-1");
        assert 1 == state.openSpans.size();
        assert state.openSpans.getFirst().getName().startsWith("test-1");
        Tracing.trace("test-2");
        assert 1 == state.openSpans.size();
        assert state.openSpans.getFirst().getName().startsWith("test-2");
        MessageOut msg = new MessageOut(MessagingService.Verb.READ);
        assert msg.parameters.containsKey(ZipkinTracing.ZIPKIN_TRACE_HEADERS);
        byte[] bytes = (byte[]) msg.parameters.get(ZipkinTracing.ZIPKIN_TRACE_HEADERS);
        assert 32 == bytes.length;
        SpanId spanId = SpanId.fromBytes(bytes);
        assert state.openSpans.getFirst().getTrace_id() == spanId.traceId;
        assert state.openSpans.getFirst().getId() == spanId.spanId;
        assert state.openSpans.getFirst().getParent_id() == spanId.parentId;
        tracing.stopSession();
    }

}
