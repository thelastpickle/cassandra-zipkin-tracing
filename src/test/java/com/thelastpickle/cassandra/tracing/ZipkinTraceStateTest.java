
package com.thelastpickle.cassandra.tracing;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.FixedSampleRateTraceFilter;
import com.github.kristofa.brave.IdConversion;
import com.github.kristofa.brave.LoggingSpanCollector;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.BraveHttpHeaders;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;


import org.apache.http.HttpRequest;
import org.junit.Test;

import static java.util.Collections.singletonMap;


public final class ZipkinTraceStateTest {

    @Test
    public void test_trace() {
        System.out.println("traceImpl");
        System.setProperty("cassandra.storagedir", "target");
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

    static class CodeForSlides {
        void brave_cs_cr_with_headers(HttpRequest request) {


            SpanId spanId = clientTracer.startNewSpan(request.getRequestLine().getUri());

            request.addHeader(BraveHttpHeaders.Sampled.getName(), "1");
            request.addHeader(BraveHttpHeaders.TraceId.getName(), IdConversion.convertToString(spanId.traceId));
            request.addHeader(BraveHttpHeaders.SpanId.getName(), IdConversion.convertToString(spanId.spanId));

            clientTracer.setClientSent();
            Result result = doRequest(request);
            clientTracer.setClientReceived();

            System.out.println(result);
        }

        void opentracing_cs_cr_with_headers(HttpRequest request) {
            Result result;


            try (Span span = tracer.buildSpan(request.getRequestLine().getUri()).start()) {
                result = doRequest(request);
            }

            System.out.println(result);
        }

        void opentracing_cs_cr_with_datastax(Session session, Statement statement) {

            ResultSet result;

            try (Span span = tracer.buildSpan(statement.toString()).start()) {
                result = session.execute(statement);
            }



            System.out.println(result);
        }

        void cs_cr_with_datastax(Session session, Statement statement) {


            SpanId spanId = clientTracer.startNewSpan(statement.toString());

            ByteBuffer traceHeaders = ByteBuffer.wrap(spanId.bytes());
            statement.setOutgoingPayload(singletonMap("zipkin", traceHeaders));

            clientTracer.setClientSent();
            ResultSet result = session.execute(statement);
            clientTracer.setClientReceived();



            System.out.println(result);
        }

        private static Result doRequest(HttpRequest request) {return null;}

        private final Tracer tracer = new Tracer() {
                @Override
                public Tracer.SpanBuilder buildSpan(String string) {
                    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                }

                @Override
                public <C> void inject(SpanContext sc, Format<C> format, C c) {
                    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                }

                @Override
                public <C> SpanContext extract(Format<C> format, C c) {
                    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                }
            };
        private final ClientTracer clientTracer = ((ZipkinTracing) Tracing.instance).brave.clientTracer();
        private static class Result {}
    }
}
