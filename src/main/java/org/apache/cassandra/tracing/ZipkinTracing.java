
package org.apache.cassandra.tracing;

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.FixedSampleRateTraceFilter;
import com.github.kristofa.brave.ServerTracer;
import com.github.kristofa.brave.ServerSpanThreadBinder;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.TraceFilter;
import com.google.common.collect.ImmutableMap;
import com.twitter.zipkin.gen.AnnotationType;
import com.twitter.zipkin.gen.BinaryAnnotation;
import com.twitter.zipkin.gen.Span;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scribe.thrift.LogEntry;
import scribe.thrift.scribe;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class ZipkinTracing extends TracingImpl
{
    public static final String ZIPKIN_TRACE_HEADERS = "zipkin";

    private static final Logger logger = LoggerFactory.getLogger(ZipkinTracing.class);

    private volatile SpanCollector spanCollector = new DemoScribeSpanCollector();

    private final int SAMPLE_RATE = 1;

    private final List<TraceFilter> traceFilters
            // Sample rate = 1 means every request will get traced.
            = Collections.singletonList(new FixedSampleRateTraceFilter(SAMPLE_RATE));

    private final ServerSpanThreadBinder serverSpanThreadBinder;

    public ZipkinTracing()
    {
        submitEndpoint();
        this.serverSpanThreadBinder = Brave.getServerSpanThreadBinder();
    }

    private static void submitEndpoint()
    {
        String hostAddress = FBUtilities.getBroadcastAddress().getHostAddress();
        int nativeTransportPort = DatabaseDescriptor.getNativeTransportPort();
        String name = "c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getBroadcastAddress().getHostName();
        Brave.getEndpointSubmitter().submit(hostAddress, nativeTransportPort, name);
    }

    ClientTracer getClientTracer()
    {
        return Brave.getClientTracer(spanCollector, traceFilters);
    }

    private ServerTracer getServerTracer()
    {
        return Brave.getServerTracer(spanCollector, traceFilters);
    }


    @Override
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        ByteBuffer bb = customPayload.get(ZIPKIN_TRACE_HEADERS);
        if (null != bb)
        {
            getServerTracer().setStateCurrentTrace(bb.getLong(), bb.getLong(), null, traceType.name());
        }
        else
        {
            getServerTracer().setStateUnknown(traceType.name());
        }
        return super.newSession(sessionId, traceType, customPayload);
    }

    @Override
    public void stopSession()
    {
        ZipkinTraceState state = (ZipkinTraceState) get();
        if (state != null)
        {
            state.close();
            getServerTracer().setServerSend();
            getServerTracer().clearCurrentSpan();
        }
        super.stopSession();
    }

    @Override
    public void doneWithNonLocalSession(TraceState s)
    {
        ZipkinTraceState state = (ZipkinTraceState) s;
        state.close();
        getServerTracer().setServerSend();
        getServerTracer().clearCurrentSpan();
        super.doneWithNonLocalSession(state);
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters)
    {
        getServerTracer().submitBinaryAnnotation("client", client.toString());
        getServerTracer().submitBinaryAnnotation("request", request);
        return super.begin(request, client, parameters);
    }

    @Override
    public TraceState initializeFromMessage(final MessageIn<?> message)
    {
        byte [] bytes = message.parameters.get(ZIPKIN_TRACE_HEADERS);
        if (null != bytes && 0 < bytes.length)
        {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            getServerTracer().setStateCurrentTrace(bb.getLong(), bb.getLong(), null, message.getMessageType().name());
        }
        return super.initializeFromMessage(message);
    }

    @Override
    public Map<String, byte[]> getTraceHeaders()
    {
        assert isTracing();
        Span span = Brave.getClientSpanThreadBinder().getCurrentClientSpan();

        return ImmutableMap.<String, byte[]>builder()
                .putAll(super.getTraceHeaders())
                .put(
                        ZIPKIN_TRACE_HEADERS,
                        ByteBuffer.allocate(16).putLong(span.getTrace_id()).putLong(span.getId()).array())
                .build();
    }

    @Override
    protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType)
    {
        getServerTracer().setServerReceived();
        getServerTracer().submitBinaryAnnotation("sessionId", sessionId.toString());
        getServerTracer().submitBinaryAnnotation("coordinator", coordinator.toString());
        getServerTracer().submitBinaryAnnotation("started_at", Instant.now().toString());

        return new ZipkinTraceState(
                coordinator,
                sessionId,
                traceType,
                serverSpanThreadBinder.getCurrentServerSpan(),
                getClientTracer(),
                spanCollector);
    }

    private class DemoScribeSpanCollector implements SpanCollector
    {

        // default zipkin scribe collector port
        private String scribeHost = "localhost";
        private int scribePort = 9410;

        private final TTransport transport;
        private final scribe.Client scribeClient;
        private final Set<BinaryAnnotation> defaultAnnotations = new CopyOnWriteArraySet<>();
        private final AtomicLong lastReconnectAttempt = new AtomicLong(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));

        DemoScribeSpanCollector()
        {
            transport = new TFramedTransport(new TSocket(scribeHost, scribePort));
            scribeClient = new scribe.Client(new TBinaryProtocol(transport));
        }

        @Override
        public void collect(Span span)
        {
            for (final BinaryAnnotation ba : defaultAnnotations)
            {
                span.addToBinary_annotations(ba);
            }
            logger.debug("Collecting span " + span.toString());
            try
            {
                if (!transport.isOpen()) {
                    transport.open();
                }
                logger.info("sending span " + span);
                scribeClient.Log(Collections.singletonList(create(span)));
            }
            catch (TException e)
            {
                logger.error("dropped zipkin trace -- " + e.getMessage());
                tryReconnectOncePerSecond();
            }
        }

        private void tryReconnectOncePerSecond()
        {
            long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            if (lastReconnectAttempt.getAndSet(currentSecond) != currentSecond)
            {
                spanCollector = new DemoScribeSpanCollector();
                close();
            }
        }

        @Override
        public void addDefaultAnnotation(String key, String value)
        {
            try
            {
                final BinaryAnnotation binaryAnnotation = new BinaryAnnotation(
                        key,
                        ByteBuffer.wrap(value.getBytes("UTF-8")),
                        AnnotationType.STRING);

                defaultAnnotations.add(binaryAnnotation);
            }
            catch (final UnsupportedEncodingException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close()
        {
            transport.close();
        }

        private LogEntry create(final Span span) throws TException
        {
            final String spanAsString = new Base64().encodeToString(spanToBytes(span));
            return new LogEntry("zipkin", spanAsString);
        }

        private byte[] spanToBytes(final Span thriftSpan) throws TException
        {
            return new TSerializer().serialize(thriftSpan);
        }
    }
}
