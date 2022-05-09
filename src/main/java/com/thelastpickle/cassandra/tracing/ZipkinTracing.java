/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thelastpickle.cassandra.tracing;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.EmptySpanCollectorMetricsHandler;
import com.github.kristofa.brave.Sampler;
import com.github.kristofa.brave.ServerTracer;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.HttpSpanCollector;
import com.twitter.zipkin.gen.Span;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class ZipkinTracing extends Tracing
{
    public static final String ZIPKIN_TRACE_HEADERS = "zipkin";

    static final int SAMPLE_RATE = 1;

    private static final Logger logger = LoggerFactory.getLogger(ZipkinTracing.class);

    private static final String HTTP_SPAN_COLLECTOR_HOST = System.getProperty("ZipkinTracing.httpCollectorHost", "127.0.0.1");
    private static final String HTTP_SPAN_COLLECTOR_PORT = System.getProperty("ZipkinTracing.httpCollectorPort", "9411");
    private static final String HTTP_COLLECTOR_URL = "http://" + HTTP_SPAN_COLLECTOR_HOST + ':' + HTTP_SPAN_COLLECTOR_PORT;

    private final SpanCollector spanCollector
            = HttpSpanCollector.create(HTTP_COLLECTOR_URL, new EmptySpanCollectorMetricsHandler());
            //= KafkaSpanCollector.create("127.0.0.1:9092", new EmptySpanCollectorMetricsHandler());

    private final Sampler SAMPLER = Sampler.ALWAYS_SAMPLE;

    private final AtomicMonotonicTimestampGenerator TIMESTAMP_GENERATOR = new AtomicMonotonicTimestampGenerator();

    volatile Brave brave = new Brave
            .Builder( "c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getJustBroadcastAddress().getHostName())
            .spanCollector(spanCollector)
            .traceSampler(SAMPLER)
            .clock(() -> { return TIMESTAMP_GENERATOR.next(); })
            .build();


    public ZipkinTracing()
    {
    }

    ClientTracer getClientTracer()
    {
        return brave.clientTracer();
    }

    private ServerTracer getServerTracer()
    {
        return brave.serverTracer();
    }

    // defensive override, see CASSANDRA-11706
    @Override
    public UUID newSession(UUID sessionId, Map<String,ByteBuffer> customPayload)
    {
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }

    @Override
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String,ByteBuffer> customPayload)
    {
        ByteBuffer bb = null != customPayload ? customPayload.get(ZIPKIN_TRACE_HEADERS) : null;
        if (null != bb)
        {
            if (isValidHeaderLength(bb.limit()))
            {
                extractAndSetSpan(bb.array(), traceType.name());
            }
            else
            {
                logger.error("invalid customPayload in {}", ZIPKIN_TRACE_HEADERS);
                getServerTracer().setStateUnknown(traceType.name());
            }
        }
        else
        {
            getServerTracer().setStateUnknown(traceType.name());
        }
        return super.newSession(sessionId, traceType, customPayload);
    }

    @Override
    protected void stopSessionImpl()
    {
        ZipkinTraceState state = (ZipkinTraceState) get();
        if (state != null)
        {
            state.close();
            getServerTracer().setServerSend();
            getServerTracer().clearCurrentSpan();
        }
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
        if (null != client)
            getServerTracer().submitBinaryAnnotation("client", client.toString());

        getServerTracer().submitBinaryAnnotation("request", request);
        return get();
    }

    @Override
    public TraceState initializeFromMessage(Message.Header header)
    {
        byte [] bytes = null != header.customParams()
                ? header.customParams().get(ZIPKIN_TRACE_HEADERS)
                : null;

        assert null == bytes || isValidHeaderLength(bytes.length)
                : "invalid customPayload in " + ZIPKIN_TRACE_HEADERS;

        if (null != bytes)
        {
            if (isValidHeaderLength(bytes.length))
            {
                extractAndSetSpan(bytes, header.verb.toString());
            }
            else
            {
                logger.error("invalid customPayload in {}", ZIPKIN_TRACE_HEADERS);
            }
        }
        return super.initializeFromMessage(header);
    }

    private void extractAndSetSpan(byte[] bytes, String name) {
        if (32 == bytes.length)
        {
            // Zipkin B3 propagation
            SpanId spanId = SpanId.fromBytes(bytes);
            getServerTracer().setStateCurrentTrace(spanId.traceId, spanId.spanId, spanId.parentId, name);
        }
        else
        {
            // deprecated approach
            ByteBuffer bb = ByteBuffer.wrap(bytes);

            getServerTracer().setStateCurrentTrace(
                    bb.getLong(),
                    bb.getLong(),
                    24 <= bb.limit() ? bb.getLong() : null,
                    name);
        }
    }

    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable)
    {
        assert isTracing();
        Span span = brave.clientSpanThreadBinder().getCurrentClientSpan();

        SpanId spanId = SpanId.builder()
                .traceId(span.getTrace_id())
                .parentId(span.getParent_id())
                .spanId(span.getId())
                .build();

        addToMutable.put(
                ParamType.CUSTOM_MAP,
                new HashMap<String,byte[]>() {{ put(ZIPKIN_TRACE_HEADERS, spanId.bytes()); }});

        return super.addTraceHeaders(addToMutable);
    }

    @Override
    public void trace(final ByteBuffer sessionId, final String message, final int ttl)
    {
        UUID sessionUuid = UUIDGen.getUUID(sessionId);
        TraceState state = Tracing.instance.get(sessionUuid);
        state.trace(message);
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, UUID sessionId, TraceType traceType)
    {
        getServerTracer().setServerReceived();
        getServerTracer().submitBinaryAnnotation("sessionId", sessionId.toString());
        getServerTracer().submitBinaryAnnotation("coordinator", coordinator.toString());
        getServerTracer().submitBinaryAnnotation("started_at", Instant.now().toString());

        return new ZipkinTraceState(
                brave,
                coordinator,
                sessionId,
                traceType,
                brave.serverSpanThreadBinder().getCurrentServerSpan());
    }

    private static boolean isValidHeaderLength(int length)
    {
        return 16 == length || 24 == length || 32 == length;
    }
}
