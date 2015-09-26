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

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.ServerSpan;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.Span;
import org.apache.cassandra.tracing.TraceStateImpl;
import org.apache.cassandra.tracing.Tracing;

import java.net.InetAddress;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;


/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
final class ZipkinTraceState extends TraceState
{
    private final ClientTracer clientTracer;
    private final SpanCollector spanCollector;
    private final ServerSpan serverSpan;

    private final Deque<Span> openSpans = new ConcurrentLinkedDeque();
    private final ThreadLocal<Span> previous = new ThreadLocal<>();

    public ZipkinTraceState(
            InetAddress coordinator,
            UUID sessionId,
            Tracing.TraceType traceType,
            ServerSpan serverSpan,
            ClientTracer clientTracer,
            SpanCollector spanCollector)
    {
        super(coordinator, sessionId, traceType);
        assert null != serverSpan;
        this.serverSpan = serverSpan;
        this.clientTracer = clientTracer;
        this.spanCollector = spanCollector;
    }

    protected void traceImpl(String message)
    {
        traceImplWithSpans(message);
    }

    void close()
    {
        Brave.getServerSpanThreadBinder().setCurrentSpan(serverSpan);
        for (Span span : openSpans)
        {
            Brave.getClientSpanThreadBinder().setCurrentSpan(span);
            clientTracer.setClientReceived();
        }
        openSpans.clear();
    }

    private void traceImplWithSpans(String message)
    {
        Brave.getServerSpanThreadBinder().setCurrentSpan(serverSpan);
        if (null != previous.get())
        {
            clientTracer.setClientReceived();
            openSpans.remove(previous.get());
        }
        clientTracer.startNewSpan(message + " [" + Thread.currentThread().getName() + "]");
        clientTracer.setClientSent();
        Span prev = Brave.getClientSpanThreadBinder().getCurrentClientSpan();
        previous.set(prev);
        openSpans.addLast(prev);
    }

    private void traceImplUsingAnnotations(String message)
    {
        long startTime = System.currentTimeMillis() * 1000 + (TimeUnit.NANOSECONDS.toMicros(System.nanoTime()) % 1000);

        Annotation annotation = new Annotation();
        annotation.setTimestamp(startTime);
        annotation.setValue(message);
        synchronized (serverSpan.getSpan())
        {
            serverSpan.getSpan().addToAnnotations(annotation);
        }
    }
}
