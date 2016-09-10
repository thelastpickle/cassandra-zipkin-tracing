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
import com.github.kristofa.brave.ServerSpan;
import com.twitter.zipkin.gen.Annotation;
import com.twitter.zipkin.gen.Span;
import org.apache.cassandra.tracing.TraceState;
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
    private final Brave brave;
    private final ServerSpan serverSpan;

    // re-using property from TraceStateImpl.java
    private static final int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
      Integer.valueOf(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "1"));

    final Deque<Span> openSpans = new ConcurrentLinkedDeque();
    private final ThreadLocal<Span> currentSpan = new ThreadLocal<>();

    public ZipkinTraceState(
            Brave brave,
            InetAddress coordinator,
            UUID sessionId,
            Tracing.TraceType traceType,
            ServerSpan serverSpan)
    {
        super(coordinator, sessionId, traceType);
        assert null != serverSpan;
        this.brave = brave;
        this.serverSpan = serverSpan;
    }

    @Override
    protected void traceImpl(String message)
    {
        traceImplWithClientSpans(message);
    }

    void close()
    {
        brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);
        closeClientSpans();
    }

    private void traceImplWithClientSpans(String message)
    {
        brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);
        if (null != currentSpan.get())
        {
            brave.clientTracer().setClientReceived();
            openSpans.remove(currentSpan.get());
            currentSpan.remove();
        }
        brave.clientTracer().startNewSpan(message + " [" + Thread.currentThread().getName() + "]");
        brave.clientTracer().setClientSent();
        Span prev = brave.clientSpanThreadBinder().getCurrentClientSpan();
        currentSpan.set(prev);
        openSpans.addLast(prev);
    }

    private void closeClientSpans()
    {
        for (Span span : openSpans)
        {
            brave.clientSpanThreadBinder().setCurrentSpan(span);
            brave.localTracer().finishSpan();
        }
        openSpans.clear();
        currentSpan.remove();
    }

    private void traceImplWithLocalSpans(String message)
    {
//        brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);
//        if (null != currentSpan.get())
//        {
//            brave.localTracer().finishSpan();
//            openSpans.remove(currentSpan.get());
//            currentSpan.remove();
//        }
//        brave.localTracer().startNewSpan("jvm", message + " [" + Thread.currentThread().getName() + "]");
//        Span prev = brave.localSpanThreadBinder().getCurrentClientSpan();
//        currentSpan.set(prev);
//        openSpans.addLast(prev);
    }

    private void closeLocalSpans()
    {
//        for (Span span : openSpans)
//        {
//            brave.localSpanThreadBinder().setCurrentSpan(span);
//            brave.localTracer().finishSpan();
//        }
//        openSpans.clear();
//        currentSpan.remove();
    }

    private void traceImplUsingAnnotations(String message)
    {
        long startTime = System.currentTimeMillis() * 1000 + (TimeUnit.NANOSECONDS.toMicros(System.nanoTime()) % 1000);

        Annotation annotation = Annotation.create(startTime, message, null);
        synchronized (serverSpan.getSpan())
        {
            serverSpan.getSpan().addToAnnotations(annotation);
        }
    }

    @Override
    protected void waitForPendingEvents() {
        int sleepTime = 100;
        int maxAttempts = WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS / sleepTime;
        for (int i = 0; 0 < openSpans.size() && i < maxAttempts ; ++i)
        {
            try
            {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex)
            {
            }
        }
    }
}
