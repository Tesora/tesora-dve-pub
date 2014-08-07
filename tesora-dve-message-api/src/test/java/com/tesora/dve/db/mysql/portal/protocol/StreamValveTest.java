package com.tesora.dve.db.mysql.portal.protocol;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.NoSuchElementException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 *
 */

public class StreamValveTest {

    @Mock private ChannelHandlerContext mockRegisteredContext;
    @Mock private EventExecutor mockLoop;
    @Mock private ChannelPipeline mockPipeline;
    @Mock private ChannelConfig mockConfig;

    @Mock private ChannelPipeline mockProvidedPipeline;
    @Mock private ChannelHandlerContext mockProvidedContext;
    @Mock private StreamValve mockValve;
    @Mock private ChannelHandler mockProvidedHandler;


    StreamValve valve;


    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(mockProvidedPipeline.context(StreamValve.class)).thenReturn(mockProvidedContext);
        when(mockProvidedContext.handler()).thenReturn(mockValve);

        //pre-construct and register valve, it's expected for almost every test.
        valve = new StreamValve();
        valve.channelRegistered(mockRegisteredContext);

        verify(mockRegisteredContext, times(1)).fireChannelRegistered();
    }

    @Test(expected = NullPointerException.class)
    public void testChannelRegisteredRequiresContext() throws Exception {
        new StreamValve().channelRegistered(null);
    }

    @Test(expected = NoSuchElementException.class)
    public void testLocateValveFailsIfNotInPipeline() throws Exception {
        reset(mockProvidedPipeline);

        when(mockProvidedPipeline.context(StreamValve.class)).thenReturn(null);

        StreamValve.locateValve(mockProvidedPipeline);
    }

    @Test
    public void testLocateValveAsksProvidedContextForHandler() throws Exception {

        StreamValve locatedValve = StreamValve.locateValve(mockProvidedPipeline);

        verify(mockProvidedPipeline,atLeast(1)).context(StreamValve.class);
        verify(mockProvidedContext,atLeast(1)).handler();

        assertEquals(mockValve, locatedValve);
    }

}
