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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
*
*/
public class StreamValve extends ChannelInboundHandlerAdapter {
    static Logger logger = LoggerFactory.getLogger(StreamValve.class);

    ChannelHandlerContext savedContext;

    //this queue needs to be unbounded, since we don't know how many messages might already in the pipeline and must be forwarded (IE, already decoded in a ByteToMessageDecoder).
    //using a linked list allows memory to be reclaimed, and avoids an arraycopy on remove.  Unfortunately it also creates objects for each add.
    Queue<Object> bufferedReads = new LinkedList<>();
    boolean paused;

    static final ThreadLocal<StreamValve> currentValve = new ThreadLocal<>();
    Runnable scheduledRunnable = null;

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        //track which pipeline we are on.
        this.savedContext = ctx;
        logger.debug("{} registered on {}",this,ctx);
        super.channelRegistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        //enqueue behind any existing packets.
        bufferedReads.add(msg);

        if (paused)
            logger.debug("{} is paused, buffering read ==> {}",this,msg);
        else
            forwardQueuedReads();
    }

    private void forwardQueuedReads() {
        StreamValve current = currentValve.get();
        if (current == this || scheduledRunnable != null){
            logger.debug("ignoring request to deliver messages on {}, thread {} has future opportunity to.  current={},  scheduled={}",new Object[]{this,Thread.currentThread().getName(),current,scheduledRunnable});
            return;
        } else if (current != null) {
            logger.debug("scheduling later delivery on {}, thread {} already delivering messages on {}",new Object[]{this,Thread.currentThread().getName(),current});
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    scheduledRunnable = null;
                    forwardQueuedReads();
                }
            };
            scheduledRunnable = task;
            savedContext.executor().submit(task);
            return;
        }

        logger.debug("no scheduled opportunity to deliver messages on {}, thread {} starting delivery.",new Object[]{this,Thread.currentThread().getName()});

        currentValve.set(this);
        try {
            while (!paused && !bufferedReads.isEmpty()) {
                Object next = bufferedReads.remove();
                logger.debug("{} forwarding read ==> {}", this, next);

                //NOTE: callee might invoke this.pause() or this.resume() on this thread before returning.
                savedContext.fireChannelRead(next);
            }

            if (!paused && bufferedReads.isEmpty()) {
                //valve is open and nothing is buffered, enable the channels autoread again.
                logger.debug("{} out of messages and not paused, ensuring channel autoread is enabled", this);
                savedContext.channel().config().setAutoRead(true);
            }
        } finally {
            currentValve.set(null);
        }
    }


    public boolean isPaused() {
        return paused;
    }

    public void pause(ChannelPipeline pipeline){
        validateInLoopAndPipeline(pipeline);
        if (!paused){
            logger.info("{} flow paused", this);
            this.paused = true;
            pipeline.channel().config().setAutoRead(false);
        }
    }

    public void resume(ChannelPipeline pipeline){
        validateInLoopAndPipeline(pipeline); 
        //we deliver this check is needed to avoid recursing if someone calls resume on open valve.
        if (paused){
            this.paused = false;
            logger.info("{} flow resumed",this);
            this.forwardQueuedReads();
        }
    }

    public void validateInLoopAndPipeline(ChannelPipeline otherPipeline) {
        if (!this.savedContext.executor().inEventLoop()){
            throw new IllegalStateException("Cannot request pause/resume outside of event loop, current thread is "+Thread.currentThread());
        }

        if (this.savedContext.pipeline() != otherPipeline){
            logger.warn("{} ");
            throw new IllegalStateException("Cannot request pause/resume from different pipeline");
        }
    }


    //********Static utility methods to reduce boilerplate in handlers.

    public static boolean isPipelinePaused(ChannelPipeline pipeline){
        StreamValve streamValve = locateValve(pipeline);
        return streamValve.isPaused();
    }


    public static void pipelinePause(ChannelPipeline pipeline){
        StreamValve streamValve = locateValve(pipeline);
        streamValve.pause(pipeline);
    }

    public static void pipelineResume(ChannelPipeline pipeline){
        StreamValve streamValve = locateValve(pipeline);
        streamValve.resume(pipeline);
    }

    public static StreamValve locateValve(ChannelPipeline pipeline) throws NoSuchElementException {
        //TODO: this assumes only one StreamValve in a pipeline. -sgossard
        ChannelHandlerContext context = pipeline.context(StreamValve.class);

        if (context == null) //TODO: if users wind up calling this directly, it would be nice to provide a no-op version on request. -sgossard
            throw new NoSuchElementException("PauseResume not configured on the current pipeline");

        //cast shouldn't fail, we looked up the context by handler class.
        return (StreamValve) context.handler();
    }


    public static FlowControl wrap(final ChannelHandlerContext ctx) {
        final ChannelPipeline pipeline = ctx.pipeline();
        return new FlowControl() {
            @Override
            public void pauseSourceStreams() {
                StreamValve valve = locateValve(pipeline);
                valve.pause(pipeline);
            }

            @Override
            public void resumeSourceStreams() {
                StreamValve valve = locateValve(pipeline);
                valve.resume(pipeline);
            }
        };
    }
}
