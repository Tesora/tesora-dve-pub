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

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class ValveFlowControlSet implements FlowControl {
    IdentityHashMap<ChannelHandlerContext, FlowControl> contextToFC = new IdentityHashMap<>();
    UpstreamFlowControlSet flowControlSet = new UpstreamFlowControlSet();

    public void register(ChannelHandlerContext ctx){
        if (!contextToFC.containsKey(ctx)){
            FlowControl fc = StreamValve.wrap(ctx);
            contextToFC.put(ctx,fc);
            flowControlSet.registerUpstream(fc);
        }
    }

    public void clear(){
        for (FlowControl fc : contextToFC.values())
            flowControlSet.unregisterUpstream(fc);
        contextToFC.clear();
    }

    public void unregister(ChannelHandlerContext ctx){
        FlowControl fc = contextToFC.remove(ctx);
        if (fc != null){
            flowControlSet.unregisterUpstream(fc);
            fc.resumeSourceStreams();
        }
    }


    @Override
    public void pauseSourceStreams() {
        flowControlSet.pauseSourceStreams();
    }

    @Override
    public void resumeSourceStreams() {
        flowControlSet.resumeSourceStreams();
    }
}
