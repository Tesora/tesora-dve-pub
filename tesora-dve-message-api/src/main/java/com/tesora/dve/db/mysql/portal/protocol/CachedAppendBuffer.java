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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteOrder;

/**
*
*/
public class CachedAppendBuffer {
    public static final int SLAB_SIZE = 1024 * 8;//start with a 8K slab for transcoding outbound writes.
    static boolean PREFER_DIRECT = PlatformDependent.directBufferPreferred();

    int appendStartedAtIndex;
    ByteBuf cachedSlab;

    public void allocateSlabIfNeeded(ChannelHandlerContext ctx) {
        if (cachedSlab != null) //we already have a slab.
            return;

        if (PREFER_DIRECT) {
            cachedSlab = ctx.alloc().ioBuffer(SLAB_SIZE);
        } else {
            cachedSlab = ctx.alloc().heapBuffer(SLAB_SIZE);
        }

        cachedSlab = cachedSlab.order(ByteOrder.LITTLE_ENDIAN);
    }

    public ByteBuf startAppend(ChannelHandlerContext ctx){
        allocateSlabIfNeeded(ctx);

        //if we don't have any outbound slices holding references, reset to full slab.
        if (cachedSlab.refCnt() == 1){
            cachedSlab.clear();
        }

        appendStartedAtIndex = cachedSlab.writerIndex();
        return cachedSlab;
    }

    public ByteBuf sliceWritableData(){
        int writtenLength = cachedSlab.writerIndex() - appendStartedAtIndex;

        if (writtenLength <= 0)
            return Unpooled.EMPTY_BUFFER;
        else {
            ByteBuf sliceSinceStarted = cachedSlab.slice(appendStartedAtIndex, writtenLength).retain(); //this will be written out to netty, so inc the ref count.
            return sliceSinceStarted;
        }
    }



    public void releaseSlab() {
        ReferenceCountUtil.release(cachedSlab);
        cachedSlab = null;
    }
}
