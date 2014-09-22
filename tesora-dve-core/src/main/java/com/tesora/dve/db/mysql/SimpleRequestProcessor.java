package com.tesora.dve.db.mysql;

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

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

/**
 *
 */
public class SimpleRequestProcessor implements MysqlCommandRequestProcessor {
    final MysqlMessage outboundMessage;
    final boolean shouldFlush;
    final boolean expectingResults;

    public SimpleRequestProcessor(MysqlMessage outboundMessage) {
        this(outboundMessage,false, true);
    }

    public SimpleRequestProcessor(MysqlMessage outboundMessage, boolean shouldFlush, boolean expectingResults) {
        this.outboundMessage = outboundMessage;
        this.shouldFlush = shouldFlush;
        this.expectingResults = expectingResults;
    }

    @Override
    public void executeInContext(ChannelHandlerContext ctx, Charset charset) throws PEException {
        if (shouldFlush)
            ctx.writeAndFlush(outboundMessage);
        else
            ctx.write(outboundMessage);
    }

    @Override
    public boolean isExpectingResults(ChannelHandlerContext ctx) {
        return expectingResults;
    }
}
