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

/**
 *
 */
public class DelegatingResultsProcessor implements MysqlCommandResultsProcessor {
    MysqlCommandResultsProcessor delegate;

    public DelegatingResultsProcessor(MysqlCommandResultsProcessor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        delegate.active(ctx);
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        return delegate.processPacket(ctx, message);
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) throws PEException {
        delegate.packetStall(ctx);
    }

    @Override
    public void failure(Exception e) {
        delegate.failure(e);
    }

    @Override
    public void end(ChannelHandlerContext ctx) {
        delegate.end(ctx);
    }
}
