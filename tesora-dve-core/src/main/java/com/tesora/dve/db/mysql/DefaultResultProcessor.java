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

import com.tesora.dve.concurrent.CompletionHandle;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.ChannelHandlerContext;

/**
 *
 */
public class DefaultResultProcessor implements MysqlCommandResultsProcessor, CompletionHandle<Boolean> {
    CompletionHandle<Boolean> promise;

    public DefaultResultProcessor(CompletionHandle<Boolean> promise) {
        this.promise = promise;
    }

    @Override
    public void active(ChannelHandlerContext ctx) { }

    @Override
    public boolean isDone(ChannelHandlerContext ctx) {
        return promise.isFulfilled();
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        return isDone(ctx);
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) throws PEException {
    }

    @Override
    public void success(Boolean returnValue) {
        promise.success(returnValue);
    }

    @Override
    public void failure(Exception e) {
        promise.failure(e);
    }

    @Override
    public boolean trySuccess(Boolean returnValue) {
        return promise.trySuccess(returnValue);
    }

    @Override
    public boolean isFulfilled() {
        return promise.isFulfilled();
    }

}
