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

import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

/**
*
*/
public class SimpleMysqlCommandBundle implements MysqlCommandBundle, MysqlCommandRequestProcessor, MysqlCommandResultsProcessor {
    //these Timers are used to measure how much time is being spent on the backend for a given frontend request.
    //the frontend timer is picked up from a thread local, since changing all the subclasses of MysqlCommand and their callers would be prohibitive.

    Timer frontendTimer = Singletons.require(TimingService.class).getTimerOnThread();

    //a place for MysqlCommandSenderHandler to hang backend timing info for this command.  Not great encapsulation / ood, ,but makes life much easier.
    protected Timer commandTimer;

    MysqlCommandRequestProcessor requestProcessor;
    MysqlCommandResultsProcessor  responseProcessor;

    public SimpleMysqlCommandBundle(MysqlCommandBundle otherBundle) {
        this.requestProcessor = otherBundle.getRequestProcessor();
        this.responseProcessor = otherBundle.getResponseProcessor();
    }

    public SimpleMysqlCommandBundle(MysqlCommandRequestProcessor requestProcessor, MysqlCommandResultsProcessor responseProcessor) {
        this.requestProcessor = requestProcessor;
        this.responseProcessor = responseProcessor;
    }

    @Override
    public MysqlCommandRequestProcessor getRequestProcessor() {
        return requestProcessor;
    }

    @Override
    public MysqlCommandResultsProcessor  getResponseProcessor() {
        return responseProcessor;
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        responseProcessor.active(ctx);
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        return responseProcessor.processPacket(ctx,message);
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) throws PEException {
        responseProcessor.packetStall(ctx);
    }

    public void failure(Exception e) {
        responseProcessor.failure(e);
    }

    @Override
    public void end(ChannelHandlerContext ctx) {
        responseProcessor.end(ctx);
    }

    @Override
    public void executeInContext(ChannelHandlerContext ctx, Charset charset) throws PEException {
        requestProcessor.executeInContext(ctx,charset);
    }

    @Override
    public boolean isExpectingResults(ChannelHandlerContext ctx) {
        return requestProcessor.isExpectingResults(ctx);
    }

    public static SimpleMysqlCommandBundle bundle(MysqlCommandBundle command){
        if (command instanceof SimpleMysqlCommandBundle)
            return (SimpleMysqlCommandBundle)command;
        else
            return new SimpleMysqlCommandBundle(command.getRequestProcessor(),command.getResponseProcessor());
    }

    public static SimpleMysqlCommandBundle bundle(MysqlCommandRequestProcessor requestProcessor, MysqlCommandResultsProcessor responseProcessor){
        return new SimpleMysqlCommandBundle(requestProcessor,responseProcessor);
    }

    public static SimpleMysqlCommandBundle bundle(MysqlMessage message, MysqlCommandResultsProcessor results){
        return new SimpleMysqlCommandBundle(new SimpleRequestProcessor(message, false,true),results);
    }

}
