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
import com.tesora.dve.exceptions.PECodingException;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

public class MysqlForwardedExecuteCommand extends MysqlConcurrentCommand {
	
	static Logger logger = Logger.getLogger( MysqlForwardedExecuteCommand.class );

	final MysqlMultiSiteCommandResultsProcessor resultsHandler;
    StorageSite site;
    boolean siteHasBeenRegistered = false;

	public MysqlForwardedExecuteCommand(StorageSite storageSite, MysqlMultiSiteCommandResultsProcessor resultHandler, CompletionHandle<Boolean> completionPromise) {
		super(completionPromise);
        this.site = storageSite;
		this.resultsHandler = resultHandler;
	}

	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug("Written: " + this);
        //TODO: this would be cleaner in active(), but it apparently doesn't get called until ready to process a response. -sgossard
        registerWithBuilder(ctx);
    }

    @Override
    public boolean isExpectingResults(ChannelHandlerContext ctx) {
        return false;
    }

    private void registerWithBuilder(ChannelHandlerContext ctx) {
        this.resultsHandler.addSite(site,ctx);
        siteHasBeenRegistered = true;
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        resultsHandler.active(ctx);
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        throw new PECodingException("Should never receive a packet, designed to gather site information only.");
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) throws PEException {
    }

    @Override
    public void failure(Exception e) {
        resultsHandler.failure(e);
    }

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"{"+ getCompletionHandle()+"}";
	}

}
