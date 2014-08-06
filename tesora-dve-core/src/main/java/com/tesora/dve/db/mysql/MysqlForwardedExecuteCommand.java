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
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;

public class MysqlForwardedExecuteCommand extends MysqlConcurrentCommand {
	
	static Logger logger = Logger.getLogger( MysqlForwardedExecuteCommand.class );

	final MysqlMultiSiteCommandResultsProcessor resultsHandler;
	final StorageSite site;

	public MysqlForwardedExecuteCommand(MysqlMultiSiteCommandResultsProcessor resultHandler, CompletionHandle<Boolean> completionPromise, StorageSite site) {
		super(completionPromise);
		this.resultsHandler = resultHandler;
		this.site = site;
	}

	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug("Written: " + this);
		resultsHandler.addSite(site, ctx);
	}

	@Override
	public MysqlCommandResultsProcessor getResultHandler() {
		return resultsHandler;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"{"+ getCompletionHandle()+", " + site + "}";
	}

	@Override
	public boolean isPreemptable() {
		return true;
	}
}
