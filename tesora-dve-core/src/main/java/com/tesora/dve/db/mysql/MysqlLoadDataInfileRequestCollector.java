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

import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyLoadDataResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;

public class MysqlLoadDataInfileRequestCollector {

	ChannelHandlerContext ctx = null;
	String fileName = null;

	public MysqlLoadDataInfileRequestCollector(ChannelHandlerContext ctx) {
		this.ctx = ctx;
	}

	public ChannelHandlerContext getCtx() {
		return ctx;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public MyLoadDataInfileContext getLoadDataInfileContext() {
		MyLoadDataInfileContext loadDataInfileCtx = ctx.attr(
				MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).get();
		if (loadDataInfileCtx == null) {
			loadDataInfileCtx = new MyLoadDataInfileContext();
			ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).set(
					loadDataInfileCtx);
		}

		return loadDataInfileCtx;
	}

	public void sendStartDataRequest() {
		ctx.writeAndFlush(new MyLoadDataResponse(getFileName()));
	}

	public void sendError(Exception e) {
		MyMessage respMsg = new MyErrorResponse(e);
		ctx.writeAndFlush(respMsg);
	}
}
