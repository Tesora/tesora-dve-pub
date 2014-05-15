// OS_STATUS: public
package com.tesora.dve.db.mysql;

import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyLoadDataResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;

public class MysqlLoadDataInfileRequestCollector {

	ChannelHandlerContext ctx = null;
	String fileName = null;
	byte sequenceId;

	public MysqlLoadDataInfileRequestCollector(ChannelHandlerContext ctx,
			byte sequenceId) {
		this.ctx = ctx;
		this.sequenceId = sequenceId;
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
		ctx.write(new MyLoadDataResponse(getFileName())
				.withPacketNumber(sequenceId + 1));
		ctx.flush();
	}

	public void sendError(Exception e) {
		MyMessage respMsg = new MyErrorResponse(e);
		respMsg.setPacketNumber(++sequenceId);
		ctx.write(respMsg);
		ctx.flush();
	}
}
