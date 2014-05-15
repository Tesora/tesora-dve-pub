// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEDefaultPromise;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.concurrent.PEFuture.Listener;
import com.tesora.dve.db.DBConnection.Monitor;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlPrepareStatementForwarder extends MysqlPrepareParallelConsumer {

	private ChannelHandlerContext outboundCtx;
	MyPreparedStatement<String> outboundPStmt;
	
	public MysqlPrepareStatementForwarder(ChannelHandlerContext outboundCtx, MyPreparedStatement<String> pstmt) {
		this.outboundPStmt = pstmt;
		this.outboundCtx = outboundCtx;
	}

	@Override
	public void consumeHeader(MyPrepareOKResponse preparedOK) {
        MyPrepareOKResponse copyPrepareOK = new MyPrepareOKResponse(preparedOK);//copy since we are mutating the id.
        int outboundID = Long.valueOf(outboundPStmt.getStmtId()).intValue();
        copyPrepareOK.setStmtId( outboundID );
		outboundCtx.write(copyPrepareOK);
		outboundCtx.flush();
		outboundPStmt.setNumParams(getNumParams());
	}

	@Override
	public PEFuture<Boolean> writeCommandExecutor(final Channel channel, StorageSite site, Monitor connectionMonitor, SQLCommand sql,
			final PEPromise<Boolean> promise) {
		final MysqlPrepareStatementForwarder resultForwarder = this;
		final PEDefaultPromise<Boolean> preparePromise = new PEDefaultPromise<Boolean>();
		preparePromise.addListener(new Listener<Boolean>() {
			@Override
			public void onSuccess(Boolean returnValue) {
				MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt = resultForwarder.getPreparedStatement();
				channel.write(new MysqlStmtCloseCommand(pstmt));
				channel.flush();
				promise.success(false);
			}
			@Override
			public void onFailure(Exception e) {
				promise.failure(e);
			}
		});
		super.writeCommandExecutor(channel, site, connectionMonitor, sql, preparePromise);
		return promise;
	}

	@Override
	public void consumeParamDef(MyFieldPktResponse paramDef) {
		outboundCtx.write(paramDef);
	}

	@Override
	public void consumeParamDefEOF(MyEOFPktResponse myEof) {
		outboundCtx.write(myEof);
		outboundCtx.flush();
	}

	@Override
	public void consumeColDef(MyFieldPktResponse columnDef) {
		outboundCtx.write(columnDef);
	}

	@Override
	public void consumeColDefEOF(MyEOFPktResponse colEof) {
		outboundCtx.write(colEof);
		outboundCtx.flush();
	}

	@Override
	public void consumeError(MyErrorResponse error) {
		outboundCtx.write(error);
		outboundCtx.flush();
	}


	public void sendError(Exception e) {
        Throwable rootCause = e;

        while (rootCause != null){
            rootCause = rootCause.getCause();
            if (rootCause instanceof PESQLStateException)
                return;//we've already forwarded sql error responses directly back to the client in consumeError().
        }

		MyMessage respMsg = new MyErrorResponse(e);
		respMsg.setPacketNumber(1);
		outboundCtx.write(respMsg);
		outboundCtx.flush();
	}

}
