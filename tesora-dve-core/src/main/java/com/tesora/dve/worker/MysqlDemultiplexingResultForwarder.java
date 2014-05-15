// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.exceptions.PEException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.logging.LoggingHandler;

import org.apache.log4j.Logger;

import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;

public abstract class MysqlDemultiplexingResultForwarder extends MysqlParallelResultConsumer {
	
	private static final Logger logger = Logger.getLogger(MysqlDemultiplexingResultForwarder.class);

	static public class ByteLogger extends LoggingHandler {

        public void printBuffer(String message, MyMessage recv) {
//			System.out.println(formatByteBuf(message, buf));
        }
	}

	protected static ByteLogger byteLogger = new ByteLogger();
	
	protected final ChannelHandlerContext outboundCtx;
	protected byte sequenceId = 0;
	long tmr;

	private long resultsLimit = Long.MAX_VALUE;
	long rowCount = 0;

	public MysqlDemultiplexingResultForwarder(ChannelHandlerContext outboundCtx) {
		this.outboundCtx = outboundCtx;
	}

	public MysqlDemultiplexingResultForwarder(ChannelHandlerContext outboundCtx, byte sequenceId) {
		this.outboundCtx = outboundCtx;
		this.sequenceId = sequenceId;
	}


    @Override
    public void consumeEmptyResultSet(MyOKResponse ok) {
        byteLogger.printBuffer("consumeEmptyResultSet", ok);
    }


	@Override
	public void consumeError(MyErrorResponse errorResp) {
		byteLogger.printBuffer("consumeError", errorResp);
		outboundCtx.write(errorResp);
        outboundCtx.flush();
		if (logger.isDebugEnabled()) {
			logger.debug("Error forwarded to user: " + errorResp);
		}
	}


    @Override
    public void consumeFieldCount(MyColumnCount colCount) {
        byteLogger.printBuffer("consumeFieldCount", colCount);
        tmr = System.currentTimeMillis();
        sendMessage(colCount);
    }

    public void sendMessage(MyMessage msg) {
        msg.setPacketNumber(++sequenceId);
        byteLogger.printBuffer("writtenMessage", msg);
        outboundCtx.write(msg);
    }

    @Override
    public void consumeField(int field_, MyFieldPktResponse columnDef, ColumnInfo columnInfo) {
		if (columnInfo == null) {
			byteLogger.printBuffer("consumeField", columnDef);
			sendMessage(columnDef);
		} else {
            columnDef.setColumn(columnInfo.getAlias());
			columnDef.setOrig_column(columnInfo.getName());
			sendMessage(columnDef);
		}
	}

	@Override
	public void consumeFieldEOF(MyMessage someMessage) {
		byteLogger.printBuffer("consumeFieldEOF", someMessage);
		sendMessage(someMessage);
		outboundCtx.flush();
	}


	@Override
	public void consumeRowEOF() {
//		byteLogger.printBuffer("consumeRowEOF", wholePacket);
//		writePacket(wholePacket);
//		System.out.println("Time1: " + 1.0 * (System.currentTimeMillis() - tmr)/1000);
//		outboundCtx.flush().addListener(new ChannelFutureListener() {
//			@Override
//			public void operationComplete(ChannelFuture future) throws Exception {
//				System.out.println("Time2: " + 1.0 * (System.currentTimeMillis() - tmr)/1000);
//			}
//		});
	}

    public void consumeRowText(MyTextResultRow textRow){
        handleUnknownRow(textRow);
    }

    public void consumeRowBinary(MyBinaryResultRow binRow){
        handleUnknownRow(binRow);
    }

    @Override
    public void rowFlush() throws PEException {
        outboundCtx.flush();
    }

    public void handleUnknownRow(MyMessage row) {
        if (rowCount >= resultsLimit)
            return;

        rowCount++;
        row.setPacketNumber(++sequenceId);
        outboundCtx.write(row);
    }


    public byte getSequenceId() {
		return sequenceId;
	}


	@Override
	public void setResultsLimit(long resultsLimit) {
		this.resultsLimit = resultsLimit;
	}


	public long getResultsLimit() {
		return resultsLimit;
	}


	public void sendSuccess(SSConnection ssConn) {
		if (hasResults())
			sendEOFPacket(ssConn);
		else 
			sendOKPacket(ssConn);
	}
	
	/**
	 * @param ssConn
	 */
	private void sendEOFPacket(SSConnection ssConn) {
		MyEOFPktResponse eofPacket1 = new MyEOFPktResponse();
		eofPacket1.setPacketNumber(++sequenceId);
		eofPacket1.setStatusFlags(statusFlags);
		eofPacket1.setWarningCount(warnings);
        outboundCtx.write(eofPacket1);
	}


	private void sendOKPacket(SSConnection ssConn) {
		MyOKResponse okPacket1 = new MyOKResponse();
        byte sendingPacketNumber = ++sequenceId;
        okPacket1.setPacketNumber(sendingPacketNumber);
		okPacket1.setAffectedRows(numRowsAffected);
		okPacket1.setServerStatus(statusFlags);
		okPacket1.setInsertId(ssConn.getLastInsertedId());
		okPacket1.setWarningCount(ssConn.getMessageManager().getNumberOfMessages());
		okPacket1.setMessage(infoString);
		outboundCtx.write(okPacket1);
	}


	public void sendError(Exception e) {
		MyMessage respMsg = new MyErrorResponse(e);
		respMsg.setPacketNumber(++sequenceId);
		outboundCtx.write(respMsg);
		outboundCtx.flush();
	}

}
