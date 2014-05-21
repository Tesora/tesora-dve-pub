// OS_STATUS: public
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

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.exceptions.PESQLStateException;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.DBConnection.Monitor;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlExecuteCommand extends MysqlConcurrentCommand implements MysqlCommandResultsProcessor {

	static Logger logger = Logger.getLogger( MysqlExecuteCommand.class );


    enum ResponseState { AWAIT_FIELD_COUNT, AWAIT_FIELD, AWAIT_FIELD_EOF, AWAIT_ROW, DONE }

    ResponseState messageState = ResponseState.AWAIT_FIELD_COUNT;
    private int messageFieldCount;

	SQLCommand sqlCommand;
	MysqlQueryResultConsumer resultConsumer;

	private int field = 0;
    private int writtenFrames;
	private Monitor connectionMonitor;

	public MysqlExecuteCommand(SQLCommand sqlCommand,
			Monitor connectionMonitor, MysqlQueryResultConsumer resultConsumer, PEPromise<Boolean> promise) {
		super(promise);
		this.sqlCommand = sqlCommand;
		this.resultConsumer = resultConsumer;
		this.connectionMonitor = connectionMonitor;
	}

	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug("Written: " + this);

        MSPComQueryRequestMessage queryMsg = new MSPComQueryRequestMessage((byte)0,sqlCommand.getSQLAsBytes());
        ctx.write(queryMsg);
    }

	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "{" + getPromise() + ", " + sqlCommand.getDisplayForLog() + "}";
	}



    @Override
    public boolean isDone(ChannelHandlerContext ctx) {
        return messageState == ResponseState.DONE;
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) throws PEException {
        flushIfNeeded();
    }

    //this is an execute response, which returns a ERR, OK for zero rows, or for N rows, [FIELD COUNT, N*fields, an EOF, M*rows, final EOF]
	@Override
	public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {

        try{
            switch (messageState) {
                case AWAIT_ROW:
                    processAwaitRow(message);
                    break;
                case AWAIT_FIELD_COUNT:
                    processAwaitFieldCount(message);
                    break;
                case AWAIT_FIELD:
                    processAwaitField(message);
                    break;
                case AWAIT_FIELD_EOF:
                    processAwaitFieldEOF(message);
                    break;
                default:
                    throw new PECodingException("Unrecognized message state " + messageState + " occurred while processing packets in " + this.getClass().getSimpleName());
            }
            return isDone(ctx);
        } catch (Exception e){
            getPromise().failure(e);
            throw e;
        }
	}



    public void processAwaitFieldEOF(MyMessage message) throws PEException {
        messageState = ResponseState.AWAIT_ROW;
        MyMessage raw = message;

		if (!getPromise().isFulfilled()) {
			resultConsumer.fieldEOF(raw);
		}
	}

    public void processAwaitField(MyMessage message)
			throws PEException {
        if (--messageFieldCount == 0)
            messageState = ResponseState.AWAIT_FIELD_EOF;

        MyFieldPktResponse columnDef = (MyFieldPktResponse)message;

		if (!getPromise().isFulfilled()) {
			ColumnInfo columnProjection = null;
			if (sqlCommand.getProjection() != null)
				columnProjection = sqlCommand.getProjection().getColumnInfo(field+1);

            resultConsumer.field(field++, columnDef, columnProjection);
		}
	}



    public void processAwaitFieldCount(MyMessage message) throws PEException {

        MyMessageType messageType = message.getMessageType();

        switch (messageType) {
            case OK_RESPONSE:
                messageState = ResponseState.DONE;
                MyOKResponse ok = (MyOKResponse)message;
                if (resultConsumer.emptyResultSet(ok) && connectionMonitor != null)
                    connectionMonitor.onUpdate();
                getPromise().success(true);
                break;
            case ERROR_RESPONSE:
                messageState = ResponseState.DONE;
                MyErrorResponse errorResponse = (MyErrorResponse)message;
                resultConsumer.error(errorResponse);
                getPromise().failure(new PEMysqlErrorException(errorResponse.asException()));
                break;
            case RESULTSET_RESPONSE:
                messageState = ResponseState.AWAIT_FIELD;
                MyColumnCount columnCount = (MyColumnCount)message;
                this.messageFieldCount = columnCount.getColumnCount();
                resultConsumer.fieldCount(columnCount);
                break;
            default:
                throw new PEException("Received unexpected packet type "+messageType+" in " + this.getClass().getSimpleName());
        }
    }



    public void processAwaitRow(MyMessage message) throws PEException {
        if (message instanceof MyEOFPktResponse){
            messageState = ResponseState.DONE;
            resultConsumer.rowEOF((MyEOFPktResponse) message);
            if (!getPromise().isFulfilled()) {
                getPromise().success(true);
            }
        } else if (message instanceof MyBinaryResultRow){
            writtenFrames++;
            resultConsumer.rowBinary((MyBinaryResultRow) message);
        } else if (message instanceof MyTextResultRow){
            writtenFrames++;
            resultConsumer.rowText((MyTextResultRow)message);
        } else {
            throw new PEException("Unexpected message type " + message.getClass().getSimpleName() + " received by "+this.getClass().getSimpleName());
        }
    }

    public void flushIfNeeded() throws PEException {
        if (writtenFrames > 0) {
            resultConsumer.rowFlush();
            writtenFrames = 0;
        }
    }

    @Override
	public MysqlCommandResultsProcessor getResultHandler() {
		return this;
	}

	@Override
	public void failure(Exception e) {
		getPromise().failure(e);
	}

}
