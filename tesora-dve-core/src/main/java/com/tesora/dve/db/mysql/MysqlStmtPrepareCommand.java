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
import com.tesora.dve.db.CommandChannel;
import com.tesora.dve.db.mysql.libmy.*;
import io.netty.channel.ChannelHandlerContext;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class MysqlStmtPrepareCommand extends MysqlCommand {

	static Logger logger = Logger.getLogger(MysqlStmtPrepareCommand.class);
    private CompletionHandle<Boolean> promise;

    enum ResponseState { AWAIT_HEADER, AWAIT_COL_DEF, AWAIT_COL_DEF_EOF, AWAIT_PARAM_DEF, AWAIT_PARAM_DEF_EOF, DONE };
	ResponseState state = ResponseState.AWAIT_HEADER;

	String sqlCommand;
    private CommandChannel executingOnChannel;
	private MysqlPrepareParallelConsumer consumer;

	private int numCols;
	private int numParams;

    public MysqlStmtPrepareCommand(CommandChannel executingOnChannel, String sql,
                                   MysqlPrepareParallelConsumer mysqlStatementPrepareConsumer, CompletionHandle<Boolean> promise) {
        super();
        this.executingOnChannel = executingOnChannel;
        this.promise = promise;
        this.sqlCommand = sql;
		this.consumer = mysqlStatementPrepareConsumer;
	}

    @Override
    public void active(ChannelHandlerContext ctx) {
        //NOOP.
    }


    //this is a prepare response, which returns a ERR, or [PREPARE_OK, N*param defs, an EOF, M*column defs, final EOF]
	@Override
	public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
		
        try {
			switch (state) {
                case AWAIT_HEADER:
				if (message instanceof MyPrepareOKResponse) {
                    MyPrepareOKResponse prepareOK = (MyPrepareOKResponse)message;
					numCols = prepareOK.getNumColumns();
					numParams = prepareOK.getNumParams();
					//				System.out.println("cols " + numCols + ", params " + numParams);
					if (numParams > 0)
						state = ResponseState.AWAIT_PARAM_DEF;
					else if (numCols > 0)
						state = ResponseState.AWAIT_COL_DEF;
					else
						state = ResponseState.DONE;

					consumer.header(executingOnChannel,ctx, prepareOK);

					if (state == ResponseState.DONE)
						onCompletion();
				} else if (message instanceof MyErrorResponse) {
                    MyErrorResponse error = (MyErrorResponse)message;
					try {
						consumer.errorResponse(ctx, error);
                        promise.failure(error.asException());
					} catch (Exception e) {
                        promise.failure(e);
					} finally {
                        state = ResponseState.DONE;
                    }
				} else {
					throw new PEException("Invalid packet from mysql (expected PrepareResponse header, got " + message.getClass().getSimpleName() +")");
				}
				break;
			case AWAIT_PARAM_DEF:
                if (--numParams== 0)
                    state = ResponseState.AWAIT_PARAM_DEF_EOF;
                MyFieldPktResponse paramDef = (MyFieldPktResponse)message;

                if (!promise.isFulfilled()){
					consumer.paramDef(ctx, paramDef);
                }
				break;
			case AWAIT_PARAM_DEF_EOF:
                MyEOFPktResponse paramEof  = (MyEOFPktResponse)message;
                state = (numCols == 0) ? ResponseState.DONE : ResponseState.AWAIT_COL_DEF;
                if (!promise.isFulfilled()) {
					consumer.paramDefEOF(ctx, paramEof);
					if (state == ResponseState.DONE)
						onCompletion();
				}
				break;
			case AWAIT_COL_DEF:
                if (--numCols == 0)
                    state = ResponseState.AWAIT_COL_DEF_EOF;
                MyFieldPktResponse columnDef = (MyFieldPktResponse)message;
                if (!promise.isFulfilled()){
					consumer.colDef(ctx, columnDef);
                }
				break;
			case AWAIT_COL_DEF_EOF:
                MyEOFPktResponse colEof = (MyEOFPktResponse)message;
				state = ResponseState.DONE;
                if (!promise.isFulfilled()) {
					consumer.colDefEOF(ctx, colEof);
					onCompletion();
				}
				break;
				
			case DONE:
                logger.debug("Received a packet after we believe we are DONE, packet was type " + message.getClass().getSimpleName());
                break;
			}
		} catch (Exception e) {
            promise.failure(e);
		}
		return state == ResponseState.DONE;
	}

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
    }

    protected void onCompletion() {
		if (logger.isDebugEnabled())
			logger.debug("Promise fulfilled on " + this);

        promise.success(false);

		state = ResponseState.DONE;
	}

    @Override
	public void failure(Exception e) {
        promise.failure(e);
	}

	@Override
	public String toString() {
        return this.getClass().getSimpleName() + "{" + promise + ", " + state + ", " + sqlCommand + "}";
	}

}
