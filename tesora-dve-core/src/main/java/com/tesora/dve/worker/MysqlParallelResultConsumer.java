package com.tesora.dve.worker;

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
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import io.netty.channel.ChannelHandlerContext;

public abstract class MysqlParallelResultConsumer implements MysqlQueryResultConsumer, DBResultConsumer {

	protected enum ResponseState {
		AWAIT_FIELD_COUNT, AWAIT_FIELD, AWAIT_ROW, DONE
	}

	RowCountAdjuster rowAdjuster = null;

	ResponseState state = ResponseState.AWAIT_FIELD_COUNT;
	int senderCount = 0;
	int field = 0;
	int senderComplete = 0;
	MyOKResponse okPacket = null;
	MyEOFPktResponse eofPacket = null;
	long numRowsAffected = 0;
	long numRowsAffectedAccum = 0;
	short statusFlags = 0;
	short warnings = 0;
	long lastInsertId = 0;
	String infoString;
	private boolean hasResults = false;
	boolean successful = false;

	public MysqlParallelResultConsumer() {
		super();
	}

    @Override
    public void active(ChannelHandlerContext ctx) {
        //NOOP.
    }

    public boolean emptyResultSet(MyOKResponse ok) {
		synchronized (this) {
			if (senderCount == 0)
				throw new PECodingException("ResultConsumer fired, but no senderCount set");

			getOKPacket();
            okPacket = ok;
			numRowsAffectedAccum += okPacket.getAffectedRows();
			statusFlags |= okPacket.getServerStatus();
			warnings += okPacket.getWarningCount();
			infoString = okPacket.getMessage();
			lastInsertId = okPacket.getInsertId();
			// System.out.println("emptyResultSet: " + numRowsAffected + ": " +
			// infoString);

			if (++senderComplete == senderCount && state != ResponseState.DONE) {
				if (rowAdjuster != null)
					numRowsAffectedAccum = rowAdjuster.adjust(numRowsAffectedAccum, senderCount);
				numRowsAffected += numRowsAffectedAccum;
				consumeEmptyResultSet(ok);
				successful = true;
				state = ResponseState.DONE;
			}
			return okPacket.getAffectedRows() > 0;
		}
	}

	protected MyOKResponse getOKPacket() {
		if (okPacket == null)
			okPacket = new MyOKResponse();
		return okPacket;
	}

	@Override
	public void rollback() {
		numRowsAffected = 0;
	}

    public abstract void consumeEmptyResultSet(MyOKResponse ok);


    @Override
	public void error(MyErrorResponse err) throws PEException {
		synchronized (this) {
			if (state != ResponseState.DONE) {
				state = ResponseState.DONE;
				consumeError(err);
			}
		}
	}

    public abstract void consumeError(MyErrorResponse errorResp) throws PEException;

    @Override
    public void fieldCount(MyColumnCount colCount) {
		synchronized (this) {

			hasResults = true;
			if (state == ResponseState.AWAIT_FIELD_COUNT) {
				consumeFieldCount(colCount);
				state = ResponseState.AWAIT_FIELD;
			}
		}
	}

    public abstract void consumeFieldCount(MyColumnCount colCount);

	@Override
	public void field(int fieldIndex, MyFieldPktResponse columnDef, ColumnInfo columnProjection)
			throws PEException {
		synchronized (this) {
			if (state == ResponseState.AWAIT_FIELD && this.field == fieldIndex) {
				consumeField(fieldIndex, columnDef, columnProjection);
				++this.field;
			}
		}
	}

    public abstract void consumeField(int field, MyFieldPktResponse columnDef, ColumnInfo columnProjection) throws PEException;

	@Override
	public void fieldEOF(MyMessage rawMessage) {
		synchronized (this) {
			if (state == ResponseState.AWAIT_FIELD) {
				consumeFieldEOF(rawMessage);
				state = ResponseState.AWAIT_ROW;
			}
		}
	}

	public abstract void consumeFieldEOF(MyMessage someMessage);

	@Override
	public void rowEOF(MyEOFPktResponse wholePacket) throws PEException {
		synchronized (this) {
			if (senderCount == 0)
				throw new PECodingException("ResultConsumer fired, but no senderCount set");

			if (++senderComplete == senderCount && state != ResponseState.DONE) {
				consumeRowEOF();
				state = ResponseState.DONE;
			}
		}
	}

	public abstract void consumeRowEOF() throws PEException;

    MyEOFPktResponse getEOFPacket() {
		if (eofPacket == null) {
			eofPacket = new MyEOFPktResponse();
		}
		return eofPacket;
	}

    public abstract void consumeRowText(MyTextResultRow textRow) throws PEException;
    public abstract void consumeRowBinary(MyBinaryResultRow binRow) throws PEException;

    @Override
    public void rowText(MyTextResultRow textRow) throws PEException {
        //TODO: having the same sync block in rowText() and rowBinary() is less than ideal.
        synchronized (this) {
            if (state != ResponseState.DONE) {
                if (state != ResponseState.AWAIT_ROW)
                    throw new PECodingException("Processing row out of sequence");
                consumeRowText(textRow);
            }
        }
    }

    @Override
    public void rowBinary(MyBinaryResultRow binRow) throws PEException {
        //TODO: having the same sync block in rowText() and rowBinary() is less than ideal.
        synchronized (this) {
            if (state != ResponseState.DONE) {
                if (state != ResponseState.AWAIT_ROW)
                    throw new PECodingException("Processing row out of sequence");
                consumeRowBinary(binRow);
            }
        }
    }

    @Override
    public void rowFlush() throws PEException {
        //ignored.
    }

	public long getNumRowsAffected() {
		return numRowsAffected;
	}

	public short getStatusFlags() {
		return statusFlags;
	}

	public short getWarnings() {
		return warnings;
	}

	public String getInfoString() {
		return infoString;
	}

	public int getSenderCount() {
		return senderCount;
	}

	public long getLastInsertId() {
		return lastInsertId;
	}

	@Override
	public void setSenderCount(int senderCount) {
		this.senderCount = senderCount;
		senderComplete = 0;
		numRowsAffectedAccum = 0;
		state = ResponseState.AWAIT_FIELD_COUNT;
	}

	@Override
	public boolean hasResults() {
		return hasResults;
	}

	public void setHasResults() {
		hasResults = true;
	}

	@Override
	public long getUpdateCount() {
		return numRowsAffected;
	}

	@Override
	public void setNumRowsAffected(long numRowsAffected) {
		this.numRowsAffected = numRowsAffected;
	}

	@Override
	public void setRowAdjuster(RowCountAdjuster rowAdjuster) {
		this.rowAdjuster = rowAdjuster;
	}

	@Override
	public boolean isSuccessful() {
		return successful;
	}

}