package com.tesora.dve.db.mysql.libmy;

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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.tesora.dve.errmap.FormattedErrorInfo;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;

public class MyErrorResponse extends MyResponseMessage {

	static final Logger logger = Logger.getLogger(MyErrorResponse.class);

	public static final byte ERRORPKT_FIELD_COUNT = ((byte) 0xff);
	private static final Map<String, PESQLStateException> SQLSTATE_TRANSFORM_TABLE = ImmutableMap.<String, PESQLStateException>builder()
			.put("XA102", new PESQLStateException(1213, "40001", "Deadlock found when trying to get lock; try restarting transaction"))
			.build();

	private int errorNumber = 0;
	private String sqlState = "00000";
	private String errorMsg = "Unknown Error";

	public MyErrorResponse() {
		setOK(false);
	}

	public MyErrorResponse(FormattedErrorInfo fei) {
		this();
		this.errorNumber = fei.getErrNo();
		this.sqlState = fei.getSQLState();
		this.errorMsg = fei.getErrorMessage();
	}
	
	public MyErrorResponse(Exception e) {
		this();
		setException(e);
		setSQLErrorInformation(doTransforms(extractRootCause(e)));
	}

    @Override
    public boolean isErrorResponse() {
        return true;
    }

    private Throwable extractRootCause(Exception e) {
		if (e instanceof PESQLStateException) {
			return e;
		} else if (e instanceof PEException) {
			return getLastException();
		}
		
		return e;
	}

	private static Throwable doTransforms(Throwable t) {
		if (t instanceof PESQLStateException) {
			String key = ((PESQLStateException) t).getSqlState();
			if (SQLSTATE_TRANSFORM_TABLE.containsKey(key)) {
				return SQLSTATE_TRANSFORM_TABLE.get(key);
			}
		} else if (t instanceof SQLException) {
			return doTransforms(new PESQLStateException((SQLException) t));
		}

		return t;
	}

	private void setSQLErrorInformation(Throwable t) {
		if (t instanceof PESQLStateException) {
			PESQLStateException sqle = (PESQLStateException) t;
			setSQLErrorInformation(sqle.getErrorNumber(), sqle.getSqlState(), sqle.getErrorMsg());
		} else {
			setSQLErrorInformation(99, "99999", extractExceptionMessage(t));
		}
	}

	private void setSQLErrorInformation(int errorNumber, String sqlState, String errorMsg) {
		setErrorMsg(errorMsg);
		if (sqlState != null)
			setSqlState(sqlState);
		setErrorNumber(errorNumber);
	}

	public static String extractExceptionMessage(Throwable use) {
		return use.getClass().getSimpleName() + ": " + ((use.getMessage() != null) ? use.getMessage() : use.toString());
	}
	
	public Throwable getLastException() {
		Throwable lastEx = exception;
		for (Throwable root = exception; (root = root.getCause()) != null;) {
			lastEx = root;
		}
		return lastEx;
	}

	@Override
    public void marshallMessage(ByteBuf cb) {
		cb.writeByte(ERRORPKT_FIELD_COUNT);
		cb.writeShort((short) errorNumber);
		cb.writeBytes("#".getBytes());
		cb.writeBytes(sqlState.substring(0, 5).getBytes());
		cb.writeBytes(errorMsg.getBytes());
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		cb.skipBytes(1); // error header
		errorNumber = cb.readUnsignedShort();
		cb.skipBytes(1);
		sqlState = MysqlAPIUtils.readBytesAsString(cb, 5, CharsetUtil.UTF_8);
		errorMsg = MysqlAPIUtils.readBytesAsString(cb, CharsetUtil.UTF_8);
	}

	public PEException asException() {
		return new PESQLStateException(errorNumber, sqlState, errorMsg);
	}

	public int getErrorNumber() {
		return errorNumber;
	}

	public void setErrorNumber(int errorNumber) {
		this.errorNumber = errorNumber;
	}

	public String getSqlState() {
		return sqlState;
	}

	public void setSqlState(String sqlState) {
		this.sqlState = sqlState;
	}

	public String getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(String errorMsg) {
		if ( errorMsg != null )
			this.errorMsg = errorMsg;
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.ERROR_RESPONSE;
	}

	@Override
	public String toString() {
		return "Mysql Error: errorNumber=" + errorNumber + " sqlState=" + sqlState + " errorMsg=" + errorMsg;
	}

	public static void throwException(ByteBuf cb) throws PESQLStateException {
		throw asException(cb);
	}

	public static PESQLStateException asException(ByteBuf in) {
		
		ByteBuf cb = in.order(ByteOrder.LITTLE_ENDIAN);
		cb.skipBytes(1); // error header
		int errorNumber = cb.readUnsignedShort();
		cb.skipBytes(1); // sqlState header
		String sqlState = MysqlAPIUtils.readBytesAsString(cb, 5, CharsetUtil.UTF_8);
		String errorMsg = MysqlAPIUtils.readBytesAsString(cb, CharsetUtil.UTF_8);
		return new PESQLStateException(errorNumber, sqlState, errorMsg);
	}
}
