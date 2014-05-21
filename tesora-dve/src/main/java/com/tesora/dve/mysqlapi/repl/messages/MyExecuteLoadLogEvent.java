// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl.messages;

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
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.MyReplicationSlaveService;
import com.tesora.dve.variable.SchemaVariableConstants;

public class MyExecuteLoadLogEvent extends MyLogEventPacket {
	private static final Logger logger = Logger
			.getLogger(MyExecuteLoadLogEvent.class);

	static final int MAX_BUFFER_LEN = 100000;
	int threadId;
	int time;
	byte dbLen;
	short errorCode;
	short statusBlockLen;
	int fileId;
	int startPos;
	int endPos;
	byte duplicateFlag;
	String dbName;
	ByteBuf query;
	String origQuery;
	String skipErrorMessage;

	MyStatusVariables statusVars = new MyStatusVariables();
	
	public MyExecuteLoadLogEvent(MyReplEventCommonHeader ch) {
		super(ch);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		threadId = cb.readInt();
		time = cb.readInt();
		dbLen = cb.readByte();
		errorCode = cb.readShort();
		statusBlockLen = cb.readShort();
		fileId = cb.readInt();
		startPos = cb.readInt();
		endPos = cb.readInt();
		duplicateFlag = cb.readByte();
		// really we should check if replication version >=4 or else this is wrong
		statusVars.parseStatusVariables(cb, statusBlockLen);
		
		dbName = MysqlAPIUtils.readBytesAsString(cb, dbLen, CharsetUtil.UTF_8);
		cb.skipBytes(1); //for trailing 0
		query = Unpooled.buffer(cb.readableBytes());
		query.writeBytes(cb);
		origQuery = query.toString(CharsetUtil.UTF_8);
	}

	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(threadId);
		cb.writeInt(time);
		cb.writeByte(dbLen);
		cb.writeShort(errorCode);
		cb.writeShort(statusBlockLen);
		cb.writeInt(fileId);
		cb.writeInt(startPos);
		cb.writeInt(endPos);
		cb.writeByte(duplicateFlag);

		statusVars.writeStatusVariables(cb);
		
		cb.writeBytes(dbName.getBytes(CharsetUtil.UTF_8));
		cb.writeByte(0); //for trailing 0
		cb.writeBytes(query);
	}

	@Override
	public void processEvent(MyReplicationSlaveService plugin) throws PEException {
		boolean switchToDb = true;
		ServerDBConnection conn = null;

		try {
			if (!includeDatabase(plugin, dbName)) {
				// still want to update log position if we filter out message
				updateBinLogPosition(plugin);
				return;
			}

			conn = plugin.getServerDBConnection();

			// If any session variables are to be set do it first
			plugin.getSessionVariableCache().setAllSessionVariableValues(conn);

			if (logger.isDebugEnabled()) {
				logger.debug("** START ExecuteLoadLog Event **");
				if ( switchToDb ) logger.debug("USE " + dbName);
				logger.debug(origQuery);
				logger.debug("** END ExecuteLoadLog Event **");
			}

			if ( switchToDb ) conn.setCatalog(dbName);

			// since we don't want to parse here to determine if a time function is specified 
			// set the TIMESTAMP variable to the master statement execution time
			conn.executeUpdate("set " + SchemaVariableConstants.REPL_SLAVE_TIMESTAMP + "=" + getCommonHeader().getTimestamp());
			
			conn.executeLoadDataRequest(plugin.getClientConnectionContext().getCtx(), origQuery.getBytes(CharsetUtil.UTF_8));

			// start throwing down the bytes from the load data infile
			File infile = plugin.getInfileHandler().getInfile(fileId);

			FileInputStream in = null;
	        byte[] readData = new byte[MAX_BUFFER_LEN];
	        try {
	            in = new FileInputStream(infile);
	            int len = in.read(readData);
	            do {
	            	conn.executeLoadDataBlock(plugin.getClientConnectionContext().getCtx(), 
	            			(len == MAX_BUFFER_LEN) ? readData : ArrayUtils.subarray(readData, 0, len));
	            	len = in.read(readData);
	            } while (len > -1);
	            conn.executeLoadDataBlock(plugin.getClientConnectionContext().getCtx(), ArrayUtils.EMPTY_BYTE_ARRAY);
	        } finally {
	            IOUtils.closeQuietly(in);
				plugin.getInfileHandler().cleanUp();
	        }

			updateBinLogPosition(plugin);
			
		} catch (Exception e) {
			if (plugin.validateErrorAndStop(getErrorCode(), e)) {
				logger.error("Error occurred during replication processing: ",e);
				try {
					conn.execute("ROLLBACK");
				} catch (SQLException e1) {
					throw new PEException("Error attempting to rollback after exception",e); // NOPMD by doug on 18/12/12 8:07 AM
				}
			} else {
				skipErrorMessage = "Replication Slave failed processing: '" + origQuery
						+ "' but slave_skip_errors is active. Replication processing will continue";
					
				setSkipErrors(true);
			}
			throw new PEException("Error executing: " + origQuery,e);
		} finally { // NOPMD by doug on 18/12/12 8:08 AM
			// Clear all the session variables since they are only good for one
			// event
			plugin.getSessionVariableCache().clearAllSessionVariables();
		}
	}

	public short getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(short errorCode) {
		this.errorCode = errorCode;
	}

}
