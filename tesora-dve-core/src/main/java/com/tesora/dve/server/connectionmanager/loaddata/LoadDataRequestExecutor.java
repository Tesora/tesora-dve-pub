package com.tesora.dve.server.connectionmanager.loaddata;

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

import com.tesora.dve.server.connectionmanager.messages.PrepareRequestExecutor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.db.mysql.MysqlLoadDataInfileRequestCollector;
import com.tesora.dve.db.mysql.MysqlPrepareStatementDiscarder;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.server.connectionmanager.loaddata.MSPLoadDataDecoder ;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.variables.KnownVariables;

public class LoadDataRequestExecutor {

	private static final Logger logger = Logger.getLogger(LoadDataRequestExecutor.class);
	
	private static final int BLOCKSIZE = 100000000; //100 MB
	
	public static void execute(ChannelHandlerContext ctx, SSConnection connMgr, MysqlLoadDataInfileRequestCollector resultConsumer, Charset cs, byte[] message) throws Throwable {
		
		// parse the query
		Charset charSet = cs;
		if (charSet == null) {
			charSet =  KnownVariables.CHARACTER_SET_CLIENT.getSessionValue(connMgr).getJavaCharset();
		}
		String query = PECharsetUtils.getString(message, charSet, true);
		if (logger.isDebugEnabled()) {
			logger.debug("Load Data Request: Query[" + query + "]");
		}

		SchemaContext sc = connMgr.getSchemaContext();
		List<Statement> stmts = InvokeParser.parse(query, sc);
		LoadDataInfileStatement stmt = (LoadDataInfileStatement)stmts.get(0);
		
		resultConsumer.setFileName(stmt.getFileName());
		if (logger.isDebugEnabled()) {
			logger.debug("Load Data Request: File[" + resultConsumer.getFileName() + "]");
		}
		
		List<PEColumn> projection = buildProjectionColumns(sc,stmt);
		StringBuilder columns = new StringBuilder();
		StringBuilder params = new StringBuilder();
		boolean first = true;
		for(PEColumn col : projection) {
			if (!first) {
				columns.append(",");
				params.append(",");
			}
			columns.append(col.getName().get());
			params.append("?");
			first = false;
		}

		StringBuilder buf = new StringBuilder();
//			if (stmt.isReplace()) {
//				buf.append("REPLACE");
//			} else {
			buf.append("INSERT");
			if (stmt.isLocal() || stmt.isIgnore()) {
				buf.append(" IGNORE");
			}
//			}
		buf.append(" INTO ");
		buf.append(stmt.getDatabase(sc).getName().get());
		buf.append(".");
		buf.append(stmt.getTableName().get());
		buf.append(" (");
		buf.append(columns);
		buf.append(")");
		
		buf.append(" VALUES ");
		
		StringBuilder tuple = new StringBuilder();
		tuple.append("(");
		tuple.append(params);
		tuple.append(")");
		
		resultConsumer.getLoadDataInfileContext().resetState();
		resultConsumer.getLoadDataInfileContext().setCharset(cs);
		resultConsumer.getLoadDataInfileContext().setProjection(projection);
		resultConsumer.getLoadDataInfileContext().setLoadDataInfileStatementParts(sc,stmt);

		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 1);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 2);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 3);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 4);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 5);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 6);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 7);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 8);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 9);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 10);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 100);
		createPreparedStatement(connMgr, ctx, charSet, resultConsumer,
				buf.toString(), tuple.toString(), projection.size(), 500);
		
		if (connMgr.getReplicationOptions().connectionFromReplicationSlave()) {
			return;
		}

        //It's counter intuitive, but local means the file is local to the calling client (IE, mysqlimport), not local the database (mysqld, dve).
		if (stmt.isLocal()) {
			// check the client capabilities flag to see if it supports loading client local files.
			if (!connMgr.getClientCapabilities().allowLocalInfile()) {
				throw new PEException("The client does not support the LOCAL option.");
			}
		} else {
            //check if the sepcified filename exists on this server.
			try {
				Path p1 = Paths.get(stmt.getFileName());
				boolean exists = Files.exists(p1);
				boolean notExists = Files.notExists(p1);
				if (exists == notExists) {
					// can't happen
					throw new PEException("Error with '" + stmt.getFileName() + "'.  Cannot verify the existence of the file.  Check the configuration if any access control system (for example AppArmor) is being used.");
				}
				if (!exists && notExists) {
					// file does not exist
					throw new PEException("Error with '" + stmt.getFileName() + "'.  The file does not exist.");
				}
				
				boolean isReadable = Files.isReadable(p1);
				if (!isReadable) {
					throw new PEException("Error with '" + stmt.getFileName() + "'.  No permission to read the file.");
				}
			} catch (InvalidPathException ipe) {
				throw new PEException("Error with '" + stmt.getFileName() + "' due to '" + ipe.getReason() + "'", ipe);
			} catch (SecurityException se) {
				throw new PEException("Error with '" + stmt.getFileName() + "' due to '" + se.getMessage() + "'", se);
			}

			// we can open the file and read the chunks directly into the server now
			ByteBuffer dataBlock = ByteBuffer.allocate(BLOCKSIZE);
			try (FileChannel fileChannel = FileChannel.open(Paths.get(stmt.getFileName()), StandardOpenOption.READ)) {
				dataBlock.clear();
				while(fileChannel.read(dataBlock) != -1) {
					int len = dataBlock.position();
					byte[] dataBlockBytes = new byte[len];
					dataBlock.rewind();
					dataBlock.get(dataBlockBytes,0,len);
					processDataBlock(resultConsumer.getCtx(), connMgr, dataBlockBytes);
					dataBlock.clear();
				}
			} catch (IOException io) {
				throw new PEException(io);
			}
			
			resultConsumer.getCtx().write(MSPLoadDataDecoder.createLoadDataEOFMsg(resultConsumer.getLoadDataInfileContext(), Byte.valueOf("0")));
		}
	}
	
	static void createPreparedStatement(SSConnection connMgr,
			ChannelHandlerContext ctx, Charset charSet,
			MysqlLoadDataInfileRequestCollector resultConsumer,
			String insertPrefix, String parameterizedTuple, 
			int numColumns,	int numTuples) throws PEException {
		
		StringBuilder insert = new StringBuilder(insertPrefix);
		boolean first = true;
		for(int i=0; i<numTuples; i++) {
			if (!first) {
				insert.append(",");
			}
			insert.append(parameterizedTuple);
			first = false;
		}
		
		MyPreparedStatement<String> pStmt = new MyPreparedStatement<String>(null);
		pStmt.setQuery(insert.toString().getBytes(CharsetUtil.ISO_8859_1));
		pStmt.setNumColumns(numColumns);
		pStmt.setNumParams(numColumns);
		
		resultConsumer.getLoadDataInfileContext().addPreparedStatement(numTuples, pStmt);
		resultConsumer.getLoadDataInfileContext().getOrCreatePrepStmtContextFromChannel(connMgr, ctx).addPreparedStatement(pStmt);

		MysqlPrepareStatementDiscarder prepStmtConsumer = new MysqlPrepareStatementDiscarder();
		try {
			PrepareRequestExecutor.execute(connMgr, prepStmtConsumer, pStmt.getStmtId(), charSet, insert.toString().getBytes(CharsetUtil.ISO_8859_1));
		} catch (Throwable t) {
			throw new PEException("Failed to prepare statement:" + insert.toString(), t);
		}
	}

	static List<PEColumn> buildProjectionColumns(SchemaContext sc, LoadDataInfileStatement stmt) throws PEException {
		List<PEColumn> projection = new ArrayList<PEColumn>();
		
		PETable tbl = sc.findTable(PEAbstractTable.getTableKey(stmt.getDatabase(sc), stmt.getTableName())).asTable();
		if (stmt.getColOrVarList() != null) {
			for (Name name : stmt.getColOrVarList()) {
				PEColumn peCol = tbl.lookup(sc, name);
				if (peCol == null) {
					throw new PEException("'" + name.get() + "' is not a column in table '" +  tbl.getName().get() + "'.  If '" + name.get() + "' is a variable then it is not currently supported."); 
				}
				projection.add(peCol);
			}
		} else {
			// for now just get the column list from the table
			for(PEColumn col : tbl.getColumns(sc)) {
				projection.add(col);
			}
		}
		
		return projection;
	}

	static void processDataBlock(final ChannelHandlerContext ctx, final SSConnection ssCon, final byte[] dataBlockBytes) throws Throwable {
		Throwable t = ssCon.executeInContext(new Callable<Throwable>() {
			public Throwable call() {
				try {
					LoadDataBlockExecutor.executeInsert(ctx, ssCon, 
							LoadDataBlockExecutor.processDataBlock(ctx, ssCon, dataBlockBytes));
				} catch (Throwable e) {
					return e;
				}
				return null;
			}
		});
		if (t != null && t.getCause() != null) {
			throw new PEException(t);
		}
	}
}
