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

import com.tesora.dve.db.mysql.MyLoadDataInfileContext;
import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.server.connectionmanager.messages.ExecutePreparedStatementRequestExecutor;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.db.ValueConverter;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.variable.ClientCharSetSessionVariableHandler;
import com.tesora.dve.variables.Variables;
import com.tesora.dve.worker.MysqlSyntheticPreparedResultForwarder;

public class LoadDataBlockExecutor {

	private static final Logger logger = Logger.getLogger(LoadDataBlockExecutor.class);

	public static List<List<byte[]>> processDataBlock(ChannelHandlerContext ctx, SSConnection connMgr, byte[] dataBlock) throws Throwable {

		MyLoadDataInfileContext loadDataInfileContext = ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).get();
		if ( loadDataInfileContext == null ) {
			throw new PEException("Cannot process Load Data Infile data block because load data infile context is missing.");
		}

		if (loadDataInfileContext.getCharset() == null) {
			loadDataInfileContext.setCharset(Singletons.require(HostService.class).getCharSetNative().getCharSetCatalog().findCharSetByName(
					Variables.CHARACTER_SET_CLIENT.getSessionValue(connMgr), true).getJavaCharset());
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Processing datablock: line prefix[" +	loadDataInfileContext.getLineOption().getStarting() + 
					"],  line term[" + loadDataInfileContext.getLineOption().getTerminated() + 
					"],  col enclosed[" + loadDataInfileContext.getColOption().getEnclosed() + 
					"],  col escaped[" + loadDataInfileContext.getColOption().getEscaped() + 
					"],  col term[" + loadDataInfileContext.getColOption().getTerminated() + "]" );
//				logger.debug("Processing datablock: data[" + query + "]" );
		}

		byte[] newDataBlock = appendPartialBlockData(loadDataInfileContext, dataBlock);
		LoadDataBlockParser parser = new LoadDataBlockParser(loadDataInfileContext);
		parser.parse(newDataBlock);
		
		// determine partial block
		if (dataBlock.length > 0) {
			int lastlineindex = parser.getLastLineEndIndex();
			loadDataInfileContext.appendPartialInfileDataBlock(ArrayUtils.subarray(newDataBlock, lastlineindex, newDataBlock.length));
			parser.getRows().remove(parser.getRows().size()-1);
		}
		
		return parser.getRows();
	}

	public static void executeInsert(ChannelHandlerContext ctx, SSConnection connMgr, List<List<byte[]>> rows) throws Throwable {
		
		MyLoadDataInfileContext loadDataInfileContext = ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).get();
		if ( loadDataInfileContext == null ) {
			throw new PEException("Cannot process Load Data Infile data block because load data infile context is missing.");
		}

		if (loadDataInfileContext.getCharset() == null) {
			loadDataInfileContext.setCharset(Singletons.require(HostService.class).getCharSetNative().getCharSetCatalog().findCharSetByName(
					Variables.CHARACTER_SET_CLIENT.getSessionValue(connMgr), true).getJavaCharset());
		}

		List<String> params = new ArrayList<String>();
		int targetNumTuples = rows.size();
		int currentTupleCount = 1;
		
		for(List<byte[]> cols : rows) {
		
			if (cols.size() == 0) 
				continue;

			buildInsertStatement(loadDataInfileContext, cols, params);
			
			if (optimalTuples(loadDataInfileContext, targetNumTuples, currentTupleCount)) {
				MyPreparedStatement<String> pStmt = loadDataInfileContext.getPreparedStatement(currentTupleCount);
				if (pStmt == null) {
					throw new PEException("A prepared statement with no id found in the connection's context");
				}

				MysqlSyntheticPreparedResultForwarder resultConsumer = new MysqlSyntheticPreparedResultForwarder(ctx);

				ExecutePreparedStatementRequestExecutor.execute(connMgr, pStmt.getStmtId(), params, resultConsumer);
				
				loadDataInfileContext.incrementRowsAffected(resultConsumer.getNumRowsAffected());
				loadDataInfileContext.incrementInfileWarnings(resultConsumer.getWarnings());

				targetNumTuples -= currentTupleCount;
				params.clear();
				currentTupleCount = 0;
			}
			currentTupleCount++;
		}
	}

	static boolean optimalTuples(MyLoadDataInfileContext loadDataInfileContext, int maxTuples, int currentTupleCount) {
		return (currentTupleCount == loadDataInfileContext.getClosestPreparedStatementTuples(maxTuples));
	}

	static void buildInsertStatement(MyLoadDataInfileContext loadDataInfileContext, List<byte[]> cols, List<String> params)	throws PEException {
		if (logger.isDebugEnabled()) {
			logger.debug("Processing datablock: col length = " + cols.size()
					+ ", projection length = "
					+ loadDataInfileContext.getProjection().size());
		}

		if (cols.size() < loadDataInfileContext.getProjection().size()) {
			// pad out the remaining columns with empty values
			for (int i = 0; i < loadDataInfileContext.getProjection().size()
					- cols.size(); i++) {
				cols.add(Unpooled.buffer(0).array());
			}
		}

		if (cols.size() > loadDataInfileContext.getProjection().size()) {
			// mysql lops off the excess columns
			for (int i = cols.size(); i > loadDataInfileContext.getProjection()
					.size(); i--) {
				cols.remove(i - 1);
			}
		}

		if (cols.size() != loadDataInfileContext.getProjection().size()) {
			// should never happen...
			List<String> colData = new ArrayList<String>();
			for (byte[] data : cols) {
				colData.add(new String(data));
			}
			throw new PEException("Expected "
					+ loadDataInfileContext.getProjection().size()
					+ " columns but parsed " + cols.size()
					+ " columns for row data '"
					+ StringUtils.join(colData, ",") + "'");
		}

		ValueConverter converter = ValueConverter.INSTANCE;
		for (int i = 0; i < loadDataInfileContext.getProjection().size(); i++) {
			Object value = null;
			if (cols.get(i).length > 0) {
				value = converter.convert(PECharsetUtils.getString(cols.get(i),
						loadDataInfileContext.getCharset()),
						loadDataInfileContext.getProjection().get(i).getType());
				String v = DBTypeBasedUtils.getMysqlTypeFunc(
                        MyFieldType.mapFromNativeType(loadDataInfileContext
                                .getProjection().get(i).getType().getMysqlType().getMysqlType()))
						.getParamReplacement(value,false);
				if (loadDataInfileContext.getProjection().get(i).getType()
						.isStringType()) {
					v = "'"
							+ StringUtils.replace(PEStringUtils.dequote(v),
									"'", "\\'") + "'";
				}
				params.add(v);
			} else {
				if (loadDataInfileContext.getProjection().get(i).getType()
						.isStringType()) {
					params.add("''");
				}
			}
		}
	}

	static byte[] appendPartialBlockData(MyLoadDataInfileContext loadDataInfileContext, byte[] dataBlock) {
		byte[] prevDataBlock = loadDataInfileContext.readPartialInfileDataBlock();
		if (prevDataBlock == null) {
			return dataBlock;
		}
		return ArrayUtils.addAll(prevDataBlock, dataBlock);
	}

	static public class LoadDataBlockParser {
		
		public enum TokenType {
			LINE_START,
			LINE_TERMINATE,
			COLUMN_ENCLOSED,
			COLUMN_ESCAPE,
			COLUMN_TERMINATE,
			REGULAR_DATA
		}
		
		int index = 0;
		int lastLineEndIndex = 0;
		int startLineDelimLength = 0;
		int endLineDelimLength = 0;
		int endColDelimLength = 0;
		char charEnclosed;
		char charEscaped;
		boolean inColEnclosedState = false;

		ByteBuf value = Unpooled.buffer(1000000);
		List<byte[]> values = new ArrayList<byte[]>();
		List<List<byte[]>> rows = new ArrayList<List<byte[]>>();

		TokenType lastToken = null;
		MyLoadDataInfileContext loadDataInfileContext = null;
		
		public LoadDataBlockParser(MyLoadDataInfileContext loadDataInfileContext) {
			this.loadDataInfileContext = loadDataInfileContext; 
			charEnclosed = StringUtils.isBlank(loadDataInfileContext.getColOption().getEnclosed()) ? 0 : loadDataInfileContext.getColOption().getEnclosed().charAt(0);
			charEscaped = StringUtils.isBlank(loadDataInfileContext.getColOption().getEscaped()) ? 0 :loadDataInfileContext.getColOption().getEscaped().charAt(0);
			startLineDelimLength = loadDataInfileContext.getLineOption().getStarting().length(); 
			endLineDelimLength = loadDataInfileContext.getLineOption().getTerminated().length();
			endColDelimLength = loadDataInfileContext.getColOption().getTerminated().length();
		}
		
		public void parse(byte[] dataBlock) throws PEException {

			resetIndex();
			
			while( index < dataBlock.length) {
				TokenType tt = getNextTokenType(dataBlock, index);
				switch(tt) {
					case LINE_START:
						processLineStart();
						break;

					case LINE_TERMINATE:
						processLineTerminate();
						break;
					
					case COLUMN_ENCLOSED:
						processColumnEnclosed();
						break;
					
					case COLUMN_ESCAPE:
						processColumnEscape(dataBlock);
						break;
					
					case COLUMN_TERMINATE:
						processColumnTerminate();
						break;
					
					case REGULAR_DATA:
						processRegularData(dataBlock);
						break;
				}
				setLastToken(tt);
			}

			// write the last line
			writeEOBLineValue();
			
		}
		
		public TokenType getNextTokenType(byte[] dataBlock, int index) {

			TokenType tt = TokenType.REGULAR_DATA;
			
			if ((lastToken == TokenType.LINE_TERMINATE) && isMatchingSequence(dataBlock, index, loadDataInfileContext.getLineOption().getStarting())) {
				tt = TokenType.LINE_START;
			} else if (!inColEnclosedState && isMatchingSequence(dataBlock, index, loadDataInfileContext.getLineOption().getTerminated())) {
				tt = TokenType.LINE_TERMINATE;
			} else if (!inColEnclosedState && isMatchingSequence(dataBlock, index, loadDataInfileContext.getColOption().getTerminated())) {
				tt = TokenType.COLUMN_TERMINATE;
			} else if (isMatchingChar(dataBlock[index], charEscaped)) {
				tt = TokenType.COLUMN_ESCAPE;
			} else if (isMatchingChar(dataBlock[index], charEnclosed)) {
				tt = TokenType.COLUMN_ENCLOSED;
			}
			
			return tt;
		}

		void processLineStart() {
			if (!StringUtils.isBlank(loadDataInfileContext.getLineOption().getStarting())) {
				// increment the number of characters to skip
				index += startLineDelimLength; 
			}
		}
		
		void processLineTerminate() throws PEException {
			writeLineValue();
			
			// increment the number of characters to skip
			index += endLineDelimLength;
			
			lastLineEndIndex = getIndex();
		}

		void processColumnEnclosed() {
			if (isInEnclosedState()) {
				setEndEnclosedState();
			} else {
				setStartEnclosedState();
			}
			index++;
		}

		void processColumnEscape(byte[] dataBlock) {
			if ((charEscaped != '\\') ||
					((charEscaped == '\\') && isEscapeChar(dataBlock))) {
				value.writeByte(dataBlock[getIndex()]);
			}
			index++;
		}

		void processColumnTerminate() {
			writeColumnValue();
			index += endColDelimLength;
		}

		void writeColumnValue() {
			byte[] b = new byte[value.readableBytes()];
			value.getBytes(0, b);
			values.add(b);
			value.clear();
		}
		
		void writeEOBLineValue() {
			writeLineValue();
		}
		
		void writeLineValue() {
			if ((lastToken == TokenType.COLUMN_TERMINATE) || (lastToken == TokenType.COLUMN_ENCLOSED) || 
					(lastToken == TokenType.REGULAR_DATA && (value.readableBytes() > 0))) {
			writeColumnValue();
			}
			rows.add(values);
			values = new ArrayList<byte[]>();
			value.clear();
		}
		
		void processRegularData(byte[] dataBlock) {
			value.writeByte(dataBlock[getIndex()]);
			index++;
		}

		boolean isMatchingChar(byte c1, char c2) {
			return c1 == c2;
		}

		boolean isMatchingSequence(byte[] query, int index,	String match) {
			boolean ret = true;
			for(int idx = 0; idx < match.length(); idx++ ) {
				if (query[index + idx] != match.charAt(idx)) {
					ret = false;
					break;
				}
			}
			return ret;
		}
		
		boolean isEscapeChar(byte[] dataBlock) {
			boolean ret = false;
			
			if ((getIndex() + 1) >= dataBlock.length) {
				// out of bounds so just return true so that the escape character is written out
				return true;
			}

//			\0 	An ASCII NUL (0x00) character.
//			\' 	A single quote (“'”) character.
//			\" 	A double quote (“"”) character.
//			\b 	A backspace character.
//			\n 	A newline (linefeed) character.
//			\r 	A carriage return character.
//			\t 	A tab character.
//			\Z 	ASCII 26 (Control+Z). See note following the table.
//			\\ 	A backslash (“\”) character.
//			\% 	A “%” character.
//			\_ 	A “_” character.
			switch(dataBlock[getIndex() + 1]) {
			case '0': 
			case '\'': 
			case '"': 
			case 'b': 
			case 'n': 
			case 'r': 
			case 't': 
			case 'z': 
			case '\\': 
			case '%': 
			case '_': 
				ret = true;
				break;
			}
			return ret;
		}

		void resetIndex() {
			index = 0;
		}
		
		int getIndex() {
			return index;
		}
		
		void setStartEnclosedState() {
			inColEnclosedState = true;
		}

		void setEndEnclosedState() {
			inColEnclosedState = false;
		}
		
		boolean isInEnclosedState() {
			return inColEnclosedState;
		}
		
		int getLastLineEndIndex() {
			return lastLineEndIndex;
		}

		void setLastToken(TokenType lastToken) {
			this.lastToken = lastToken;
		}

		public List<List<byte[]>> getRows() {
			return rows;
		}
	}
}
