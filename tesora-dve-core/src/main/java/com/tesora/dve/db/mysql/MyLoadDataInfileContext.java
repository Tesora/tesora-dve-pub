// OS_STATUS: public
package com.tesora.dve.db.mysql;


import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.LoadDataInfileColOption;
import com.tesora.dve.sql.schema.LoadDataInfileLineOption;
import com.tesora.dve.sql.schema.LoadDataInfileModifier;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;

public class MyLoadDataInfileContext {
	public static AttributeKey<MyLoadDataInfileContext> LOAD_DATA_INFILE_CONTEXT_KEY = new AttributeKey<MyLoadDataInfileContext>("LoadDataInfileContext");

	LoadDataInfileModifier modifier;
	boolean local;
	String fileName;
	boolean replace;
	boolean ignore;
	PEDatabase db;
	Name tableName;
	Name characterSet;
	LoadDataInfileColOption colOption = new LoadDataInfileColOption();
	LoadDataInfileLineOption lineOption = new LoadDataInfileLineOption();
	Integer ignoredLines;
	List<Name> colOrVarList;
	List<ExpressionNode> updateExprs;

	byte[] partialInfileDataBlock = null;
	List<PEColumn> projection = null;

	TreeMap<Integer, MyPreparedStatement<String>> pStmts = new TreeMap<Integer, MyPreparedStatement<String>>();
	
	Charset cs;
	long infileRowsAffected = 0;
	long infileWarnings = 0;

	public static MyLoadDataInfileContext getOrCreateLoadDataInfileContextFromChannel(
			ChannelHandlerContext ctx) {
		MyLoadDataInfileContext loadDataInfileCtx = getLoadDataInfileContextFromChannel(ctx);
		if (loadDataInfileCtx == null) {
			loadDataInfileCtx = new MyLoadDataInfileContext();
			ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).set(loadDataInfileCtx);
		}
		return loadDataInfileCtx;
	}

	public static MyLoadDataInfileContext getLoadDataInfileContextFromChannel(
			ChannelHandlerContext ctx) {
		return ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).get();
	}

	public static void setLoadDataInfileContextOnChannel(
			ChannelHandlerContext ctx, MyLoadDataInfileContext loadDataInfileCtx) {
		ctx.attr(MyLoadDataInfileContext.LOAD_DATA_INFILE_CONTEXT_KEY).set(loadDataInfileCtx);
	}

	public MyPrepStmtConnectionContext getOrCreatePrepStmtContextFromChannel(SSConnection ssCon,
			ChannelHandlerContext ctx) {
		MyPrepStmtConnectionContext mpscc = ctx.attr(MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).get();
	
		if ( mpscc == null ) {
			mpscc = new MyPrepStmtConnectionContext(ssCon);
			ctx.attr(MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).set(mpscc);
		}
		
		return mpscc;
	}
	
	public byte[] getPartialInfileDataBlock() {
		return partialInfileDataBlock;
	}

	public void appendPartialInfileDataBlock(byte[] dataBlock) {
		partialInfileDataBlock = dataBlock;
	}

	public byte[] readPartialInfileDataBlock() {
		return partialInfileDataBlock;
	}

	public List<PEColumn> getProjection() {
		return projection;
	}

	public void setProjection(List<PEColumn> cols) {
		projection = cols;
	}

	public long getInfileRowsAffected() {
		return infileRowsAffected;
	}

	public void setInfileRowsAffected(long infileRowsAffected) {
		this.infileRowsAffected = infileRowsAffected;
	}

	public void resetRowsAffected() {
		infileRowsAffected = 0;
	}

	public void incrementRowsAffected(long rowsAffected) {
		infileRowsAffected += rowsAffected;
	}

	public long getInfileWarnings() {
		return infileWarnings;
	}

	public void setInfileWarnings(long infileWarnings) {
		this.infileWarnings = infileWarnings;
	}

	public void resetInfileWarnings() {
		infileWarnings = 0;
	}

	public void incrementInfileWarnings(long warnings) {
		infileWarnings += warnings;
	}

	public void resetCounters() {
		resetRowsAffected();
		resetInfileWarnings();
	}

	public LoadDataInfileModifier getModifier() {
		return modifier;
	}

	public void setModifier(LoadDataInfileModifier modifier) {
		this.modifier = modifier;
	}

	public boolean isLocal() {
		return local;
	}

	public void setLocal(boolean local) {
		this.local = local;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public boolean isReplace() {
		return replace;
	}

	public void setReplace(boolean replace) {
		this.replace = replace;
	}

	public boolean isIgnore() {
		return ignore;
	}

	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}

	public PEDatabase getDb() {
		return db;
	}

	public void setDb(PEDatabase db) {
		this.db = db;
	}

	public Name getTableName() {
		return tableName;
	}

	public void setTableName(Name tableName) {
		this.tableName = tableName;
	}

	public Name getCharacterSet() {
		return characterSet;
	}

	public void setCharacterSet(Name characterSet) {
		this.characterSet = characterSet;
	}

	public LoadDataInfileColOption getColOption() {
		return colOption;
	}

	public void setColOption(LoadDataInfileColOption colOption) {
		this.colOption = colOption;
	}

	public LoadDataInfileLineOption getLineOption() {
		return lineOption;
	}

	public void setLineOption(LoadDataInfileLineOption lineOption) {
		this.lineOption = lineOption;
	}

	public Integer getIgnoredLines() {
		return ignoredLines;
	}

	public void setIgnoredLines(Integer ignoredLines) {
		this.ignoredLines = ignoredLines;
	}

	public List<Name> getColOrVarList() {
		return colOrVarList;
	}

	public void setColOrVarList(List<Name> colOrVarList) {
		this.colOrVarList = colOrVarList;
	}

	public List<ExpressionNode> getUpdateExprs() {
		return updateExprs;
	}

	public void setUpdateExprs(List<ExpressionNode> updateExprs) {
		this.updateExprs = updateExprs;
	}

	/**
	 * Convenience method to break up a Statement into the various parts.
	 * @param stmt
	 */
	public void setLoadDataInfileStatementParts(SchemaContext sc, LoadDataInfileStatement stmt) {
		if (stmt == null) {
			return;
		}

		setModifier(stmt.getModifier());
		setLocal(stmt.isLocal());
		setFileName(stmt.getFileName());
		setReplace(stmt.isReplace());
		setIgnore(stmt.isIgnore());
		setDb(stmt.getDatabase(sc));
		setTableName(stmt.getTableName());
		setCharacterSet(stmt.getCharacterSet());
		setColOption(stmt.getColOption());
		setLineOption(stmt.getLineOption());
		setIgnoredLines(stmt.getIgnoredLines());
		setColOrVarList(stmt.getColOrVarList());
		setUpdateExprs(stmt.getUpdateExprs());
	}

	public Charset getCharset() {
		return cs;
	}

	public void setCharset(Charset cs) {
		this.cs = cs;
	}

	public void resetState() {
		partialInfileDataBlock = null;
		projection = null;
		cs = null;
		infileRowsAffected = 0;
		infileWarnings = 0;
	}
	
	public void addPreparedStatement(int numTuples, MyPreparedStatement<String> pStmt) {
		pStmts.put(numTuples, pStmt);
	}

	public MyPreparedStatement<String> getPreparedStatement(int numTuples) {
		return pStmts.get(numTuples);
	}
	
	public Collection<MyPreparedStatement<String>> getPreparedStatements() {
		return pStmts.values();
	}
	
	public void clearPreparedStatements() {
		pStmts.clear();
	}
	
	public int getClosestPreparedStatementTuples(int numTuples) {
		Integer bestTuples = pStmts.lowerKey(numTuples);
		if (bestTuples == null) {
			return 1;
		}
		
		return bestTuples;
	}
}