// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.LoadDataInfileColOption;
import com.tesora.dve.sql.schema.LoadDataInfileLineOption;
import com.tesora.dve.sql.schema.LoadDataInfileModifier;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class LoadDataInfileStatement extends SessionStatement {
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
	
	public LoadDataInfileStatement(LoadDataInfileModifier modifier, boolean local, String fileName,
			boolean replace, boolean ignore, PEDatabase db, Name tableName, Name characterSet,
			LoadDataInfileColOption colOption, LoadDataInfileLineOption lineOption,
			Integer ignoredLines, List<Name> colOrVarList, List<ExpressionNode> updateExprs) {
		super("LOAD DATA INFILE");
		this.modifier = modifier;
		this.local = local;
		this.fileName = fileName;
		this.replace = replace;
		this.ignore = ignore;
		this.db = db;
		this.tableName = tableName;
		this.characterSet = characterSet;
		if (colOption != null) this.colOption = colOption;
		if (lineOption != null) this.lineOption = lineOption;
		this.ignoredLines = ignoredLines;
		this.colOrVarList = colOrVarList;
		this.updateExprs = updateExprs;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new EmptyExecutionStep(0,"EMPTY LOAD DATA INFILE")); 
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

	@Override
	public PEDatabase getDatabase(SchemaContext sc) {
		return db;
	}
	
	public void setDatabase(PEDatabase db) {
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
		if (colOption != null) {
			this.colOption = colOption;
		}
	}

	public LoadDataInfileLineOption getLineOption() {
		return lineOption;
	}

	public void setLineOption(LoadDataInfileLineOption lineOption) {
		if (lineOption != null) {
			this.lineOption = lineOption;
		}
	}

	public List<Name> getColOrVarList() {
		return colOrVarList;
	}

	public void setColOrVarList(List<Name> colOrVarList) {
		this.colOrVarList = colOrVarList;
	}

	public Integer getIgnoredLines() {
		return ignoredLines;
	}

	public void setIgnoredLines(Integer ignoredLines) {
		this.ignoredLines = ignoredLines;
	}

	public List<ExpressionNode> getUpdateExprs() {
		return updateExprs;
	}

	public void setUpdateExprs(List<ExpressionNode> updateExprs) {
		this.updateExprs = updateExprs;
	}

}
