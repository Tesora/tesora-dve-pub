// OS_STATUS: public
package com.tesora.dve.sql.parser;

import org.antlr.runtime.ANTLRStringStream;

import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;

public interface InputState {

	public ANTLRStringStream buildNewStream();
	
	public String describe();
	
	// for holding the offset within the stmt
	public void setCurrentPosition(int pos);
	public int getCurrentPosition();
	
	// for holding the skeleton
	public void setInsertSkeleton(InsertIntoValuesStatement is);
	public InsertIntoValuesStatement getInsertSkeleton();
	
	public String getCommand();
	
	public long getThreshold();

	public boolean isInsert();
}
