// OS_STATUS: public
package com.tesora.dve.variable;

import java.sql.Types;
import java.util.Arrays;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;


public abstract class ScopedVariableHandler extends VariableHandler {

	private static final String RESULT_VALUE_COLUMN_NAME = "Value";

	public abstract void setValue(String scopeName, String name, String value) throws PEException;
	
	public abstract String getValue(String scopeName, String name) throws PEException;

	public ResultCollector getValueAsResult(String scopeName, String variableName) throws PEException {
		ColumnSet cs = new ColumnSet();
		cs.addColumn(RESULT_VALUE_COLUMN_NAME, 255, "varchar", Types.VARCHAR);
		ResultColumn rcol = new ResultColumn(getValue(scopeName, variableName));
		ResultRow row = new ResultRow(Arrays.asList(rcol));
		ResultChunk rc = new ResultChunk(Arrays.asList(row));
		return ResultCollectorFactory.getInstance(cs, rc);
	}
}
