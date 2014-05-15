// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.resultset.ColumnMetadata;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;

import java.util.ArrayList;
import java.util.List;

public class MysqlPrepareStatementCollector extends MysqlPrepareParallelConsumer {

	private static final Logger logger = Logger.getLogger(MysqlPrepareStatementCollector.class);

    List<MyFieldPktResponse> savedParamDefs = new ArrayList<>();
	ColumnSet paramColumns;
	ColumnSet resultColumns;

	@Override
	void consumeHeader(MyPrepareOKResponse prepareOK) {
		paramColumns = null;//defer building this until it is asked for.
		pstmt.setNumParams(getNumParams());
		resultColumns = new ColumnSet();
		pstmt.setNumColumns(getNumCols());
	}

	@Override
	void consumeParamDef(MyFieldPktResponse columnDef) throws PEException {
        //defer building metadata object for parameters until someone asks for it via getParamColumns().
        savedParamDefs.add(columnDef);
		pstmt.addParameter(new MyParameter(columnDef.getColumn_type()));
	}

	@Override
	void consumeParamDefEOF(MyEOFPktResponse myEof) {
	}

	@Override
	void consumeColDef(MyFieldPktResponse columnDef) {
		resultColumns.addColumn(FieldMetadataAdapter.buildMetadata(columnDef));
		if (logger.isDebugEnabled())
			logger.debug(this.getClass().getSimpleName() + " reads column " + columnDef);
	}

	@Override
	void consumeColDefEOF(MyEOFPktResponse colEof) {
	}

	@Override
	void consumeError(MyErrorResponse error) throws PESQLStateException {
		throw (PESQLStateException)error.asException();
	}

	public ColumnSet getResultColumns() {
		return resultColumns;
	}
	
	public ColumnSet getParamColumns() {
        if (paramColumns == null){
            paramColumns = new ColumnSet();
            for (MyFieldPktResponse paramDef : savedParamDefs){
                ColumnMetadata meta = FieldMetadataAdapter.buildMetadata(paramDef);
                paramColumns.addColumn(meta);
            }
        }
		return paramColumns;
	}

}
