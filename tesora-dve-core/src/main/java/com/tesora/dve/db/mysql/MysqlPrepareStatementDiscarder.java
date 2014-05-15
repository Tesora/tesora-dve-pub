// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.libmy.MyEOFPktResponse;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyFieldPktResponse;
import com.tesora.dve.db.mysql.libmy.MyPrepareOKResponse;

/**
 * The {@link MysqlPrepareStatementDiscarder} class is used when the results of a prepare statement need to 
 * be consumed, the actual results (metadata) aren't needed.  The pstmt id is still harvested by the super class.
 * 
 * @author mwj
 *
 */
public class MysqlPrepareStatementDiscarder extends MysqlPrepareParallelConsumer {

	@Override
	void consumeHeader(MyPrepareOKResponse prepareOK) {
	}

	@Override
	void consumeParamDef(MyFieldPktResponse paramDef) {
	}

	@Override
	void consumeParamDefEOF(MyEOFPktResponse myEof) {
	}

	@Override
	void consumeColDef(MyFieldPktResponse columnDef) {
	}

	@Override
	void consumeColDefEOF(MyEOFPktResponse colEof) {
	}

	@Override
	void consumeError(MyErrorResponse error) {
	}

}
