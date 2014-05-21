// OS_STATUS: public
package com.tesora.dve.db.mysql;

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
