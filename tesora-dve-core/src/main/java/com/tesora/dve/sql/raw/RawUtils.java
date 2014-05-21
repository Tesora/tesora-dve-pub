// OS_STATUS: public
package com.tesora.dve.sql.raw;

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

import java.sql.Types;

import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.raw.jaxb.Rawplan;

public class RawUtils {

	public static IntermediateResultSet buildRawExplainResults(Rawplan rp) {
		String xml = null;
		try {
			xml = PEXmlUtils.marshalJAXB(rp);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to marshal raw plan object");
		}
		ColumnSet cs = new ColumnSet();

		cs.addColumn("rawplan", xml.length(), "varchar",Types.VARCHAR);
		ResultRow rr = new ResultRow();
		rr.addResultColumn(xml);
		return new IntermediateResultSet(cs,rr);
	}
}
