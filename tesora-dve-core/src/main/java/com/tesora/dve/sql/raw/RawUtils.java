// OS_STATUS: public
package com.tesora.dve.sql.raw;

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
