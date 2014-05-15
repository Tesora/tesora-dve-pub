// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

// ensure that temp tables are not used before they are scheduled
public class TempTableSanity {

	private ListSet<TempTable> scheduled;
	
	public TempTableSanity() {
		scheduled = new ListSet<TempTable>();
	}

	public void assertSane(SchemaContext sc, DMLStatement dmls) throws PEException {
		for(TableKey tk : dmls.getDerivedInfo().getAllTableKeys()) {
			if (!(tk.getTable() instanceof PETable)) continue;
			PETable pet = (PETable) tk.getTable();
			if (!pet.isTempTable()) continue;
			if (!scheduled.contains(pet)) {
				String message = "Internal error: use of temp table " + pet.getName(sc) + " before definition";
//				System.err.println(message);
				throw new PEException(message);
			}
		}
	}
	
	public void onTempTableDefinition(TempTable tt) {
		scheduled.add(tt);
	}
	
}
