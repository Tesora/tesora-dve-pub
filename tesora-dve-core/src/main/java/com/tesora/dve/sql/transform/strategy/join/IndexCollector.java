// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.Collection;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.MultiMap.HashedCollectionFactory;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.Functional;

public class IndexCollector {

	private MultiMap<PETable,PEColumn> collector = new MultiMap<PETable,PEColumn>(new HashedCollectionFactory<PEColumn>());
	
	public IndexCollector() {
		
	}
	
	public void addColumnInstance(ColumnInstance nci) {
		if (nci.getTableInstance().getAbstractTable().isTempTable())
			collector.put(nci.getTableInstance().getAbstractTable().asTable(),	nci.getPEColumn());
	}
	
	public void setIndexes(SchemaContext sc) {
		for(PETable pet : collector.keySet()) {
			Collection<PEColumn> sub = collector.get(pet);
			if (sub == null || sub.isEmpty()) continue;
			TempTable tt = (TempTable) pet;
			tt.noteJoinedColumns(sc,Functional.toList(sub));
		}
	}

	public static void collect(SchemaContext sc, ExpressionNode en) {
		IndexCollector ic = new IndexCollector();
		for(ColumnInstance ci : ColumnInstanceCollector.getColumnInstances(en))
			ic.addColumnInstance(ci);
		ic.setIndexes(sc);
	}
	
}
