package com.tesora.dve.sql.transform.strategy;

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

import java.util.Collection;

import com.tesora.dve.common.LinkedHashSetFactory;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.Functional;

public class IndexCollector {

	private MultiMap<PETable,PEColumn> collector = new MultiMap<PETable,PEColumn>(new LinkedHashSetFactory<PEColumn>());
	
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
