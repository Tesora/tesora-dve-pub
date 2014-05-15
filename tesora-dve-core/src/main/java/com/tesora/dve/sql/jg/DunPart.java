// OS_STATUS: public
package com.tesora.dve.sql.jg;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class DunPart extends DPart {

	protected ListSet<TableKey> table;

	public DunPart(TableKey tk, int id) {
		super(id);
		table = new ListSet<TableKey>();
		table.add(tk);
	}
	
	@Override
	public boolean isUnary() {
		return true;
	}

	@Override
	public ListSet<TableKey> getTables() {
		return table;
	}

	@Override
	public String getGraphRole() {
		return "UnaryPartition";
	}

	@Override
	public List<JoinEdge> getEmbeddedJoins() {
		return Collections.emptyList();
	}

	@Override
	public List<DunPart> getUnaryParts() {
		return Collections.singletonList(this);
	}
	
	@Override
	public DistributionVector getGoverningVector(SchemaContext sc, TableKey forTable) {
		return forTable.getAbstractTable().getDistributionVector(sc);
	}

}
