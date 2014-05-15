// OS_STATUS: public
package com.tesora.dve.sql.jg;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

// an n-way join
public class DGMultiJoin extends DGJoin {

	public DGMultiJoin(JoinedTable enclosing, int branch, int offset, boolean fixedLocation, ListSet<TableKey> left, TableKey right) {
		super(enclosing, branch, offset, fixedLocation, left, right);
	}

	List<JoinEdge> components;
			
	@Override
	public boolean isMultijoin() {
		return true;
	}

	@Override
	public void describe(SchemaContext sc, StringBuilder buf) {
		buf.append("multi ");
		super.describe(sc, buf);
	}


	@Override
	public List<ExpressionNode> getRedistJoinExpressions(final TableKey forTable) {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		if (forTable.equals(rhs)) {
			for(JoinEdge je : forEdges) {
				out.addAll(je.getRedistJoinExpressions(rhs));
			}
		} else {
			for(JoinEdge je : forEdges) {
				out.addAll(je.getRedistJoinExpressions(je.getLHSTab()));
			}
		}
		return out;
	}

	
}
