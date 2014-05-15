// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.util.Functional;

public class DistributionKeyTemplate {

	private Map<Integer, String> columns;
	private PEAbstractTable<?> distLike;
	
	public DistributionKeyTemplate(PEAbstractTable<?> dl) {
		columns = new TreeMap<Integer,String>();
		distLike = dl;
	}
	
	public void addColumn(ExpressionNode c, Integer p) {
		String name = getColumnName(c);
		String was = columns.put(p, name);
		if (was != null)
			throw new SchemaException(Pass.PLANNER,"Duplicate distribution key column position");
	}
	
	public String describe(SchemaContext sc) {
		return getModel(sc).getSQL() + " on {" + Functional.join(getColumnNames(), ",") + "}";
	}
	
	public boolean usesColumns(SchemaContext sc) {
		if (distLike == null)
			return getModel(sc).getUsesColumns();
		return getVector(sc).usesColumns(sc);
	}
	
	public Model getModel(SchemaContext sc) {
		return distLike.getDistributionVector(sc).getModel();
	}
	
	public DistributionVector getVector(SchemaContext sc) {
		if (distLike == null) return null;
		return distLike.getDistributionVector(sc);
	}
	
	public PEAbstractTable<?> getTable() {
		return distLike;
	}
	
	public List<String> getColumnNames() {
		return Functional.toList(columns.values());
	}
	
	private static ExpressionAlias getEnclosingExpressionAlias(ExpressionNode e) {
		ExpressionAlias ea = null;
		if (e instanceof ExpressionAlias) {
			ea = (ExpressionAlias) e;
		} else {
			LanguageNode ln = e.getParent();
			if (ln instanceof ExpressionAlias) {
				ea = (ExpressionAlias) ln;
			}
		}
		if (ea == null)
			throw new SchemaException(Pass.PLANNER, "Dist expression '" + e + "' not part of projection");
		return ea;
	}
	
	public static String getColumnName(ExpressionNode e) {
		return getEnclosingExpressionAlias(e).getAlias().get();
	}
}
