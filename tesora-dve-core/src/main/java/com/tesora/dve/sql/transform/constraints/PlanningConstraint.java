// OS_STATUS: public
package com.tesora.dve.sql.transform.constraints;


import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.MatchableKey;

public interface PlanningConstraint extends Comparable<PlanningConstraint> {

	public PlanningConstraintType getType();

	// columns in the constraint, in declaration order
	public List<PEColumn> getColumns(SchemaContext sc);
	
	public Map<PEColumn, ConstantExpression> getValues();
	
	public PEAbstractTable<?> getTable();
	
	// the specific table this constraint is against (i.e. self joins with different constraints)
	public TableKey getTableKey();
	
	public ExpressionPath getPath();
	
	public boolean sameUnderlyingKey(PlanningConstraint other);
		
	public MatchableKey getKey(SchemaContext sc);
	
	// this is an estimate
	// return -1 if the value is unknown
	public long getRowCount();
	
}
