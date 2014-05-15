// OS_STATUS: public
package com.tesora.dve.sql.transform;

import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.ListOfPairs;

public interface MatchableKey {

	public boolean isComplete(SchemaContext sc, Set<PEColumn> have, boolean partialOk);
	
	public List<PEColumn> getColumns(SchemaContext sc);

	public PlanningConstraint buildEmptyConstraint(SchemaContext sc, TableKey tk, ListOfPairs<PEColumn,ConstantExpression> values);

	public long getCardRatio(SchemaContext sc);
}
