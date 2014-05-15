// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.aggregation;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.node.EngineBlock;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

public class ProjectionCharacterization {
	
	private static final Object characterKey = new Object();

	public static ProjectionCharacterization getProjectionCharacterization(SelectStatement ss) {
		EngineBlock eb = ss.getBlock();
		ProjectionCharacterization pc = (ProjectionCharacterization)eb.getFromStorage(characterKey);
		if (pc == null) {
			pc = new ProjectionCharacterization(ss);
			eb.store(characterKey, pc);
		}
		return pc;
	}

	
	protected List<ColumnCharacterization> characterizations;
	protected Boolean grandAgg = null;
	protected Boolean anyAgg = null;
	protected Boolean anyAggNoCount = null;
	protected Boolean distinctAgg = null;
	
	public ProjectionCharacterization(SelectStatement in) {
		characterizations = new ArrayList<ColumnCharacterization>();
		for(ExpressionNode en : in.getProjection()) {
			ExpressionAlias ea = (ExpressionAlias) en;
			characterizations.add(new ColumnCharacterization(ea));
		}
	}
	
	public List<ColumnCharacterization> getColumns() {
		return characterizations;
	}
	
	public boolean anyAggFuns() {
		if (anyAgg == null) {
			anyAgg = Functional.any(characterizations, new UnaryPredicate<ColumnCharacterization>() {

				@Override
				public boolean test(ColumnCharacterization object) {
					return object.hasAnyAggFuns();
				}

			});
		}
		return anyAgg.booleanValue();
	}
	
	public boolean anyAggFunsNoCount() {
		if (anyAggNoCount == null) {
			anyAggNoCount = Functional.any(characterizations, new UnaryPredicate<ColumnCharacterization>() {

				@Override
				public boolean test(ColumnCharacterization object) {
					return object.hasAnyAggFunsNoCount();
				}

			});
		}
		return anyAggNoCount.booleanValue();
	}

}