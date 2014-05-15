// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;


import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;

public abstract class ProjectionMutator {

	protected List<ColumnMutator> columns;
	protected final SchemaContext context;
	
	public ProjectionMutator(SchemaContext sc) {
		columns = new ArrayList<ColumnMutator>();
		context = sc;
	}
	
	public List<ColumnMutator> getMutators() {
		return columns;
	}

	public abstract List<ExpressionNode> adapt(MutatorState ms, List<ExpressionNode> proj);

	/**
	 * @param planner
	 * @param ms
	 * @param proj
	 * @param options
	 * @return
	 */
	public List<ExpressionNode> apply(FeaturePlanner planner, MutatorState ms, List<ExpressionNode> proj, ApplyOption options) {
		if (planner != null && planner.emitting())
			planner.emit("pre:  " + this.getClass().getSimpleName() + ".apply(" + proj + ", " + options + ")");
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ColumnMutator cm : columns) {
			out.addAll(cm.apply(proj, options));
		}
		if (planner != null && planner.emitting())
			planner.emit("post: " + this.getClass().getSimpleName() + ".apply(" + out + ", " + options + ")");

		return out;
		
	}
	
	public List<ExpressionNode> applyAdapted(List<ExpressionNode> proj, MutatorState ms) {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(ColumnMutator cm : columns) {
			cm.setAfterOffsetBegin(out.size());
			out.addAll(cm.adapt(context,proj,ms));
			cm.setAfterOffsetEnd(out.size());
		}
		return out;
	}
}