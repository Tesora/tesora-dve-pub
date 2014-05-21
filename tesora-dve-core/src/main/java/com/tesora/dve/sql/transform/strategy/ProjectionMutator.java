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