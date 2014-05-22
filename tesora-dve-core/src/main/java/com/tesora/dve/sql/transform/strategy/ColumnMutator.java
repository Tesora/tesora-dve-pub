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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;

public abstract class ColumnMutator {

	protected int beforeOffset = -1;
	protected int afterOffsetBegin = -1;
	protected int afterOffsetEnd = -1;
	
	public ColumnMutator() {
		
	}
	
	public void setBeforeOffset(int v) {
		beforeOffset = v;
	}
	
	public int getBeforeOffset() {
		return beforeOffset;
	}
	
	public void setAfterOffsetBegin(int v) {
		afterOffsetBegin = v;
	}
	
	public void setAfterOffsetEnd(int v) {
		afterOffsetEnd = v;
	}
	
	public int getAfterOffsetBegin() {
		return afterOffsetBegin;
	}
	
	public int getAfterOffsetEnd() {
		return afterOffsetEnd;
	}
	
	public boolean containsAfterOffset(int v) {
		return afterOffsetBegin <= v && v <= afterOffsetEnd;
	}
	
	public static ExpressionNode getProjectionEntry(List<ExpressionNode> proj, int offset) {
		ExpressionNode entry = proj.get(offset);
		if (entry instanceof ExpressionAlias)
			entry = ((ExpressionAlias)entry).getTarget();
		return entry;
	}

	public static List<ExpressionNode> getProjectionEntries(List<ExpressionNode> proj, int begin, int end) {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		for(int i = begin; i < end; i++) {
			out.add(getProjectionEntry(proj,i));
		}
		return out;
	}
	
	protected List<ExpressionNode> getSingleColumn(List<ExpressionNode> proj, int offset) {
		return Collections.singletonList(proj.get(offset));
	}
	
	public abstract List<ExpressionNode> adapt(SchemaContext sc, List<ExpressionNode> proj, MutatorState ms);
	public abstract List<ExpressionNode> apply(List<ExpressionNode> proj, ApplyOption opts);
}