package com.tesora.dve.sql.node.expression;

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

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.transform.CopyContext;

public class IntervalExpression extends ExpressionNode {
	
	private final SingleEdge<IntervalExpression,ExpressionNode> target =
			new SingleEdge<IntervalExpression,ExpressionNode>(IntervalExpression.class,this,EdgeName.INTERVAL_TARGET);
	String unit = null;
	
	public IntervalExpression(ExpressionNode expr_unit, String unit, SourceLocation orig) {
		super(orig);
		this.target.set(expr_unit);
		this.unit = unit;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return (List<T>) Collections.singletonList(target);
	}
	
	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		IntervalExpression nfc = new IntervalExpression(target.get(), unit, null);
		return nfc;
	}

	public ExpressionNode getExpr_unit() {
		return target.get();
	}

	public String getUnit() {
		return unit;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		IntervalExpression ofc = (IntervalExpression) other;
		return StringUtils.equals(unit, ofc.getUnit());
	}

	@Override
	protected int selfHashCode() {
		return unit.hashCode();
	}

}
