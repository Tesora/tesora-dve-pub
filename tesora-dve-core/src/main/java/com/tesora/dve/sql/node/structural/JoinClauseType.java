package com.tesora.dve.sql.node.structural;

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
import com.tesora.dve.sql.schema.Name;

public class JoinClauseType 
{

	public enum ClauseType {
		ON, USING
	}

	private ClauseType clauseType = ClauseType.ON;
	private ExpressionNode node = null;
	private List<Name> columnIdentifiers = null;
	
	public JoinClauseType(ExpressionNode en, List<Name> columnIdentifiers, ClauseType clauseType) {
		this.setNode(en);
		if (columnIdentifiers != null) {
			this.columnIdentifiers = new ArrayList<Name>();
			this.columnIdentifiers.addAll(columnIdentifiers);
		}
		this.clauseType = clauseType;
	}

	public ClauseType getClauseType() {
		return clauseType;
	}

	public void setClauseType(ClauseType clauseType) {
		this.clauseType = clauseType;
	}

	public ExpressionNode getNode() {
		return node;
	}

	public void setNode(ExpressionNode node) {
		this.node = node;
	}

	public List<Name> getColumnIdentifiers() {
		return columnIdentifiers;
	}

	public void setColumnIdentifiers(List<Name> columnIdentifiers) {
		this.columnIdentifiers = columnIdentifiers;
	}

}
