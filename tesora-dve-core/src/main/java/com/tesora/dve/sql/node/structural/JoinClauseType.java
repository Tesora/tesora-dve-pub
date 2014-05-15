// OS_STATUS: public
package com.tesora.dve.sql.node.structural;

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
