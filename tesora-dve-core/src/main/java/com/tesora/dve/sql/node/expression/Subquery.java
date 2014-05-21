// OS_STATUS: public
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

import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;

public class Subquery extends ExpressionNode {

	private SingleEdge<Subquery, ProjectingStatement> stmt =
		new SingleEdge<Subquery, ProjectingStatement>(Subquery.class, this, EdgeName.SUBQUERY);
	private PEAbstractTable<?> table = null;
	private Name alias;
	
	public Subquery(ProjectingStatement ss, Name specifiedAlias, SourceLocation orig) {
		super(orig);
		stmt.set(ss);
		alias = specifiedAlias;
	}
	
	public ProjectingStatement getStatement() {
		return stmt.get();
	}
	
	public Name getAlias() {
		return alias;
	}

	public PEAbstractTable<?> getTable() {
		return table;
	}
	
	public void setTable(PEAbstractTable<?> sqt) {
		table = sqt;
	}
	
	@Override
	public NameAlias buildAlias(SchemaContext sc) {
		if (alias != null) return new NameAlias(alias.getUnqualified());
		return new NameAlias(new UnqualifiedName("subq"));
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		ProjectingStatement sub = CopyVisitor.copy(stmt.get(), cc);
		Subquery nsq = new Subquery(sub, alias, getSourceLocation());
		nsq.setTable(table);
		return nsq;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<? extends Edge<?,?>> getEdges() {
		return Collections.singletonList(stmt);
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return true;
	}

	@Override
	protected int selfHashCode() {
		return 0;
	}

}
