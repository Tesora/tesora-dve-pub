// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

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

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListOfPairs;

public class EquijoinInfo {

	protected PEAbstractTable<?> lhs;
	protected Database<?> ldb;
	protected PEAbstractTable<?> rhs;
	protected Database<?> rdb;
	protected JoinSpecification type;

	protected ListOfPairs<PEColumn, PEColumn> joinExpressions;

	public EquijoinInfo(PEAbstractTable<?> l, PEAbstractTable<?> r, JoinSpecification spec, SchemaContext sc) {
		lhs = l;
		rhs = r;
		joinExpressions = new ListOfPairs<PEColumn, PEColumn>();
		type = spec;
		this.ldb = lhs.getDatabase(sc);
		this.rdb = rhs.getDatabase(sc);
	}

	public PEAbstractTable<?> getLHS() {
		return lhs;
	}

	public PEAbstractTable<?> getRHS() {
		return rhs;
	}

	public Database<?> getLDB() {
		return ldb;
	}

	public Database<?> getRDB() {
		return rdb;
	}

	public JoinSpecification getType() {
		return type;
	}

	protected void addJoinExpression(ColumnInstance lc, ColumnInstance rc) {
		if (lc.getPEColumn().getTable().equals(lhs)) {
			joinExpressions.add(lc.getPEColumn(), rc.getPEColumn());
		} else {
			joinExpressions.add(rc.getPEColumn(), lc.getPEColumn());
		}
	}

	public ListOfPairs<PEColumn, PEColumn> getEquijoins() {
		return joinExpressions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result)
				+ ((joinExpressions == null) ? 0 : joinExpressions.hashCode());
		result = (prime * result) + ((lhs == null) ? 0 : lhs.hashCode());
		result = (prime * result) + ((rhs == null) ? 0 : rhs.hashCode());
		result = (prime * result) + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof EquijoinInfo)) {
			return false;
		}

		final EquijoinInfo other = (EquijoinInfo) obj;
		return (ObjectUtils.equals(this.joinExpressions, other.joinExpressions)
				&& ObjectUtils.equals(this.lhs, other.lhs)
				&& ObjectUtils.equals(this.rhs, other.rhs)
				&& ObjectUtils.equals(this.type, other.type));
	}

}
