package com.tesora.dve.sql.schema;

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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.util.Functional;

public class DistributionKeyTemplate {

	private Map<Integer, String> columns;
	private PEAbstractTable<?> distLike;
	
	public DistributionKeyTemplate(PEAbstractTable<?> dl) {
		columns = new TreeMap<Integer,String>();
		distLike = dl;
	}
	
	public void addColumn(ExpressionNode c, Integer p) {
		String name = getColumnName(c);
		String was = columns.put(p, name);
		if (was != null)
			throw new SchemaException(Pass.PLANNER,"Duplicate distribution key column position");
	}
	
	public String describe(SchemaContext sc) {
		return getModel(sc).getSQL() + " on {" + Functional.join(getColumnNames(), ",") + "}";
	}
	
	public boolean usesColumns(SchemaContext sc) {
		if (distLike == null)
			return getModel(sc).getUsesColumns();
		return getVector(sc).usesColumns(sc);
	}
	
	public Model getModel(SchemaContext sc) {
		return distLike.getDistributionVector(sc).getModel();
	}
	
	public DistributionVector getVector(SchemaContext sc) {
		if (distLike == null) return null;
		return distLike.getDistributionVector(sc);
	}
	
	public PEAbstractTable<?> getTable() {
		return distLike;
	}
	
	public List<String> getColumnNames() {
		return Functional.toList(columns.values());
	}
	
	private static ExpressionAlias getEnclosingExpressionAlias(ExpressionNode e) {
		ExpressionAlias ea = null;
		if (e instanceof ExpressionAlias) {
			ea = (ExpressionAlias) e;
		} else {
			LanguageNode ln = e.getParent();
			if (ln instanceof ExpressionAlias) {
				ea = (ExpressionAlias) ln;
			}
		}
		if (ea == null)
			throw new SchemaException(Pass.PLANNER, "Dist expression '" + e + "' not part of projection");
		return ea;
	}
	
	public static String getColumnName(ExpressionNode e) {
		return getEnclosingExpressionAlias(e).getAlias().get();
	}
}
