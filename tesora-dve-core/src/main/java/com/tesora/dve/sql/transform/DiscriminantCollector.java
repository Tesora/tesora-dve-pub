package com.tesora.dve.sql.transform;

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


import java.util.Collection;
import java.util.List;

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class DiscriminantCollector extends KeyCollector {

	private DiscriminantCollector(SchemaContext sc, LanguageNode e) {
		super(sc, e);
	}
	
	public static List<Part> getDiscriminants(SchemaContext sc, LanguageNode wc) {
		DiscriminantCollector dc = new DiscriminantCollector(sc, wc);
		return dc.getParts();
	}
	
	@Override
	protected boolean isQualifyingColumn(ColumnInstance ci) {
		return isQualifyingColumn(ci.getPEColumn());
	}

	@Override
	protected boolean isQualifyingColumn(PEColumn c) {
		return c.isPartOfContainerDistributionVector();
	}

	@Override
	protected boolean isComplete(EqualityPart sp) {
		return isSame(sp.getColumns(),sp.getTableKey().getAbstractTable().getDiscriminantColumns(context));
	}

	@Override
	protected boolean isComplete(AndedParts cp) {
		return isSame(cp.getColumns(),cp.getTableKey().getAbstractTable().getDiscriminantColumns(context));
	}

	@Override
	protected AndedParts maybeMakeComplete(EqualityPart sp) {
		return null;
	}

	@Override
	protected AndedParts maybeMakeComplete(AndedParts cp) {
		return null;
	}

	@Override
	protected List<ColumnKey> getNeeded(Collection<ColumnKey> in) {
		TableKey tk = in.iterator().next().getTableKey();
		ListSet<ColumnKey> discCols = buildKeyColumns(tk, tk.getAbstractTable().getDiscriminantColumns(context)); 
		discCols.removeAll(in);
		return discCols;
	}

}
