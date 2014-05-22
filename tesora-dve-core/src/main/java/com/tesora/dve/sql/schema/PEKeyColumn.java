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
import java.util.Set;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.exceptions.PEException;

public class PEKeyColumn extends PEKeyColumnBase {

	protected PEColumn column;
	
	public PEKeyColumn(PEColumn c, Integer l, long card) {
		this(null, c, l, card);
	}
	
	public PEKeyColumn(PEKey key, PEColumn column, Integer length, long card) {
		super(key,length,card);
		this.column = column;
	}
	
	public PEColumn getColumn() {
		return this.column;
	}
	
	public void setKey(PEKey k) {
		ofKey = k;
		if (ofKey.getConstraint() == ConstraintType.PRIMARY)
			column.setPrimaryKeyPart();
		else if (ofKey.getConstraint() == ConstraintType.UNIQUE)
			column.setUniqueKeyPart();
		else if (ofKey.getConstraint() == null)
			column.setKeyPart();
	}
	
	public static PEKeyColumn load(KeyColumn kc, SchemaContext sc, PETable enclosingTable) {
		PEKeyColumn pec = null;
		if (pec == null) {
			if (kc.getKey().isForeignKey())
				pec = new PEForeignKeyColumn(sc,kc, enclosingTable);
			else
				pec = new PEKeyColumn(sc,kc, enclosingTable);
		}
		return pec;
	}
	
	protected PEKeyColumn(SchemaContext sc, KeyColumn kc, PETable enclosingTable) {
		super();
		sc.startLoading(this, kc);
		column = PEColumn.load(kc.getSourceColumn(), sc, enclosingTable);
		length = kc.getLength();
		cardinality = (kc.getCardinality() == null ? -1 : kc.getCardinality());
		setPersistent(sc,kc,kc.getId());
		sc.finishedLoading(this, kc);
	}
	
	@Override
	protected KeyColumn lookup(SchemaContext sc) throws PEException {
		Key k = ofKey.persistTree(sc);
		for(KeyColumn kc : k.getColumns()) {
			if (kc.getSourceColumn().getName().equals(column.getName().getUnquotedName().get()))
				return kc;
		}
		return null;
	}

	@Override
	protected KeyColumn createEmptyNew(SchemaContext sc) throws PEException {
		int offset = ofKey.getPositionOf(sc, this);
		KeyColumn kc = new KeyColumn(column.persistTree(sc),this.length,offset,(getKey().isForeign() ? null : cardinality));
		sc.getSaveContext().add(this,kc);
		return kc;
	}

	@Override
	protected void populateNew(SchemaContext sc, KeyColumn p)
			throws PEException {
	}

	@Override
	protected Persistable<PEKeyColumn, KeyColumn> load(SchemaContext sc,
			KeyColumn p) throws PEException {
		return new PEKeyColumn(sc, p, null);
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEKeyColumn, KeyColumn> other, 
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEKeyColumn oth = other.get();

		if (visited.contains(this) && visited.contains(oth)) {
			return false;
		}
		visited.add(this);
		visited.add(oth);
		
		if (maybeBuildDiffMessage(sc,messages, "column", getColumn(), oth.getColumn(), first, visited))
			return true;
		return false;
	}
	
	public PEKeyColumn copy(SchemaContext sc, PETable containingTable) {
		PEColumn inTab = (PEColumn) column.getIn(sc, containingTable);
		return new PEKeyColumn(inTab,null,cardinality);
	}
	
	public Integer getIndexSize() {
		return column.getIndexSize();
	}
	
	@Override
	public boolean isForwardKeyColumn() {
		// always, even for forward fks - this is more about whether the column has been declared yet or not
		return false;
	}
}
