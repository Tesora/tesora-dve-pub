// OS_STATUS: public
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

import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.exceptions.PEException;

public class PEForeignKeyColumn extends PEKeyColumn {

	private UnqualifiedName columnName;
	private boolean isForward;
	
	public PEForeignKeyColumn(PEColumn c, PEColumn targCol) {
		super(c, null, -1L);
		columnName = targCol.getName().getUnqualified();
		isForward = false;
	}

	public PEForeignKeyColumn(PEKey key, PEColumn column, PEColumn targCol) {
		super(key,column,null, -1L);
		columnName = targCol.getName().getUnqualified();
		isForward = false;
	}
	
	public PEForeignKeyColumn(PEColumn c, UnqualifiedName missingColumn) {
		super(c,null, -1L);
		columnName = missingColumn;
		isForward = true;
	}
	
	public PEForeignKeyColumn(PEColumn c, UnqualifiedName columnName, boolean forward) {
		super(c,null, -1L);
		this.columnName = columnName;
		this.isForward = forward;
	}
	
	private PEForeignKey getFK() {
		return (PEForeignKey)getKey();
	}
	
	public PEColumn getTargetColumn(SchemaContext sc) {
		if (isForward) return null;
		PETable targetTable = getFK().getTargetTable(sc);
		return targetTable.lookup(sc, columnName);
	}
	
	public void setTargetColumn(PEColumn pecs) {
		columnName = pecs.getName().getUnqualified();
		isForward = false;
	}
	
	public void revertToForward(SchemaContext sc) {
		isForward = true;
	}
	
	public UnqualifiedName getTargetColumnName() {
		return columnName;
	}
	
	protected PEForeignKeyColumn(SchemaContext sc, KeyColumn kc, PETable enclosing) {
		super(sc,kc, enclosing);
		if (kc.getTargetColumn() != null) {
			PETable targTab = PETable.load(kc.getTargetColumn().getUserTable(),sc).asTable();
			PEColumn tpec = PEColumn.load(kc.getTargetColumn(), sc, targTab);
			columnName = tpec.getName().getUnqualified();
			isForward = false;
		} else {
			columnName = new UnqualifiedName(kc.getTargetColumnName());
			isForward = true;
		}
	}
	
	@Override
	protected KeyColumn createEmptyNew(SchemaContext sc) throws PEException {
		int offset = ofKey.getKeyColumns().indexOf(this) + 1;
		KeyColumn kc = null;
		if (isForward) {
			kc = new KeyColumn(column.persistTree(sc), offset, columnName.getUnquotedName().get());
		} else {
			kc = new KeyColumn(column.persistTree(sc),offset,getTargetColumn(sc).persistTree(sc));
		}
		sc.getSaveContext().add(this,kc);
		return kc;
	}

	@Override
	protected void updateExisting(SchemaContext sc, KeyColumn p) throws PEException {
		if (isForward) {
			p.setTargetColumn(columnName.getUnquotedName().get());
		} else {
			p.setTargetColumn(getTargetColumn(sc).persistTree(sc));
		}
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEKeyColumn, KeyColumn> other, 
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEForeignKeyColumn oth = (PEForeignKeyColumn) other.get();

		if (visited.contains(this) && visited.contains(oth)) {
			return false;
		}
		visited.add(this);
		visited.add(oth);

		if (maybeBuildDiffMessage(sc,messages, "forward ref", isForward, oth.isForward, first, visited)) 
			return true;
		if (maybeBuildDiffMessage(sc,messages, "referred column", getTargetColumn(sc), oth.getTargetColumn(sc), first, visited)) 
			return false;
		return super.collectDifferences(sc,messages,other,first, visited);

	}
	
	@Override
	public PEKeyColumn copy(SchemaContext sc, PETable tab) {
		PEColumn inTab = (PEColumn) getColumn().getIn(sc, tab);
		if (isForward)
			return new PEForeignKeyColumn(inTab, columnName);
		else 
			return new PEForeignKeyColumn(inTab, getTargetColumn(sc));
	}

}
