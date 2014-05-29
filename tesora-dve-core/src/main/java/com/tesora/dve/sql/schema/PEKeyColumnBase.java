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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.KeyColumn;

public abstract class PEKeyColumnBase extends Persistable<PEKeyColumn, KeyColumn> {

	protected Integer length;
	protected PEKey ofKey;	
	protected long cardinality;
	
	public PEKeyColumnBase(Integer l, long card) {
		this(null, l, card);
	}
	
	public PEKeyColumnBase(PEKey key, Integer length, long card) {
		super(null);
		this.length = length;
		this.ofKey = key;
		this.cardinality = card;
	}

	// for loading
	protected PEKeyColumnBase() {
		super(null);
	}
	
	public PEKey getKey() {
		return ofKey;
	}
	
	public Integer getLength() {
		return length;
	}
	
	public long getCardinality() {
		return cardinality;
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
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return KeyColumn.class;
	}

	@Override
	protected int getID(KeyColumn p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return "KeyColumn";
	}

	public void setKey(PEKey k) {
		ofKey = k;
	}
	
	public abstract UnqualifiedName getName();
	
	public abstract PEColumn getColumn();
	
	public abstract Integer getIndexSize();
	
	public abstract PEKeyColumnBase copy(SchemaContext sc, PETable containingTable);

	public abstract boolean isUnresolved();

	public abstract PEKeyColumnBase resolve(PEColumn actual);
	
}
