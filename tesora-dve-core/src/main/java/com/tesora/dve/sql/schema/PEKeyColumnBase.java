package com.tesora.dve.sql.schema;

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

	public abstract boolean isForwardKeyColumn();
	
}
