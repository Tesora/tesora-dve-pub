package com.tesora.dve.sql.schema;

import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.exceptions.PEException;

public class PEForwardKeyColumn extends PEKeyColumnBase {

	protected UnqualifiedName forwardName;
	
	public PEForwardKeyColumn(PEKey key, UnqualifiedName forwardName, Integer length, long card) {
		super(key,length,card);
		this.forwardName = forwardName;
	}
	
	public PEKeyColumn resolve(PEColumn actual) {
		return new PEKeyColumn(actual,getLength(),getCardinality());
	}
	
	
	@Override
	protected KeyColumn lookup(SchemaContext sc) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected KeyColumn createEmptyNew(SchemaContext sc) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void populateNew(SchemaContext sc, KeyColumn p)
			throws PEException {
		// TODO Auto-generated method stub

	}

	@Override
	protected Persistable<PEKeyColumn, KeyColumn> load(SchemaContext sc,
			KeyColumn p) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean isForwardKeyColumn() {
		return true;
	}

}
