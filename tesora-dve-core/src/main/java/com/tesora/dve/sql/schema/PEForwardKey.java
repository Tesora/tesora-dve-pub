package com.tesora.dve.sql.schema;

import java.util.List;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.ListOfPairs;

public class PEForwardKey extends PEKeyBase {

	// we do the base class since the key may be forward because some, but not all, of the columns are forward
	private List<PEKeyColumnBase> columns;
	
	public PEForwardKey(Name n, List<PEKeyColumnBase> columns, IndexType k, ConstraintType cons, UnqualifiedName sym, Comment com) {
		super(n,k,cons,sym,com,false,false);
		this.columns = columns;
	}
	
	public List<PEKeyColumnBase> getKeyColumns() {
		return columns;
	}
	
	public PEKey resolve(List<PEKeyColumn> resolvedColumns) {
		PEKey pek = new PEKey(getName(), getType(), resolvedColumns, getComment(), isSynthetic());
		if (getConstraint() != null)
			pek.setConstraint(getConstraint());
		if (getSymbol() != null)
			pek.setSymbol(getSymbol());
		return pek;
	}
	
	@Override
	public PEKey getIn(SchemaContext sc, PEAbstractTable<?> tab) {
		throw new SchemaException(Pass.FIRST, "Illegal call to TableComponent.getIn()");
	}

	@Override
	public void take(SchemaContext sc, PEKey tc) {
		throw new SchemaException(Pass.FIRST, "Illegal call to TableComponent.take()");
		
	}
	@Override
	public List<PEColumn> getColumns(SchemaContext sc) {
		throw new SchemaException(Pass.FIRST, "Illegal call to MatchableKey.getColumns");
	}

	@Override
	public PlanningConstraint buildEmptyConstraint(SchemaContext sc,
			TableKey tk, ListOfPairs<PEColumn, ConstantExpression> values) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public long getCardRatio(SchemaContext sc) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	protected Key createEmptyNew(SchemaContext sc) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	protected void populateNew(SchemaContext sc, Key p) throws PEException {
		// TODO Auto-generated method stub
		
	}
	@Override
	protected Persistable<PEKey, Key> load(SchemaContext sc, Key p)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean isForwardKey() {
		return true;
	}

}
