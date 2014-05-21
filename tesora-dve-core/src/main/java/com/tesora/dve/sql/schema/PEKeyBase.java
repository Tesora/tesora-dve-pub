package com.tesora.dve.sql.schema;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.transform.constraints.PlanningConstraintType;

public abstract class PEKeyBase extends Persistable<PEKey, Key> implements TableComponent<PEKey>, MatchableKey, HasComment {

	protected IndexType type;
	protected SchemaEdge<PETable> table;
	protected Comment comment;
	protected ConstraintType constraint;
	protected UnqualifiedName symbol;
	protected boolean synthetic;
	protected boolean persisted;
	protected boolean hidden;
	protected Long cardRatio = null;

	
	public PEKeyBase(Name n, IndexType k, Comment anyComment) {
		this(n,k,anyComment,false);
	}
	
	public PEKeyBase(Name n, IndexType k, Comment anyComment, boolean synthetic) {
		super(null);
		setName(n);
		type = k;
		comment = anyComment;
		this.synthetic = synthetic;
		this.hidden = false;
		persisted = true; // default
	}

	protected PEKeyBase(Name n, IndexType k, ConstraintType cons, UnqualifiedName sym, Comment com, boolean synthetic, boolean hidden) {
		super(null);
		setName(n);
		type = k;
		constraint = cons;
		symbol = sym;
		comment = com;
		this.synthetic = synthetic;
		this.hidden = hidden;
		persisted = true; // default
	}
	
	protected PEKeyBase() {
		super(null);
	}
	
	public PETable getTable(SchemaContext sc) {
		return table.get(sc);
	}
	
	public void setTable(SchemaEdge<PETable> tab) {
		table = tab;
	}

	public void setConstraint(ConstraintType ct) {
		constraint = ct;
	}
	
	public ConstraintType getConstraint() {
		return constraint;
	}
	
	public UnqualifiedName getSymbol() {
		return symbol;
	}
	
	public void setSymbol(Name symbol) {
		this.symbol = (symbol == null ? null : symbol.getUnqualified());
	}
	
	public boolean isUnique() {
		return constraint == ConstraintType.UNIQUE || constraint == ConstraintType.PRIMARY;
	}
	
	public boolean isForeign() {
		return constraint == ConstraintType.FOREIGN;
	}
	
	public boolean isPrimary() {
		return constraint == ConstraintType.PRIMARY;
	}
		
	public boolean isSynthetic() {
		return synthetic;
	}
	
	public void setSynthetic() {
		synthetic = true;
	}
	
	public boolean isPersisted() {
		return persisted;
	}
	
	public void setPersisted(boolean v) {
		persisted = v;
	}
	
	public boolean isHidden() {
		return hidden;
	}
	
	public void setHidden(boolean v) {
		hidden = v;
	}
	
	public IndexType getType() {
		return type;
	}
	
	public PlanningConstraintType getPlanningType() {
		if (constraint == ConstraintType.PRIMARY)
			return PlanningConstraintType.PRIMARY;
		if (constraint == ConstraintType.UNIQUE)
			return PlanningConstraintType.UNIQUE;
		return PlanningConstraintType.REGULAR;
	}
	
	@Override
	public PEKey getIn(SchemaContext pc, PEAbstractTable<?> tab) {
		for(PEKey k : tab.getKeys(pc)) {
			if (getName().equals(k.getName()))
//				|| getName().equals(k.getConstraintName()))
				return k;
		}
		return null;
	}

	@Override
	public boolean isComplete(SchemaContext sc, Set<PEColumn> cols, boolean partialOk) {
		// partial is ok if the card ratio is valid
		boolean partial = partialOk && (getCardRatio(sc) > -1);
		List<PEColumn> keyCols = getColumns(sc);
		HashSet<PEColumn> mycols = new HashSet<PEColumn>(keyCols);
		mycols.removeAll(cols);
		if (mycols.isEmpty())
			return true;
		if (partial) 
			// with a partial match we have to match at least column
			return mycols.size() < keyCols.size();
		return false;
	}
	

	
	@Override
	public void take(SchemaContext pc, PEKey targ) {
		throw new SchemaException(Pass.PLANNER, "Invalid alter action: modify a key (" + getName().getSQL() + ")");
	}
	
	// return true only if the rhs has the same prefix as the lhs - 
	// that is all the columns in lhs are present in rhs in the same order
	// so rhs could have more columns
	public static boolean samePrefix(PEKey left, PEKey right) {
		List<PEKeyColumn> leftCols = left.getKeyColumns();
		List<PEKeyColumn> rightCols = right.getKeyColumns();
		if (rightCols.size() < leftCols.size()) return false;
		for(int i = 0; i < leftCols.size(); i++) {
			PEKeyColumn lpk = leftCols.get(0);
			PEKeyColumn rpk = rightCols.get(0);
			if (!lpk.getColumn().equals(rpk.getColumn())) 
				return false;
		}
		return true;
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Key.class;
	}

	@Override
	protected int getID(Key p) {
		return p.getId();
	}

	@Override
	protected Key lookup(SchemaContext sc) throws PEException {
		UserTable ut = table.get(sc).persistTree(sc);
		for(Key k : ut.getKeys()) {
			// first test on constraint type
			if (getConstraint() == k.getConstraint() && getType() == k.getType()) {
				// check names
				if (getConstraint() == ConstraintType.FOREIGN) {
					if (getSymbol() != null && getSymbol().getUnquotedName().get().equals(k.getSymbol()))
						return k;
				} else { 
					if (getName().getUnquotedName().get().equals(k.getName()))
						return k;
				}
			}
		}
		return null;
	}

	@Override
	protected String getDiffTag() {
		return "PEKey";
	}	

	@Override
	public void setComment(Comment c) {
		comment = c;
	}

	@Override
	public Comment getComment() {
		return comment;
	}
	
	public abstract boolean isForwardKey();
	
}
