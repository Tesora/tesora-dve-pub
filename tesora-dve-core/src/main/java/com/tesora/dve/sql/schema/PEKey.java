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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ScopeStack;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.validate.SimpleValidateResult;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.transform.constraints.KeyConstraint;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.transform.constraints.PlanningConstraintType;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

public class PEKey extends Persistable<PEKey, Key> implements TableComponent<PEKey>, MatchableKey, HasComment { 

	protected IndexType type;
	protected SchemaEdge<PETable> table;
	protected Comment comment;
	protected ConstraintType constraint;
	protected UnqualifiedName symbol;
	protected boolean synthetic;
	protected boolean persisted;
	protected boolean hidden;
	protected Long cardRatio = null;
	protected List<PEKeyColumnBase> columns;
	
	public PEKey(Name n, IndexType k, List<PEKeyColumnBase> cols, Comment anyComment) {
		this(n,k,cols,anyComment,false);
	}
	
	public PEKey(Name n, IndexType k, List<PEKeyColumnBase> cols, Comment anyComment, boolean synthetic) {
		this(n,k,cols,null,null,anyComment,synthetic,false);
	}

	protected PEKey(Name n, IndexType k, List<PEKeyColumnBase> cols, ConstraintType cons, UnqualifiedName sym, Comment com, boolean synthetic, boolean hidden) {
		super(null);
		setName(n);
		type = k;
		columns = cols;
		for(PEKeyColumnBase peks : columns)
			peks.setKey(this);
		setConstraint(cons);
		symbol = sym;
		comment = com;
		this.synthetic = synthetic;
		this.hidden = hidden;
		persisted = true; // default
	}
	
	public PETable getTable(SchemaContext sc) {
		return table.get(sc);
	}
	
	public void setTable(SchemaEdge<PETable> tab) {
		table = tab;
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
	

	
	// return true only if the rhs has the same prefix as the lhs - 
	// that is all the columns in lhs are present in rhs in the same order
	// so rhs could have more columns
	public static boolean samePrefix(PEKey left, PEKey right) {
		List<PEKeyColumnBase> leftCols = left.getKeyColumns();
		List<PEKeyColumnBase> rightCols = right.getKeyColumns();
		if (rightCols.size() < leftCols.size()) return false;
		for(int i = 0; i < leftCols.size(); i++) {
			PEKeyColumnBase lpk = leftCols.get(0);
			PEKeyColumnBase rpk = rightCols.get(0);
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
	
	@Override
	public String toString() {
		return "name: " + (getName() == null ? "(null)" : getName().get()) + 
				" type=" + type + " columns={"
			+ Functional.join(columns, ", ", new UnaryFunction<String,PEKeyColumnBase>() {

				@Override
				public String evaluate(PEKeyColumnBase object) {
					return object.getName().get();
				}
				
			}) + "}";
	}

	// override so that we can set flags on column
	public void setConstraint(ConstraintType ct) {
		constraint = ct;
		for(PEKeyColumnBase pekc : columns) {
			if (pekc.isUnresolved()) continue;
			PEColumn c = pekc.getColumn();
			if (ct == ConstraintType.PRIMARY) {
				c.makeNotNullable();
				c.setPrimaryKeyPart();
			} else if (ct == ConstraintType.UNIQUE) {
				c.setUniqueKeyPart();
			}
		}
	}
	
	public boolean isValidFkTarget(final SchemaContext sc) {
		final PETable table = this.getTable(sc);
		if (table.getEngine().isInnoDB()) {
			final List<PEKey> tableKeys = table.getKeys(sc);
			for (final PEKey key : tableKeys) {
				if (!(key instanceof PEForeignKey) && samePrefix(this, key)) {
					return true;
				}
			}
			
			return false;
		}
		
		return this.isUnique();
	}
	

	public static PEKey load(Key k, SchemaContext sc, PETable enclosingTable) {
		PEKey p = null;
		if (enclosingTable != null) {
			Name name;
			if (k.isForeignKey()) {
				name = new UnqualifiedName((k.getSymbol() == null) ? k.getName() : k.getSymbol());
			} else {
				name = new UnqualifiedName(k.getName());
			}
			p = enclosingTable.lookupKey(sc, name);
		}
		if (p == null) {
			if (k.isForeignKey())
				p = new PEForeignKey(sc,k, enclosingTable);
			else
				p = new PEKey(sc, k, enclosingTable);
		}
		return p;
	}
	
	protected PEKey(SchemaContext sc, Key k, PETable enclosingTable) {
		super(null);
		sc.startLoading(this, k);
		if (k.getName() != null)
			setName(new UnqualifiedName(k.getName()));
		type = k.getType();
		constraint = k.getConstraint();
		if (k.getSymbol() != null)
			setSymbol(new UnqualifiedName(k.getSymbol()));
		columns = new ArrayList<PEKeyColumnBase>();
		for(KeyColumn kc : k.getColumns()) {
			PEKeyColumn pekc = PEKeyColumn.load(kc, sc, enclosingTable);
			pekc.setKey(this);
			columns.add(pekc);
		}
		if (k.getComment() != null)
			comment = new Comment(k.getComment());
		synthetic = k.isSynthetic();
		persisted = k.isPersisted();
		hidden = k.isHidden();
		setPersistent(sc,k,k.getId());
		sc.finishedLoading(this, k);
	}
	
	public long getCardinality() {
		long max = -1;
		for(PEKeyColumnBase pekc : getKeyColumns()) {
			if (pekc.getCardinality() > max)
				max = pekc.getCardinality();
		}
		return max;
	}
	
	public long getCardinality(SchemaContext sc, PEColumn pec) {
		for(PEKeyColumnBase pekc : getKeyColumns()) {
			if (pekc.getColumn().equals(pec))
				return pekc.getCardinality();
		}
		return -1;
	}

	public long getCardRatio(SchemaContext sc, PEColumn pec) {
		return computeCardRatio(sc, getCardinality(sc,pec));
	}
	

	@Override
	public long getCardRatio(SchemaContext sc) {
		if (cardRatio == null) 
			cardRatio = computeCardRatio(sc,getCardinality());
		return cardRatio;
	}

	private long computeCardRatio(SchemaContext sc, long myCard) {
		if (isUnique())
			return 1;
		if (myCard > -1) {
			PEKey uk = getTable(sc).getUniqueKey(sc);
			if (uk == null)
				return -1;
			long pk = uk.getCardinality();
			if (pk > -1)
				return Math.round(Math.ceil((pk * 1.0)/myCard));
		}
		return -1;
	}
	
	@Override
	public void take(SchemaContext pc, PEKey targ) {
		throw new SchemaException(Pass.PLANNER, "Invalid alter action: modify a key (" + getName().getSQL() + ")");
	}
	
	
	public List<PEKeyColumnBase> getKeyColumns() {
		return columns;
	}	
	
	public boolean containsColumn(PEColumn c) {
		for(PEKeyColumnBase p : columns)
			if (p.getColumn().equals(c))
				return true;
		return false;
	}
	
	public void removeColumn(PEColumn c) {
		for(Iterator<PEKeyColumnBase> iter = columns.iterator(); iter.hasNext();) {
			PEKeyColumnBase p = iter.next();
			if (p.getColumn().equals(c)) {
				iter.remove();
				break;
			}
		}
	}

	public void addColumn(int position, PEColumn c) {
		PEKeyColumn pekc = new PEKeyColumn(this, c, null,-1);
		columns.add(position, pekc);
	}
	
	public int getPositionOf(SchemaContext sc, PEKeyColumn c) {
		PEKeyColumnBase first = columns.get(0);
		int startsAt = 1;
		if (first.getColumn().isTenantColumn())
			startsAt = 0;
		return columns.indexOf(c) + startsAt;
	}
	
	@Override
	public List<PEColumn> getColumns(SchemaContext sc) {
		return Functional.apply(columns, new UnaryFunction<PEColumn, PEKeyColumnBase>() {

			@Override
			public PEColumn evaluate(PEKeyColumnBase object) {
				return object.getColumn();
			}
			
		});
	}

	
	@Override
	protected Key createEmptyNew(SchemaContext sc) throws PEException {
		UserTable ut = table.get(sc).persistTree(sc);
		Key k = new Key((getName() == null ? null : getName().get()),type,ut,table.get(sc).getOffsetOf(sc, this));
		sc.getSaveContext().add(this, k);
		return k;
	}

	@Override
	protected void populateNew(SchemaContext sc, Key p) throws PEException {
		for(PEKeyColumnBase pekc : columns) {
			p.addColumn(pekc.persistTree(sc));
		}
		p.setConstraint(constraint);
		if (symbol != null)
			p.setSymbol(symbol.getUnquotedName().get());
		if (comment != null)
			p.setComment(comment.getComment());
		if (synthetic)
			p.setSynthetic();
	}

	@Override
	protected void updateExisting(SchemaContext sc, Key p) throws PEException {
		p.setPersisted(persisted);
		p.setHidden(hidden);
		p.setPosition(getTable(sc).getOffsetOf(sc, this));
		HashMap<String, KeyColumn> persCols = new HashMap<String, KeyColumn>();
		HashMap<String, PEKeyColumnBase> transCols = new HashMap<String,  PEKeyColumnBase>();
		for(PEKeyColumnBase pekc : getKeyColumns())
			transCols.put(pekc.getColumn().getName().getCapitalized().get(), pekc);
		for(KeyColumn kc : p.getColumns()) {
			String name = kc.getSourceColumn().getName().toUpperCase().trim();
			PEKeyColumnBase was = transCols.remove(name);
			boolean same = (was != null);
			if (same) {
				sc.clearLoaded(was.getCacheKey());
				PEKeyColumn pekc = PEKeyColumn.load(kc, sc, null);
				pekc.setKey(this);
				String anydiffs = was.differs(sc, pekc, true);
				if (anydiffs != null) {
					same = false;
					transCols.put(name,was);
				}
			}
			if (!same)
				persCols.put(name, kc);
		}
		
		for(KeyColumn kc : persCols.values()) {
			p.removeColumn(kc);
		}
		
		sc.beginSaveContext();
		try {
			for(PEKeyColumnBase pekc : transCols.values()) {
				p.addColumn(pekc.persistTree(sc));
			}
		} finally {
			sc.endSaveContext();
		}
		
	}

	
	@Override
	protected Persistable<PEKey, Key> load(SchemaContext sc, Key p)
			throws PEException {
		return new PEKey(sc,p,null);
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEKey, Key> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEKey other = oth.get();

		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);
		
		if (maybeBuildDiffMessage(sc,messages, "name", getName(), other.getName(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages, "number of columns", getKeyColumns().size(), other.getKeyColumns().size(), first, visited))
			return true;
		Iterator<PEKeyColumnBase> leftIter = getKeyColumns().iterator();
		Iterator<PEKeyColumnBase> rightIter = other.getKeyColumns().iterator();
		while(leftIter.hasNext() && rightIter.hasNext()) {
			PEKeyColumnBase lkc = leftIter.next();
			PEKeyColumnBase rkc = rightIter.next();
			if (lkc.collectDifferences(sc, messages, rkc, first, visited))
				return true;
		}
		return false;
	}

	@Override
	public PlanningConstraint buildEmptyConstraint(SchemaContext sc, TableKey tk, ListOfPairs<PEColumn,ConstantExpression> values) {
		return new KeyConstraint(sc,this,tk, values);
	}
	
	public PEKey copy(SchemaContext sc, PETable containingTable) {
		List<PEKeyColumnBase> contained = new ArrayList<PEKeyColumnBase>();
		for(PEKeyColumnBase e : getKeyColumns()) {
			PEKeyColumnBase r = e.copy(sc, containingTable);
			contained.add(r);
		}
		return new PEKey(getName(), type, contained, constraint, symbol, comment, synthetic, hidden);		
	}

	@Override
	public void checkValid(SchemaContext sc, List<ValidateResult> acc) {
		for(PEKeyColumnBase pekc : columns) {
			if (pekc.getColumn().getType().asKeyRequiresPrefix() && pekc.getLength() == null) {
				if (getTable(sc).getEngine().isInnoDB()) 
					acc.add(new SimpleValidateResult(this,true,
							"Column " + pekc.getColumn().getName() + " requires a prefix when used in a key"));
			}
		}
	}

	public Integer getIndexSize() {
		int acc = 0;
		for(PEKeyColumnBase pekc : getKeyColumns()) {
			Integer v = pekc.getIndexSize();
			if (v == null) return null;
			acc += v;
		}
		return acc;
	}
	public String[] buildHint() {
		ArrayList<String> combo = new ArrayList<String>();
		for(PEKeyColumnBase pekc : getKeyColumns()) {
			PEColumn pec = pekc.getColumn();
			combo.add(pec.getName().getUnquotedName().get());
		}
		return combo.toArray(new String[0]);
	}

	public PEKey resolve(SchemaContext pc, ScopeStack stack) {
		// check for resolution first
		List<PEKeyColumnBase> resolved = new ArrayList<PEKeyColumnBase>();
		boolean any = false;
		for(PEKeyColumnBase pekcb : getKeyColumns()) {
			if (pekcb.isUnresolved()) {
				PEColumn found = stack.lookupInProcessColumn(pekcb.getName());
				if (found == null)
					throw new SchemaException(Pass.SECOND, "Cannot resolve column " + pekcb.getName());
				resolved.add(pekcb.resolve(found));
				any = true;
			} else {
				resolved.add(pekcb);
			}
		}
		if (!any)
			return this;
		return new PEKey(getName(), getType(), resolved, getConstraint(), getSymbol(), getComment(), synthetic, hidden);
	}
	
	public boolean isUnresolved() {
		return Functional.any(columns, new UnaryPredicate<PEKeyColumnBase>() {

			@Override
			public boolean test(PEKeyColumnBase object) {
				return object.isUnresolved();
			}
			
		});
	}
	
	public List<ResultRow> buildRow(final SchemaContext sc) {
		if (isForeign()) return null;
		if (isHidden()) return null;
		if (isSynthetic()) return null;
		String tableName = 
				getTable(sc).getName().getUnqualified().getUnquotedName().get();
		long unique = (!isUnique() ? 0L : 1L);
		String keyName = getName().getUnqualified().getUnquotedName().get();
		ArrayList<ResultRow> out = new ArrayList<ResultRow>();
		int seq = 0;
		for(PEKeyColumnBase pekcb : getKeyColumns()) {
			PEKeyColumn pekc = (PEKeyColumn) pekcb;
			ResultRow rr = new ResultRow();
			out.add(rr);
			rr.addResultColumn(tableName);
			rr.addResultColumn(unique);
			rr.addResultColumn(keyName);
			rr.addResultColumn(++seq);
			rr.addResultColumn(pekc.getColumn().getName().getUnqualified().getUnquotedName().get());
			rr.addResultColumn("A");
			rr.addResultColumn(pekc.getCardinality());
			rr.addResultColumn(pekc.getIndexSize());
			rr.addResultColumn("");
			rr.addResultColumn(pekc.getColumn().isNullable() ? "YES" : "NO");
			rr.addResultColumn(getType().getSQL());
			rr.addResultColumn(getComment() == null ? "" : getComment().getComment());
			rr.addResultColumn("");
		}
		return out;
	}
	
}
