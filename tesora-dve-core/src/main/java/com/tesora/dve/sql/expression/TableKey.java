// OS_STATUS: public
package com.tesora.dve.sql.expression;


import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.sql.node.expression.MTTableInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class TableKey extends RewriteKey {

	protected Table<?> backingTable;
	protected long node;
	protected TableInstance instance = null;
	
	public TableKey(Table<?> backing, long n) {
		super();
		backingTable = backing;
		node = n;
	}
	
	@Override
	public TableInstance toInstance() {
		if (instance == null)
			instance = new TableInstance(backingTable, null, null, node,true);
		return instance;
	}
	
	public TableKey(TableInstance ti) {
		super();
		backingTable = ti.getTable();
		node = ti.getNode();
		instance = ti;
	}
	
	public static TableKey make(TableInstance ti) {
		if (ti instanceof MTTableInstance)
			return new MTTableKey((MTTableInstance)ti);
		return new TableKey(ti);
	}
	
	public long getNode() {
		return node;
	}
	
	// view support
	public void setNode(long l) {
		hc = null;
		node = l;
	}
	
	public Table<?> getTable() {
		return backingTable;
	}
	
	public PEAbstractTable<?> getAbstractTable() {
		return (PEAbstractTable<?>)backingTable;
	}
	
	@Override
	protected int computeHashCode() {
		final int prime = 31;
		int result = 0;
		result = prime * result
				+ ((backingTable == null) ? 0 : backingTable.hashCode());
		result = prime * result + (int) (node ^ (node >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableKey other = (TableKey) obj;
		if (backingTable == null) {
			if (other.backingTable != null)
				return false;
		} else if (!backingTable.equals(other.backingTable))
			return false;
		if (node != other.node)
			return false;
		return true;
	}

	@Override
	public String toString() {
		String tn = null;
		if (backingTable instanceof PEAbstractTable) {
			PEAbstractTable<?> pet = (PEAbstractTable<?>) backingTable;
			if (pet.isTempTable())
				tn = pet.getName(SchemaContext.threadContext.get()).get();
			else
				tn = pet.getName().get();
		} else {
			tn = backingTable.getName().get();
		}
		String un = ((instance == null  || instance.getAlias() == null) ? "" : "(" + instance.getAlias().getUnquotedName().get() + ")");
		return "TableKey{" + tn + un + ":" + node + "}";
	}
	
	public String describe(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		buf.append("TableKey{").append(backingTable.getName(sc).get());
		if (instance != null && instance.getAlias() != null) 
			buf.append("(").append(instance.getAlias().get()).append(")");
		buf.append(":").append(node).append("}");
		return buf.toString();
	}
	
	
	public void reload(SchemaContext sc) {
		if (backingTable instanceof PEAbstractTable) {
			backingTable = (Table<?>) sc.getSource().find(sc, ((PEAbstractTable<?>)backingTable).getCacheKey());
			clearHashCode();
		}
	}
	
	public Integer getAutoIncTrackerID() {
		return getAbstractTable().asTable().getAutoIncrTrackerID();
	}
	
	public SchemaCacheKey<?> getCacheKey() {
		if (backingTable instanceof PEAbstractTable)
			return ((PEAbstractTable<?>)backingTable).getCacheKey();
		return null;
	}	
	
	public void setFrozen() {
		if (instance != null)
			instance.setParent(null);
	}

	public LockSpecification buildLock(String reason) {
		SchemaCacheKey<?> sck = getCacheKey();
		if (sck == null) return null;
		return sck.getLockSpecification(reason);
	}
	
}
