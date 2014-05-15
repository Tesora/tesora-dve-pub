// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.io.Serializable;
import java.util.Collection;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class CacheInvalidationRecord implements Serializable {

	public static final CacheInvalidationRecord GLOBAL = new CacheInvalidationRecord(InvalidationScope.GLOBAL);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	protected final ListOfPairs<SchemaCacheKey<?>, InvalidationScope> toInvalidate;
	protected final InvalidationScope globalToken;
	
	public CacheInvalidationRecord(SchemaCacheKey<?> effected, InvalidationScope action) {
		if (effected == null) {
			toInvalidate = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>(0);
		} else {
			toInvalidate = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>(1);
			toInvalidate.add(effected, action);
		}
		globalToken = null;
	}

	public CacheInvalidationRecord(ListOfPairs<SchemaCacheKey<?>, InvalidationScope> keys) {
		toInvalidate = new ListOfPairs<SchemaCacheKey<?>, InvalidationScope>(keys);
		globalToken = null;
	}
	
	public CacheInvalidationRecord(final Collection<Pair<SchemaCacheKey<?>, InvalidationScope>> keys) {
		toInvalidate = new ListOfPairs<SchemaCacheKey<?>, InvalidationScope>(keys.size());
		toInvalidate.addAll(keys);
		globalToken = null;
	}

	// this is basically just a token invalidation record - means flush the entire cache
	public CacheInvalidationRecord(InvalidationScope scope) {
		globalToken = scope;
		toInvalidate = null;
	}
	
	public ListOfPairs<SchemaCacheKey<?>, InvalidationScope> getInvalidateActions() {
		return toInvalidate;
	}
	
	public InvalidationScope getGlobalToken() {
		return globalToken;
	}
	
	public void addInvalidateAction(SchemaCacheKey<?> effected, InvalidationScope action) {
		if (effected != null) {
			toInvalidate.add(effected, action);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("CacheInvalidationRecord{");
		if (globalToken != null)
			buf.append("global:").append(globalToken);
		else 
			buf.append(Functional.join(toInvalidate, ", ", new UnaryFunction<String,Pair<SchemaCacheKey<?>,InvalidationScope>>() {

				@Override
				public String evaluate(Pair<SchemaCacheKey<?>, InvalidationScope> object) {
					return object.getFirst() + ":" + object.getSecond();
				}
				
			}));
		buf.append("}");
		return buf.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((globalToken == null) ? 0 : globalToken.hashCode());
		result = prime * result + ((toInvalidate == null) ? 0 : toInvalidate.hashCode());
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
		CacheInvalidationRecord other = (CacheInvalidationRecord) obj;
		if (globalToken != other.globalToken)
			return false;
		if (toInvalidate == null) {
			if (other.toInvalidate != null)
				return false;
		} else if (!toInvalidate.equals(other.toInvalidate))
			return false;
		return true;
	}
	
	
}
