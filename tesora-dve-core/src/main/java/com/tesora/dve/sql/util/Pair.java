// OS_STATUS: public
package com.tesora.dve.sql.util;

public class Pair<A, B> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	private A first;
	private B second;
	
	public Pair(A f, B s) {
		first = f;
		second = s;
	}
	
	public A getFirst() { return first; }
	public B getSecond() { return second; }

	public Pair<B, A> getReverse() {
		return new Pair<B, A>(getSecond(), getFirst());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair<A, B> other = (Pair<A, B>) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "Pair(" + first + ", " + second + ")";
	}
	
}
