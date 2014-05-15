// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.ArrayList;

public class ListOfPairs<First,Second> extends ArrayList<Pair<First,Second>> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	public ListOfPairs() {
		super();
	}
	
	public ListOfPairs(int s) {
		super(s);
	}
	
	public boolean add(First f, Second s) {
		return add(new Pair<First,Second>(f,s));
	}
	
	public ListOfPairs(ListOfPairs<First,Second> other) {
		super(other);
	}
}
