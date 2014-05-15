// OS_STATUS: public
package com.tesora.dve.sql.transform.constraints;

public enum PlanningConstraintType {

	// ordered by suitability
	PRIMARY(1,true),
	UNIQUE(2,true),
	REGULAR(3,false),
	DISTVECT(4,false);
	
	private final int score;
	private final boolean unique;
	private PlanningConstraintType(int s, boolean u) {
		score = s;
		unique = u;
	}
	
	public int getScore() {
		return score;
	}
	
	public boolean isUnique() {
		return unique;
	}
}
