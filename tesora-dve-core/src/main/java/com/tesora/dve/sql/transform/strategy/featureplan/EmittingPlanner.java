package com.tesora.dve.sql.transform.strategy.featureplan;

public interface EmittingPlanner {

	public boolean emitting();
	
	public void emit(String what);

}
