// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

public class ApplyOption {
	
	protected int step;
	protected int maxSteps;
	
	public ApplyOption(int step, int maxSteps) {
		this.step = step;
		this.maxSteps = maxSteps;
	}
	
	public int getMaxSteps() {
		return maxSteps;
	}
	
	public int getCurrentStep() {
		return step;
	}
	
	@Override
	public String toString() {
		return "ApplyOption(" + step + "," + maxSteps + ")";
	}
}