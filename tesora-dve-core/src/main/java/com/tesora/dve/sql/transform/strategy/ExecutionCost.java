package com.tesora.dve.sql.transform.strategy;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.transform.constraints.PlanningConstraint;

// predicted execution cost
// we are interested in:
// - how many sites
// - how much data
// - specific row counts
// - cardinality info
//
// we have several levels of what we can know about the cost, in order from best to worst.
// [1] an estimated row count - this would be an upper bound. 
// [2] existence of a where clause
// [3] single or multisite
// we always have to keep track of all of it
public class ExecutionCost implements Comparable<ExecutionCost> {

	// may not be that bad if only on one site
	protected boolean singleSite;
	// the constraint that rules - for partition queries this is the limiting constraint
	// once joined, this is the expectation (i.e. not limiting)
	protected PlanningConstraint constraint;
	// even if we didn't find a constraint, we might still have a where clause
	protected boolean whereClause;

	// roughly, the group score
	protected int score;
	
	// row estimate
	protected long rowCount;
	
	public ExecutionCost(boolean wc, boolean oneSite, PlanningConstraint c, long rowEstimate) {
		singleSite = oneSite;
		whereClause = wc;
		if (c == null && !wc)
			score = 100;
		else if (singleSite)
			score = 1;
		else
			score = 10;
		constraint = c;
		rowCount = rowEstimate;
	}

	public ExecutionCost(boolean wc, boolean oneSite, ExecutionCost basis) {
		this(wc,oneSite,basis.getConstraint(),basis.getRowCount());
	}
	
	public ExecutionCost(PlanningConstraint c, long rowEstimate, ExecutionCost...costs) {
		singleSite = true;
		score = -1;
		for(ExecutionCost ec : costs) {
			if (ec.hasWhereClause()) whereClause = true;
			if (!ec.singleSite) singleSite = false;
			if (ec.score > score) score = ec.score;
		}
		constraint = c;
		rowCount = rowEstimate;
	}
	
	public ExecutionCost(ExecutionCost basis, long rowEstimate) {
		whereClause = basis.whereClause;
		singleSite = basis.singleSite;
		score = basis.score;
		constraint = basis.constraint;
		rowCount = rowEstimate;
	}
		
	@Override
	public String toString() {
		return "Cost(" + score + "): singleSite=" + singleSite + ",  constraint=" + constraint + ", wc=" + whereClause;
	}
	
	@Override
	public int compareTo(ExecutionCost o) {
		// a row count beats everything - it definitively says that the upper bound on number of rows
		// will be the number sites * rowCount.
		if (rowCount > -1 && o.rowCount > -1) {
			if (rowCount < o.rowCount)
				return -1;
			else if (rowCount > o.rowCount)
				return 1;
			return 0;
		}
		if (constraint != null && o.constraint != null)
			return constraint.compareTo(o.constraint);
		else if (constraint == null && o.constraint != null)
			return 1;
		else if (constraint != null && o.constraint == null)
			return -1;
		else if (score < o.score)
			return -1;
		else if (score > o.score)
			return 1;
		else if (whereClause && !o.whereClause)
			return -1;
		else if (!whereClause && o.whereClause)
			return 1;
		else
			return 0;
	}
	
	public PlanningConstraint getConstraint() {
		return constraint;
	}
	
	public boolean isSingleSite() {
		return singleSite;
	}
	
	public void setSingleSite() {
		singleSite = true;
	}
	
	public boolean hasWhereClause() {
		return whereClause;
	}
	
	public int getGroupScore() {
		return score;
	}
		
	public long getRowCount() {
		return rowCount;
	}
	
	public static ExecutionCost minimize(ExecutionCost[] in) {
		Arrays.sort(in);
		return in[0];
	}
	
	public static ExecutionCost minimize(List<ExecutionCost> in) {
		Collections.sort(in);
		return in.get(0);
	}
	
	public static ExecutionCost maximize(List<ExecutionCost> in) {
		Collections.sort(in);
		return in.get(in.size() - 1);
	}
}
