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
import java.util.Collection;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public class PETableTriggerEventInfo {

	private PETrigger[] triggers;
	
	public PETableTriggerEventInfo() {
		triggers = new PETrigger[2];
	}
	
	public void set(PETrigger trig) {
		triggers[trig.getTime().ordinal()] = trig;
	}
	
	public void remove(PETrigger trig) {
		triggers[trig.getTime().ordinal()] = null;
	}
	
	public PETrigger getBefore() {
		return triggers[TriggerTime.BEFORE.ordinal()];
	}

	public PETrigger getAfter() {
		return triggers[TriggerTime.AFTER.ordinal()];
	}

	public Collection<PETrigger> get() {
		ArrayList<PETrigger> out = new ArrayList<PETrigger>();
		for(PETrigger t : triggers) {
			if (t == null) continue;
			out.add(t);
		}
		return out;
	}
	
	public Collection<ColumnKey> getTriggerBodyColumns(SchemaContext sc) throws PEException {
		throw new PEException("Invalid trigger planning");
	}
	
	public FeatureStep getBeforeStep(SchemaContext sc) throws PEException {
		throw new PEException("No before step present in DDL trigger info");
	}
	
	public FeatureStep getAfterStep(SchemaContext sc) throws PEException {
		throw new PEException("No before step present in DDL trigger info");
	}
}
