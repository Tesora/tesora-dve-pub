package com.tesora.dve.sql.transform.strategy.triggers;

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

import java.util.Collection;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETableTriggerPlanningEventInfo;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListSet;

public abstract class TriggerPlanner extends TransformFactory {

	protected PETableTriggerPlanningEventInfo getTriggerInfo(PlannerContext context, TableKey tk, TriggerEvent event) {
		if (tk.getAbstractTable().isView()) return null;
		PETable pet = tk.getAbstractTable().asTable();
		return (PETableTriggerPlanningEventInfo) pet.getTriggers(context.getContext(), event);
	}
	
	protected ListSet<PETable> getTriggeredTables(PlannerContext context, Collection<TableKey> pertinentTables, TriggerEvent event) {
		ListSet<PETable> triggered = new ListSet<PETable>();
		for(TableKey tk : pertinentTables) {
			if (tk.getAbstractTable().isView()) continue;
			PETable pet = tk.getAbstractTable().asTable();
			if (pet.hasTrigger(context.getContext(), event))
				triggered.add(pet);
		}
		return triggered;
	}
	
	protected FeatureStep failSupport() throws PEException {
		throw new PEException("No planning/runtime support for triggers");
	}
	
	protected FeatureStep handleInsertStatement(PlannerContext context, InsertStatement is) throws PEException {
		TableKey targetTab = is.getTableInstance().getTableKey();
		PETableTriggerPlanningEventInfo triggerInfo = getTriggerInfo(context,targetTab,TriggerEvent.INSERT);
		if (triggerInfo == null)
			return null;
		return failSupport();
	}
	
}
