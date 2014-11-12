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

import com.tesora.dve.common.catalog.IDynamicPolicy;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.worker.DynamicGroup;

public class PEDynamicGroup implements PEStorageGroup {

	private DynamicGroup group;
	private GroupScale scale;

	public PEDynamicGroup(GroupScale scale) {
		this.scale = scale;
		this.group = null;
	}

	public String getLoadedBy() {
		return "PEDynamicGroup";
	}
	
	public PEDynamicGroup(SchemaContext schemaContext) {
		this.scale = null;
		this.group = null;
	}

	// used in plan caching
	public PEDynamicGroup copy() {
		return new PEDynamicGroup(scale);
	}
	
	@Override
	public StorageGroup getPersistent(SchemaContext sc, ConnectionValues cv) {
		if (group == null) try {
			if (scale == null)
				throw new SchemaException(Pass.PLANNER, "Scale not set on dynamic group prior to creation");
			
			IDynamicPolicy policy = sc.getGroupPolicy();
			
			if(policy == null)
				throw new SchemaException(Pass.PLANNER, "Policy not defined for dynamic group");
			
			group = new DynamicGroup(policy, scale);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to build dynamic group", pe);
		}
		return group;
	}

	@Override
	public PersistentGroup persistTree(SchemaContext sc) throws PEException {
		throw new PEException("persistTree method not supported by dynamic group");
	}

	@Override
	public StorageGroup getScheduledGroup(SchemaContext sc, ConnectionValues cv) {
		return getPersistent(sc,cv);
	}
	
	@Override
	public PEPersistentGroup anySite(SchemaContext sc) throws PEException {
		throw new PEException("anySite() method not supported by dynamic group");
	}

	@Override
	public boolean comparable(SchemaContext sc, PEStorageGroup other) {
		if (!(other instanceof PEDynamicGroup)) return false;
		PEDynamicGroup pedg = (PEDynamicGroup) other;
		return scale == GroupScale.AGGREGATE && pedg.scale == GroupScale.AGGREGATE;
	}

	@Override
	public boolean isSubsetOf(SchemaContext sc, PEStorageGroup other) {
		if (other == this) return true;
		return false;
	}
	
	@Override
	public boolean isSingleSiteGroup() {
		return scale == GroupScale.AGGREGATE;
	}

	@Override
	public void setCost(int score) throws PEException {
		scale = GroupScale.SMALL;
		if (score > 1)
			scale = GroupScale.MEDIUM;
		if (score > 10)
			scale = GroupScale.LARGE;
		group.setScale(scale);		
	}

	@Override
	public boolean isTempGroup() {
		return true;
	}

	public GroupScale getScale() {
		return scale;
	}

	@Override
	public String toString() {
		return "PEDynamicGroup/" + scale;
	}

	@Override
	public PEStorageGroup getPEStorageGroup(SchemaContext sc, ConnectionValues cv) {
		return this;
	}
	
}
