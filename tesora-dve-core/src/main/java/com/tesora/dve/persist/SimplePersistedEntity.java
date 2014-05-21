package com.tesora.dve.persist;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class SimplePersistedEntity implements PersistedEntity {

	// backing table
	SimpleTableMetadata table;
	// we store the values in a hashmap
	Map<String,Object> values;
	// entities we require to be inserted before us
	List<PersistedEntity> requirements;
	
	public SimplePersistedEntity(SimpleTableMetadata tab) {
		table = tab;
		values = new HashMap<String,Object>();
		requirements = new ArrayList<PersistedEntity>();
	}
	
	public void preValue(String columnName, Object value) throws PEException {
		SimpleColumnMetadata scm = table.getColumn(columnName);
		if (scm.isGenerated())
			throw new PEException("Unable to set pre insert value for column " + columnName + " - generated");
		values.put(columnName, value);
	}
	
	public void postValue(String columnName, Object value) throws PEException {
		SimpleColumnMetadata scm = table.getColumn(columnName);
		if (!scm.isGenerated())
			throw new PEException("Unable to set post insert value for column " + columnName + " - not generated");
		values.put(columnName,value);
	}
	
	public Object getValue(String name) {
		return values.get(name);
	}
	
	public SimpleTableMetadata getTable() {
		return table;
	}
	
	@Override
	public boolean hasGeneratedId() {
		return table.getGeneratedId() != null;
	}

	@Override
	public List<PersistedEntity> getRequires() {
		return requirements;
	}

	public void addRequires(SimplePersistedEntity spe) {
		requirements.add(spe);
	}
	
	@Override
	public void onInsert(long genid) throws PEException {
		for(SimpleColumnMetadata scm : table.getColumnOrder()) {
			if (scm.isGenerated()) {
				postValue(scm.getName(),new Long(genid));
			}
		}
	}
	
	@Override
	public PersistedInsert getInsertStatement() throws PEException {
		PersistedInsert pi = new PersistedInsert(table);
		for(SimpleColumnMetadata scm : table.getColumnOrder()) {
			if (scm.getDependsOn() != null) {
				SimpleTableMetadata dependentTable = scm.getDependsOn().getTable();
				SimplePersistedEntity found = null;
				for(PersistedEntity pe : requirements) {
					SimplePersistedEntity spe = (SimplePersistedEntity) pe;
					if (spe.getTable() == dependentTable) {
						if (found != null)
							throw new PEException("Multiple subject tables found");
						else
							found = spe;
					}
				}
				if (found == null)
					throw new PEException("Entity for table " + table.getName() + " requires a " + dependentTable.getName() + " entity, but none found");
				Object value = found.getValue(scm.getDependsOn().getName());
				preValue(scm.getName(),value);
			}
			if (values.containsKey(scm.getName())) {
				pi.add(scm, values.get(scm.getName()));
			}
		}
		return pi;
	}

	@Override
	public long getGeneratedID() throws PEException {
		SimpleColumnMetadata genid = table.getGeneratedId();
		if (genid == null)
			throw new PEException("No generated id on table " + table.getName());
		Object value = values.get(genid.getName());
		if (value == null)
			throw new PEException("No generated id value available on table " + table.getName());
		if (value instanceof Number) {
			Number n = (Number) value;
			return n.longValue();
		}
		throw new PEException("Invalid id type: " + value.getClass().getSimpleName());
	}

	@Override
	public String toString() {
		return table.getName() + "{" + Functional.join(values.entrySet(), ", ", new UnaryFunction<String,Map.Entry<String,Object>>() {

			@Override
			public String evaluate(Entry<String, Object> object) {
				return object.getKey() + "=" + object.getValue();
			}
			
		}) + "}";
	}
	
}
