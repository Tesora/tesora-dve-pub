// OS_STATUS: public
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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.exceptions.PEException;

// given a collection of objects with dependencies, run an insert
public class InsertEngine {

	private List<PersistedEntity> entities;
	
	private PersistProvider provider;
	
	public InsertEngine(List<PersistedEntity> ents, PersistProvider prov) {
		entities = ents;
		provider = prov;
	}
	
	public void populate() throws PEException {
		final PersistProvider impl = provider;
		// todo: figure out how to batch the inserts
		walkInDependencyOrder(new DepAction() {

			@Override
			public void onFinished(PersistedEntity pe) throws PEException {
				PersistedInsert pi = pe.getInsertStatement();
				Long value = impl.insert(pi);
				if (value != null)
					pe.onInsert(value);
			}
			
		});
	}
	
	// this is what is used to build the gold files
	public List<String> dryrun() throws PEException {
		final ArrayList<String> out = new ArrayList<String>();
		final AtomicLong idgen = new AtomicLong(0);
		walkInDependencyOrder(new DepAction() {

			@Override
			public void onFinished(PersistedEntity pe) throws PEException {
				if (pe.hasGeneratedId())
					pe.onInsert(idgen.incrementAndGet());
				out.add(pe.getInsertStatement().getSQL());

			}
			
		});
		return out;
	}
	
	// the values in the map are the entities which the key depends on
	private Map<PersistedEntity,List<PersistedEntity>> buildDependencies() {
		// I would use a multimap here, but the multimap removes keys when values are toasted - so not so much
		LinkedHashMap<PersistedEntity,List<PersistedEntity>> out =
				new LinkedHashMap<PersistedEntity,List<PersistedEntity>>();
		for(PersistedEntity pe : entities) 
			out.put(pe, new ArrayList<PersistedEntity>(pe.getRequires()));
		return out;
	}
	
	private void walkInDependencyOrder(DepAction da) throws PEException {
		Map<PersistedEntity,List<PersistedEntity>> deps = buildDependencies();
		while(!deps.isEmpty()) {
			List<PersistedEntity> keySet = new ArrayList<PersistedEntity>(deps.keySet());
			List<PersistedEntity> finished = new ArrayList<PersistedEntity>();
			for(PersistedEntity pe : keySet) {
				Collection<PersistedEntity> sub = deps.get(pe);
				if (sub == null || sub.isEmpty()) {
					finished.add(pe);
					deps.remove(pe);
					da.onFinished(pe);
				}
			}
			keySet = new ArrayList<PersistedEntity>(deps.keySet());
			for(PersistedEntity pe : finished) {
				// maintain the dependencies
				for(PersistedEntity ipe : keySet) {
					deps.get(ipe).remove(pe);
				}
			}
		}
	}
	
	interface DepAction {
		
		public void onFinished(PersistedEntity pe) throws PEException;
		
	};	
}
