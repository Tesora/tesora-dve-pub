package com.tesora.dve.common.catalog;

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

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroupGeneration;


public class StorageGroupAccessor {

	public static int getId(PersistentGroup sg) {
		return sg.id;
	}
	
	public static StorageGroupGeneration getLastGen(PersistentGroup sg) {
		return sg.getLastGen();
	}

	public static boolean areEquivalent(PersistentGroup sg1,
			PersistentGroup sg2) {
		boolean areEquiv = (sg1.generations.size() == sg2.generations.size());
		for (int i = 0; areEquiv && i < sg1.generations.size(); ++i) {
			StorageGroupGeneration gen1 = sg1.generations.get(i);
			StorageGroupGeneration gen2 = sg2.generations.get(i);
			areEquiv = (gen1.locked == gen2.locked
					&& gen1.version == gen2.version
					&& gen1.groupMembers.size() == gen2.groupMembers.size());
			for (int j = 0; areEquiv && j < gen1.groupMembers.size(); ++j) {
				areEquiv = (gen1.groupMembers.get(j).id == gen2.groupMembers.get(j).id);
			}
		}
		return areEquiv;
	}
	
	public static void addGeneration(PersistentGroup sg) {
		sg.lockGroup();
		sg.generations.add(new StorageGroupGeneration(sg, sg.generations.size()));
	}

}
