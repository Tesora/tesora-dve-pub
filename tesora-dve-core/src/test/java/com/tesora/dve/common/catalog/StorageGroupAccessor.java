// OS_STATUS: public
package com.tesora.dve.common.catalog;

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
