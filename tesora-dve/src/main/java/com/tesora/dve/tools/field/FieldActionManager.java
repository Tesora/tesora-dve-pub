// OS_STATUS: public
package com.tesora.dve.tools.field;

import java.util.ArrayList;
import java.util.List;

public class FieldActionManager {

	private static final List<FieldAction> knownActions = new ArrayList<FieldAction>();

	static {
		// knownActions.add(new <something>Action());
	};
	
	public static List<FieldAction> getAllActions() {
		return knownActions;
	}
	
	public static FieldAction getAction(String name) {
		for(FieldAction action : knownActions) {
			if(name.equalsIgnoreCase(action.getName()));
				return action;
		}
		return null;
	}
}
