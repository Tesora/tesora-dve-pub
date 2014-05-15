// OS_STATUS: public
package com.tesora.dve.distribution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class KeyTemplate extends ArrayList<UserColumn>{

	private static final long serialVersionUID = 1L;
	
	// see the patent for definition of comparable
	public boolean comparableDistributionVector(KeyTemplate other) {
		if (size() != other.size())
			return false;
		Iterator<UserColumn> liter = iterator();
		Iterator<UserColumn> riter = other.iterator();
		while(liter.hasNext() && riter.hasNext()) {
			UserColumn l = liter.next();
			UserColumn r = riter.next();
			if (!l.comparableType(r))
				return false;
		}
		return true;
	}

	public String getKeyTemplateString() {
		return Functional.apply(this, new UnaryFunction<String, UserColumn>() {
			@Override
			public String evaluate(UserColumn object) {
				return object.getNativeTypeName();
			}
		}).toString();
	}

	public List<String> asColumnList() {
		return Functional.apply(this, new UnaryFunction<String, UserColumn>() {
			@Override
			public String evaluate(UserColumn object) {
				return object.getName();
			}
		});
	}
}
