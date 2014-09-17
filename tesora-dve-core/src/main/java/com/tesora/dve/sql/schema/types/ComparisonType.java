package com.tesora.dve.sql.schema.types;

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
import java.util.List;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.persist.CatalogColumnEntity;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.Functional;

public class ComparisonType extends TextType {

	String comparisonClass;
	
	public ComparisonType(NativeType nt, short flags, int size,
			UnqualifiedName charset, UnqualifiedName collation, String comparisonClass) {
		super(nt, flags, size, charset, collation);
		this.comparisonClass = comparisonClass;
	}
	
	@Override
	public String getComparison() {
		return comparisonClass;
	}
	
	@Override
	public void addColumnTypeModifiers(UserColumn uc) {
		super.addColumnTypeModifiers(uc);
		// we put the comparison tag in the es_universe (not the right place, but whatevs)
		String existing = uc.getESUniverse();
		String comp = COMPARISON_TAG + " " + comparisonClass; 
		uc.setESUniverse((existing == null ? comp : existing + " " + comp));
	}
	
	@Override
	public void addColumnTypeModifiers(CatalogColumnEntity cce) throws PEException {
		super.addColumnTypeModifiers(cce);
		String ntmod = cce.getNativeTypeModifiers();
		List<String> entries = new ArrayList<String>();
		if (ntmod != null)
			entries.add(ntmod);
		entries.add(COMPARISON_TAG + " " + comparisonClass);
		if (!entries.isEmpty())
			cce.setNativeTypeModifiers(Functional.join(entries, " "));
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((comparisonClass == null) ? 0 : comparisonClass.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ComparisonType other = (ComparisonType) obj;
		if (comparisonClass == null) {
			if (other.comparisonClass != null)
				return false;
		} else if (!comparisonClass.equals(other.comparisonClass))
			return false;
		return true;
	}

	
}
