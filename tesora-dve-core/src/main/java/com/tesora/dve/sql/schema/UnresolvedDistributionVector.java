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
import java.util.List;

import com.tesora.dve.sql.parser.TranslatorUtils;

// parsing only class, introduced with create table as support
// since the dist vect may be declared prior to the columns
public class UnresolvedDistributionVector {

	protected DistributionVector.Model model;
	protected List<Name> columnNames;
	protected Name rangeOrContainer;
	
	public UnresolvedDistributionVector(DistributionVector.Model model,
			List<Name> columnNames, Name rangeOrContainer) {
		this.model = model;
		this.columnNames = columnNames;
		this.rangeOrContainer = rangeOrContainer;
	}
	
	public DistributionVector resolve(SchemaContext pc, TranslatorUtils utils) {
		final List<PEColumn> columns = new ArrayList<PEColumn>();
		if (columnNames != null) {
			for (final Name n : columnNames) {
				columns.add(utils.lookupInProcessColumn(n, false));
			}
		}

		return DistributionVector.buildDistributionVector(pc, model, columns, rangeOrContainer);
	}
}
