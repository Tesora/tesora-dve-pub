package com.tesora.dve.sql.schema.cache;

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

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.RebuiltPlan;

public interface RegularCachedPlan extends CachedPlan {

	public RebuiltPlan rebuildPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;
	
	public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;	
	
	// bypass the usual literals checking
	public RebuiltPlan showPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException;
	// get the literal types - used in show plan
	public List<ExtractedLiteral.Type> getLiteralTypes();
}
