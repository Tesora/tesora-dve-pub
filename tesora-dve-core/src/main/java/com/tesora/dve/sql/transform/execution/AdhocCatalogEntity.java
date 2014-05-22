package com.tesora.dve.sql.transform.execution;

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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.util.Pair;

public class AdhocCatalogEntity implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	private ColumnSet columnSet;
	private ResultRow resultRow;
	
	public AdhocCatalogEntity(String first, Object second)
	{
		this(new Pair<String, Object>(first, second));
	}
	
	public AdhocCatalogEntity(Pair<String, Object> pair)
	{
		this(Collections.singletonList(pair));
	}
	
	public AdhocCatalogEntity(List<Pair<String, Object>> pairs) {
		columnSet = new ColumnSet();
		resultRow = new ResultRow();
		
		for(Pair<String, Object> pair : pairs)
		{
			Object value = pair.getSecond();
			
			if (value instanceof String || value == null) {
				columnSet.addColumn(pair.getFirst(), 255, "varchar", Types.VARCHAR);
			} else if (value instanceof Integer || value instanceof Long) {
				columnSet.addColumn(pair.getFirst(), 16, "integer", Types.INTEGER);
			} else if (value instanceof Timestamp ) {
				columnSet.addColumn(pair.getFirst(), 16, "timestamp", Types.TIMESTAMP);
			} else {
				throw new SchemaException(Pass.PLANNER, "What represents " + value.getClass().getName());
			}
			resultRow.addResultColumn(value, (value == null));	
		}
	}
	
	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return columnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return resultRow;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return null;
	}

	@Override
	public int getId() {
		throw new PECodingException("Cannot get id from AdhocCatalogEntity");
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

}
