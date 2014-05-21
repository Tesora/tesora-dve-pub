package com.tesora.dve.tools.aitemplatebuilder;

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

import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public interface TemplateRangeItem extends TemplateItem {

	public abstract boolean contains(final TableStats table);

	public abstract Set<TableColumn> getRangeColumnsFor(final TableStats table);

	public Set<Type> getUniqueColumnTypes() throws PEException;

	/**
	 * Columns are compared based on their unqualified name and type only.
	 */
	public boolean hasCommonColumn(final Set<TableColumn> columns);
}
