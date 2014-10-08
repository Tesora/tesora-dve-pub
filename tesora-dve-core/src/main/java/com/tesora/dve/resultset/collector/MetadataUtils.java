package com.tesora.dve.resultset.collector;

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

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public class MetadataUtils {

	public static ColumnSet buildParameterSet(ParameterMetaData pmd) throws SQLException {
		ColumnSet cs = new ColumnSet();
		for(int colIdx = 1; colIdx <= pmd.getParameterCount(); colIdx++) {
            ColumnMetadata cm= Singletons.require(HostService.class).getDBNative().getParameterColumnInfo(pmd, colIdx);
			cs.addColumn(cm);
		}
		return cs;
	}
}
