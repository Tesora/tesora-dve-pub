package com.tesora.dve.queryplan;

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

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepShowCatalogEntityOperation extends QueryStepOperation {
	static Logger logger = Logger.getLogger( QueryStepShowCatalogEntityOperation.class );

	List<CatalogEntity> catalogEntity;
	CatalogQueryOptions queryOpts;
	
	public QueryStepShowCatalogEntityOperation( List<CatalogEntity> ce, CatalogQueryOptions opts ) {
		this.catalogEntity = ce;
		this.queryOpts = opts;
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
		if ( catalogEntity.size() > 0 ) {
			ResultChunk rc = new ResultChunk();
			for ( CatalogEntity ce : catalogEntity ) {
				ResultRow rr = ce.getShowResultRow(queryOpts);
				if ( rr == null ) {
					throw new PEException("getShowResultRow not implemented for type " + ce.getClass());
				}
				rc.addResultRow(rr);
			}	
			ColumnSet cs = catalogEntity.get(0).getShowColumnSet(queryOpts);
			if ( cs == null ) {
				throw new PEException("getShowColumnSet not implemented for type " + catalogEntity.get(0).getClass());
			}
			resultConsumer.inject(cs, rc.getRowList());
		}
	}

	
	@Override
	public boolean requiresTransaction() {
		return false;
	}

	@Override
	public boolean requiresWorkers() {
		// could be executed before the database is set
		return false;
	}

}
