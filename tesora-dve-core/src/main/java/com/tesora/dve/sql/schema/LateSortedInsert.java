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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class LateSortedInsert {

	final ListOfPairs<List<ExpressionNode>,DistributionKey> parts;
	
	final String prefix;
	final String suffix;
	
	final PETable intoTable;
	
	final DistKeyOpType keyOpType;
	
	final PEStorageGroup group;
	
	public LateSortedInsert(PETable intoTab, ListOfPairs<List<ExpressionNode>,DistributionKey> parts, 
			String prefix, String suffix, DistKeyOpType keyOpType,
			PEStorageGroup onGroup) {
		this.prefix = prefix;
		this.parts = parts;
		this.intoTable = intoTab;
		this.suffix = suffix;
		this.keyOpType = keyOpType;
		this.group = onGroup;
	}
	
	public List<JustInTimeInsert> resolve(SchemaContext sc) throws PEException {
		// sort by persistent site
		DistributionVector dv = intoTable.getDistributionVector(sc);
		LinkedHashMap<MappingSolution, DistributionKey> repKeys = new LinkedHashMap<MappingSolution, DistributionKey>();
		MultiMap<MappingSolution, List<ExpressionNode>> bySite = new MultiMap<MappingSolution, List<ExpressionNode>>();
		for(Pair<List<ExpressionNode>, DistributionKey> p : parts) {
			DistributionKey dk = p.getSecond();
			MappingSolution ms = 
					sc.getCatalog().mapKey(sc,dk.getDetachedKey(sc), dk.getModel(sc), keyOpType, group);

			if (MappingSolution.AllWorkers == ms) {
				throw new SchemaException(Pass.PLANNER, "Unable to sort inserts, key for model " + dk.getModel(sc) + " apparently not deterministic");
			}
			if (!repKeys.containsKey(ms))
				repKeys.put(ms, dk);
			bySite.put(ms, p.getFirst());
		}
		List<JustInTimeInsert> out = new ArrayList<JustInTimeInsert>();
		// now, by site, build the actual insert
		for(Map.Entry<MappingSolution, DistributionKey> me : repKeys.entrySet()) {
			MappingSolution onSite = me.getKey();
			DistributionKey dk = me.getValue();
			if (dv.isRandom()) {
				// override the group to be just the group we want
				PersistentSite ss = (PersistentSite) onSite.getSite();
				PEStorageSite pess = PEStorageSite.load(ss, sc);
				dk.setGroupOverride(pess);
			}
			Collection<List<ExpressionNode>> values = bySite.get(onSite);
			List<List<ExpressionNode>> asList = Functional.toList(values);
            Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
			StringBuilder buf = new StringBuilder();
			buf.append(prefix);
			SQLCommand sqlc = null;
			// if we have parameters we have to use the generic sql
			if (sc.getValueManager().hasPassDownParams()) {
				EmitOptions opts = EmitOptions.GENERIC_SQL;
				emitter.setOptions(opts);
				emitter.startGenericCommand();
				try {
					emitter.pushContext(sc.getTokens());
					emitter.emitInsertValues(sc, asList, buf);		
				} finally {
					emitter.popContext();
				}
				if (suffix != null)
					buf.append(suffix);

				GenericSQLCommand gsql = emitter.buildGenericCommand(buf.toString());
				if ((sc.getOptions() != null && sc.getOptions().isPrepare()))
					sqlc = new SQLCommand(gsql.resolve(sc,null));
				else {
					sqlc = new SQLCommand(gsql.resolve(sc,null),gsql.getFinalParams(sc));
				}
			} else {
				EmitOptions opts = EmitOptions.NONE.addForceParamValues();
				emitter.setOptions(opts);
				emitter.emitInsertValues(sc, asList, buf);
				if (suffix != null)
					buf.append(suffix);
				sqlc = new SQLCommand(buf.toString());
			}
			out.add(new JustInTimeInsert(sqlc,asList.size(),dk));
		}
		return out;
	}	
}
