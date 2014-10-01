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
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.Emitter.EmitterInvoker;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class LateSortedInsert {

	final InsertIntoValuesStatement stmt;
	final ListOfPairs<List<ExpressionNode>,DistributionKey> parts;
	
	public LateSortedInsert(final InsertIntoValuesStatement stmt, final ListOfPairs<List<ExpressionNode>, DistributionKey> parts) {
		this.stmt = stmt;
		this.parts = parts;
	}
	
	public List<JustInTimeInsert> resolve(SchemaContext sc) throws PEException {
		// sort by persistent site
		DistributionVector dv = this.stmt.getTable().getDistributionVector(sc);
		LinkedHashMap<MappingSolution, DistributionKey> repKeys = new LinkedHashMap<MappingSolution, DistributionKey>();
		MultiMap<MappingSolution, List<ExpressionNode>> bySite = new MultiMap<MappingSolution, List<ExpressionNode>>();
		for (Pair<List<ExpressionNode>, DistributionKey> p : this.parts) {
			DistributionKey dk = p.getSecond();
			MappingSolution ms = 
					sc.getCatalog().mapKey(sc, dk.getDetachedKey(sc), dk.getModel(sc), this.stmt.getKeyOpType(), this.stmt.getSingleGroup(sc));

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
			emitJITInsert(sc, out, dk, asList);
		}

		return out;
	}
	
	private void emitJITInsert(final SchemaContext sc, final List<JustInTimeInsert> out, final DistributionKey dk, final List<List<ExpressionNode>> asList)
			throws PEException {
		final GenericSQLCommand prefix = new EmitterInvoker() {
			@Override
			protected void emitStatement(final SchemaContext sc, final StringBuilder buf) {
				getEmitter().emitInsertPrefix(sc, LateSortedInsert.this.stmt, buf);
			}
		}.buildGenericCommand(sc);

		final GenericSQLCommand suffix = new EmitterInvoker() {
			@Override
			protected void emitStatement(final SchemaContext sc, final StringBuilder buf) {
				getEmitter().emitInsertSuffix(sc, LateSortedInsert.this.stmt, buf);
			}
		}.buildGenericCommand(sc);
		
		final EmitterInvoker valueEmitter = new EmitterInvoker() {
			@Override
			protected void emitStatement(final SchemaContext sc, final StringBuilder buf) {
				getEmitter().emitInsertValues(sc, asList, buf);
			}
		};

		// we must always use generic sql - whether we have parameters or not,
		// so that we can handle special characters correctly.
		valueEmitter.getEmitter().setOptions(EmitOptions.GENERIC_SQL);
		valueEmitter.getEmitter().startGenericCommand();
		valueEmitter.getEmitter().pushContext(sc.getTokens());
		try {
			final GenericSQLCommand valuesClause = valueEmitter.buildGenericCommand(sc).resolve(sc, null);

			final SQLCommand sqlc = reconstructSQLCommand(sc, prefix, valuesClause, suffix);

			out.add(new JustInTimeInsert(sqlc, asList.size(), dk));
		} finally {
			valueEmitter.getEmitter().popContext();
		}
	}

	private SQLCommand reconstructSQLCommand(final SchemaContext sc, final GenericSQLCommand prefix, final GenericSQLCommand valuesClause,
			final GenericSQLCommand suffix) {
		final GenericSQLCommand gsql = prefix.append(valuesClause).append(suffix);
		if (sc.getValueManager().hasPassDownParams()) {
			if ((sc.getOptions() != null && sc.getOptions().isPrepare())) {
				return new SQLCommand(gsql);
			}

			return new SQLCommand(gsql, gsql.getFinalParams(sc));
		}

		return new SQLCommand(gsql);
	}
}
