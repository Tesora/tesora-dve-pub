package com.tesora.dve.sql.statement.session;

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
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEContainerTenant;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.WorkerGroup;

public class UseContainerStatement extends SessionStatement {

	private List<Pair<PEColumn,LiteralExpression>> values;
	private PEContainerTenant tenant;
	private PEContainer container;
	
	// ctor for global
	public UseContainerStatement(SchemaContext sc, boolean nullContainer) {
		this(sc, null, null,null,null, nullContainer);
	}
	
	public UseContainerStatement(SchemaContext sc, PEContainer container, PETable baseTable, List<PEColumn> discCols, List<Pair<LiteralExpression,Name>> keyvals, boolean nullContainer) {
		super();
		if (keyvals == null) {
			values = null;
			if (nullContainer)
				tenant = null;
			else
				tenant = new PEContainerTenant(sc,null,null);
		} else {
			this.container = container;
			values = normalizeKey(sc, baseTable,discCols,keyvals);
			String lookup = PEContainerTenant.buildDiscriminantValue(sc, values);
			tenant = sc.findContainerTenant(container,lookup);
			if (tenant == null)
				throw new SchemaException(Pass.SECOND, "No such container for discriminant " + lookup);
		}
	}

	private List<Pair<PEColumn,LiteralExpression>> normalizeKey(SchemaContext pc, PETable bt, List<PEColumn> discCols, List<Pair<LiteralExpression,Name>> in) {
		HashMap<PEColumn,LiteralExpression> vals = new HashMap<PEColumn,LiteralExpression>();
		for(int i = 0; i < in.size(); i++) {
			Pair<LiteralExpression,Name> p = in.get(i);
			PEColumn dc = null;
			if (p.getSecond() != null) {
				dc = bt.lookup(pc, p.getSecond());
				if (dc == null)
					throw new SchemaException(Pass.SECOND, "No such column in table " + bt.getName(pc).getSQL());
			} else {
				dc = discCols.get(i);
			}
			LiteralExpression already = vals.get(dc);
			if (already != null)
				throw new SchemaException(Pass.SECOND, "Duplicate discriminant column value: " + dc.getName().getSQL());
			vals.put(dc,p.getFirst());
		}
		List<Pair<PEColumn,LiteralExpression>> out = new ArrayList<Pair<PEColumn,LiteralExpression>>();
		for(PEColumn pec : discCols) {
			out.add(new Pair<PEColumn,LiteralExpression>(pec,vals.get(pec)));
		}
		return out;
	}
	
	public boolean isGlobal() {
		return tenant != null && tenant.isGlobalTenant();
	}

	public boolean isNull() {
		return tenant == null;
	}
	
	public PEContainer getContainer() {
		return container;
	}
	
	public List<Pair<PEColumn,LiteralExpression>> getDiscriminant() {
		return values;
	}
	
	public PEContainerTenant getTenant() {
		return tenant;
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		es.append(new TransientSessionExecutionStep(getSQL(pc),new AdhocOperation() {
			@Override
			public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer) throws Throwable {
				ssCon.setCurrentTenant(tenant);
			}
		}));
	}
	
	
}
