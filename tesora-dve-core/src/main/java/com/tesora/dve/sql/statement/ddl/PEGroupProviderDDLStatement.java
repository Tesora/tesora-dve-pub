package com.tesora.dve.sql.statement.ddl;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.GroupProviderExecutionStep;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.SiteManagerCommand;

public abstract class PEGroupProviderDDLStatement extends DDLStatement {

	PEProvider provider;
	List<Pair<Name,LiteralExpression>> options;
	
	public PEGroupProviderDDLStatement(PEProvider pep, List<Pair<Name,LiteralExpression>> opts) {
		super(true);
		provider = pep;
		options = opts;
	}
	
	public PEProvider getProvider() {
		return provider;
	}

	public Provider getBackingProvider(SchemaContext pc) throws PEException {
		createCatalogObjects(pc);
		return getProvider().getPersistent(pc);
	}
	
	protected ListOfPairs<String, Object> buildOptions(SchemaContext pc) {
		return convertOptions(pc,options);
	}
	
	public static ListOfPairs<String,Object> convertOptions(SchemaContext sc, List<Pair<Name,LiteralExpression>> in) {
		ListOfPairs<String,Object> out = new ListOfPairs<String,Object>();
		for(Pair<Name, LiteralExpression> p : in) {
			out.add(p.getFirst().get(), (p.getSecond().isNullLiteral() ? null : p.getSecond().getValue(sc)));
		}
		return out;
		
	}
	
	public SiteManagerCommand getCommandBlock(SchemaContext pc) throws PEException {
		return new SiteManagerCommand(getCommand(), getBackingProvider(pc), buildOptions(pc));
	}
		
	@Override
	public Persistable<?, ?> getRoot() {
		return provider;
	}

	public List<Pair<Name,LiteralExpression>> getOptions() {
		return options;
	}
	
	public List<CatalogEntity> createCatalogObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			getProvider().persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}

	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return createCatalogObjects(pc);
	}

	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		return new GroupProviderExecutionStep(getProvider(),getCommandBlock(pc),getAction(),getDeleteObjects(pc),getCatalogObjects(pc));
	}

	protected abstract Command getCommand();
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// force a total reload - possibly conservative
		return CacheInvalidationRecord.GLOBAL;
	}


}
