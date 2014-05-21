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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.template.TemplateManager;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.util.Functional;

public class PECreateDatabaseStatement extends PECreateStatement<PEDatabase, UserDatabase> {

	public PECreateDatabaseStatement(
			Persistable<PEDatabase, UserDatabase> targ, boolean peOnly,
			Boolean ine, String specTag, boolean exists) {
		super(targ, peOnly, ine, specTag, exists);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		if (alreadyExists) {
			es.append(new EmptyExecutionStep(0,"already exists - " + getSQL(pc)));
		} else {
			PEDatabase peds = (PEDatabase) getCreated();
			if (peds.getTemplateName() != null) {
				List<Statement> prereqs = TemplateManager.adaptPrereqs(pc,peds);
				for(Statement s : prereqs) {
					s.plan(pc,es);
				}
			}
			es.append(buildStep(pc));
			if (peds.getMTMode().isMT()) {
				// this is all we support right now
				peds.setFKMode(FKMode.STRICT);
				// create a new landlord tenant
				PETenant landlord = new PETenant(pc,peds,AdaptiveMultitenantSchemaPolicyContext.LANDLORD_TENANT,"landlord tenant");
				// the landlord tenant will reference the database, so just serialize that out
				pc.beginSaveContext();
				try {
					landlord.persistTree(pc);
					es.append(new SimpleDDLExecutionStep(getDatabase(pc), getStorageGroup(pc), landlord, getAction(), SQLCommand.EMPTY,
							Collections.EMPTY_LIST, Functional.toList(pc.getSaveContext().getObjects()),null));
				} finally {
					pc.endSaveContext();
				}
			}
		}
	}

	public List<Statement> getPrereqs(SchemaContext pc) throws PEException {
		PEDatabase peds = (PEDatabase) getCreated();
		if (peds.getTemplateName() != null) {
			return TemplateManager.adaptPrereqs(pc,peds);
		}
		return null;
	}
	
	@Override
	public StatementType getStatementType() {
		return StatementType.CREATE_DB;
	}
		

}
