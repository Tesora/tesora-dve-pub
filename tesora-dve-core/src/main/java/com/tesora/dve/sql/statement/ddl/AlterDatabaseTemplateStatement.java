// OS_STATUS: public
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.statement.ddl.alter.ChangeTableDistributionAction;
import com.tesora.dve.sql.template.TemplateManager;
import com.tesora.dve.sql.template.jaxb.ModelType;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.util.Pair;

/**
 * Update the template and "template mode" records on the target database.
 * If a template is specified (non-optional) also alter distribution models of
 * all database tables to match the updated template (perform redistribution if
 * necessary).
 * Distribution of any tables not specified in the provided template is not
 * affected.
 * Changes to the "template model" alone only affect creation of new tables.
 */
public class AlterDatabaseTemplateStatement extends PEAlterStatement<PEDatabase> {

	private final PEDatabase target;
	private final Name templateName;
	private final TemplateMode templateMode;

	public AlterDatabaseTemplateStatement(final PEDatabase target, final Pair<Name, TemplateMode> templateDeclaration) {
		super(target, true);
		this.target = target;
		this.templateName = templateDeclaration.getFirst();
		this.templateMode = templateDeclaration.getSecond();
	}

	@Override
	protected PEDatabase modify(SchemaContext pc, PEDatabase backing) throws PEException {
		backing.setTemplateName(this.templateName);
		backing.setTemplateMode(this.templateMode);
		return backing;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(this.target.getCacheKey(), InvalidationScope.CASCADE);
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		final ExecutionStep alterDatabaseTemplate = buildStep(sc);
		es.append(alterDatabaseTemplate);

		/* Alter distribution models of individual tables. */
		if (this.templateName != null) {
			final Set<PEAlterStatement<PETable>> alterTableStatements = buildAlterTableDistributionStmts(sc);
			for (final PEAlterStatement<PETable> stmt : alterTableStatements) {
				stmt.plan(sc, es);
			}
		}
	}

	private Set<PEAlterStatement<PETable>> buildAlterTableDistributionStmts(final SchemaContext sc) {
		assert (this.templateName != null);

		final PETemplate updatedTemplate = TemplateManager.findTemplate(sc, this.templateName.get());
		final List<TableTemplateType> templateTableItems = updatedTemplate.getTemplate().getTabletemplate();
		
		final Set<PEAlterStatement<PETable>> alterStatements = new HashSet<PEAlterStatement<PETable>>(templateTableItems.size());
		for (final TableTemplateType item : templateTableItems) {
			final PEAlterStatement<PETable> stmt = getAlterDistributionStatementFor(sc, item);
			if (stmt != null) {
				alterStatements.add(stmt);
			}
		}

		return alterStatements;
	}

	private PEAlterStatement<PETable> getAlterDistributionStatementFor(final SchemaContext sc, final TableTemplateType item) {
		final String tableName = item.getMatch();
		final TableInstance ti = this.target.getSchema().buildInstance(sc, new UnqualifiedName(tableName), null, true);
		if (ti != null) {
			final PETable table = ti.getAbstractTable().asTable();
			final List<String> dvColumnNames = item.getColumn();
			final List<PEColumn> dvColumns = lookupTableColumnsByName(sc, table, dvColumnNames);

			final ChangeTableDistributionAction alterAction = getUpdateDistributionActionFor(sc, dvColumns, item.getModel(), item.getRange());

			return alterAction.requiresSingleStatement(sc, ti.getTableKey());
		}
		
		return null;
	}
	
	private List<PEColumn> lookupTableColumnsByName(final SchemaContext sc, final PETable table, final List<String> columnNames) {
		if (!columnNames.isEmpty()) {
			final List<PEColumn> columns = new ArrayList<PEColumn>(columnNames.size());
			for (final String name : columnNames) {
				columns.add(table.lookup(sc, name));
			}

			return columns;
		}

		return null;
	}

	private ChangeTableDistributionAction getUpdateDistributionActionFor(final SchemaContext sc, final List<PEColumn> dvColumns,
			final ModelType model, final String rangeName) {
		final Model distributionModel = DistributionVector.Model.getModelFromPersistent(model.value());
		final UnqualifiedName range = (rangeName != null) ? new UnqualifiedName(rangeName) : null;
		final DistributionVector dv = DistributionVector.buildDistributionVector(sc, distributionModel, dvColumns, range);

		return new ChangeTableDistributionAction(dv);
	}

}
