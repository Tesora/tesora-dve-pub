package com.tesora.dve.sql.infoschema.show;

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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.*;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;

// for show create database
public class CreateDatabaseInformationSchemaTable extends
		ShowInformationSchemaTable {

	private ShowInformationSchemaTable tableTable = null;
	private Boolean ifNotExists = null;

	public CreateDatabaseInformationSchemaTable() {
		super(null, new UnqualifiedName("create database"),
				new UnqualifiedName("create database"), false, false);
	}

	@Override
	protected void validate(AbstractInformationSchema ofView) {
		tableTable = (ShowInformationSchemaTable) ofView.lookup("database");
		if (tableTable == null)
			throw new InformationSchemaException(
					"Cannot find show database view in show view");
	}

	/**
	 * @param sc
	 * @param likeExpr
	 * @param scoping
	 * @return
	 */
	public ViewQuery buildLikeSelect(SchemaContext sc, String likeExpr,
			List<Name> scoping) {
		throw new InformationSchemaException(
				"Illegal operation: show create database does not support the like clause");
	}

	/**
	 * @param sc
	 * @param wc
	 * @param scoping
	 * @return
	 */
	public ViewQuery buildWhereSelect(SchemaContext sc, ExpressionNode wc,
			List<Name> scoping) {
		throw new InformationSchemaException(
				"Illegal operation: show create database does not support the where clause");
	}

	@Override
	public IntermediateResultSet executeUniqueSelect(SchemaContext sc,
			Name onName) {
		// we're going to delegate the initial get to the database table
        boolean FORCE_VIEW_EXTENSIONS = true;
		ViewQuery basicQuery = tableTable.buildUniqueSelect(sc, onName, FORCE_VIEW_EXTENSIONS);
		IntermediateResultSet irs = LogicalSchemaQueryEngine.buildResultSet(sc,
				basicQuery, basicQuery.getQuery().getProjectionMetadata(sc));
		if (irs.isEmpty()) {
			// database doesn't exist so send back an error
			throw new SchemaException(Pass.SECOND, "Unknown database '"
					+ onName.get() + "'");
		}

        //look through the row for the default character set column, we'll need it to form the 'Create Database' returned string
        Integer indexForDefaultCharset = null;

        ColumnSet rowMeta = irs.getMetadata();
        for (int i=1;i <= rowMeta.size();i++ ){
            ColumnMetadata meta = rowMeta.getColumn(i);
            if (meta != null && ShowSchema.Database.DEFAULT_CHARACTER_SET.equals( meta.getName() ) ){
                indexForDefaultCharset = i - 1;
                break;
            }
        }

        if (indexForDefaultCharset == null)
            throw new SchemaException(Pass.SECOND,"Internal error, couldn't find default character set for database " + onName.get());

        ResultRow returnedRow = irs.getRows().get(0);
        ResultColumn defaultCharset = returnedRow.getRow().get(indexForDefaultCharset);

		// database exists, and we have the charset, so send back required information
		ColumnSet cs = new ColumnSet();
		try {
            cs.addColumn("Database", 255, Singletons.require(HostService.class).getDBNative().getTypeCatalog()
					.findType(java.sql.Types.VARCHAR, true).getTypeName(),
					java.sql.Types.VARCHAR);
            cs.addColumn("Create Database", 255, Singletons.require(HostService.class).getDBNative()
					.getTypeCatalog().findType(java.sql.Types.VARCHAR, true)
					.getTypeName(), java.sql.Types.VARCHAR);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER,
					"Unable to find varchar type?", pe);
		}

		ResultRow rr = new ResultRow();
		rr.addResultColumn(onName.get());
		// NOTE: This is mysql specific syntax for the if not exists! Find a
		// better way if needed

		String ifNotExistsStr = BooleanUtils.isTrue(ifNotExists) ? " /*!32312 IF NOT EXISTS*/" : StringUtils.EMPTY;
        rr.addResultColumn(String.format("CREATE DATABASE%s `%s` /*!40100 DEFAULT CHARACTER SET %s */",ifNotExistsStr,onName.get(),defaultCharset.getColumnValue()));
		return new IntermediateResultSet(cs, rr);

	}

	/**
	 * @param c
	 * @param udb
	 * @param rdm
	 * @param dbn
	 * @return
	 */
	public UserTable persist(CatalogDAO c, UserDatabase udb,
			RandomDistributionModel rdm, DBNative dbn) {
		return null;
	}

	public Statement buildUniqueStatement(SchemaContext pc, Name objectName, Boolean ine) {
		this.ifNotExists = ine;
		return super.buildUniqueStatement(pc, objectName);
	}
}
