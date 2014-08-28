package com.tesora.dve.sql.infoschema.engine;

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
import java.util.Map;

import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class LogicalCatalogQuery extends LogicalQuery {
	
	protected Map<TableKey,ExpressionNode> viewToLogicalForwarding;
	protected List<List<AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn>>> projectionColumns;
	
	public LogicalCatalogQuery(ViewQuery basedOn, SelectStatement s, Map<String,Object> p, Map<TableKey, ExpressionNode> forwarding,
			List<List<AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn>>> projCols) {
		super(basedOn,s,p);
		this.viewToLogicalForwarding = forwarding;
		orig = basedOn;
		projectionColumns = projCols;
	}
	
	public LogicalCatalogQuery(LogicalCatalogQuery base, SelectStatement ss) {
		super(null,ss,base.params);
		viewToLogicalForwarding = base.viewToLogicalForwarding;
		orig = base.orig;
		projectionColumns = base.projectionColumns;
	}
	
	public Map<TableKey,ExpressionNode> getForwarding() { return viewToLogicalForwarding; }
	public List<List<AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn>>> getProjectionColumns() { return projectionColumns; }
	
	public ColumnSet buildProjectionMetadata(SchemaContext sc, ProjectionInfo pi,List<Object> examples) {
		return buildProjectionMetadata(sc,projectionColumns,pi,examples);
	}

	public static ColumnSet buildProjectionMetadata(SchemaContext sc, List<List<AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn>>> projectionColumns,
			ProjectionInfo pi, List<Object> examples) {
		ColumnSet cs = new ColumnSet();
		try {
			for(int i = 0; i < projectionColumns.size(); i++) {
				List<AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn>> p = projectionColumns.get(i);
				AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn> typeColumn = p.get(p.size() - 1);
				AbstractInformationSchemaColumnView<LogicalInformationSchemaColumn> nameColumn = p.get(0);
				Type type = typeColumn.getType();
				if (type == null) {
					Object help = null;
					if (i < examples.size())
						help = examples.get(i);
					if (help == null)
						help = "help";
					ColumnInfo ci = pi.getColumnInfo(i+1);
					buildNativeType(cs,ci.getName(),ci.getAlias(),help);					
				} else {
                    NativeType nt = Singletons.require(HostService.class).getDBNative().getTypeCatalog().findType(type.getDataType(), true);
                	ColumnMetadata cmd = new ColumnMetadata();
                    if (nameColumn.getTable() != null && nameColumn.getTable().getView() == InfoView.INFORMATION) {
                    	cmd.setDbName(nameColumn.getTable().getDatabase(sc).getName().getUnquotedName().get());
                    	cmd.setTableName(nameColumn.getTable().getName().getUnquotedName().get());
                    }
                    if (pi != null)
                    	cmd.setAliasName(pi.getColumnAlias(i+1));
                    cmd.setName(nameColumn.getName().getSQL());
                    cmd.setSize(type.getSize());
                    cmd.setNativeTypeName(nt.getTypeName());
                    cmd.setDataType(type.getDataType());
                    cs.addColumn(cmd);
				}
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to build metadata for catalog result set",pe);
		}
		if (LogicalSchemaQueryEngine.emit) {
			StringBuffer buf = new StringBuffer();
			for(int i = 1; i <= cs.size(); i++) {
				if (i > 1)
					buf.append(", ");
				// buf.append(cs.getColumn(i).getName());
				buf.append(cs.getColumn(i));
			}
			System.out.println("column set: " + buf.toString());
		}
		return cs;
		
	}

	@Override
	public boolean isDirect() {
		return false;
	}
	
}