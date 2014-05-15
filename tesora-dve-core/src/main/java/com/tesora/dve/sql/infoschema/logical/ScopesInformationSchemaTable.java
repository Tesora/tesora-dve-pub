// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;

// this table is backed by the query:
// 
// select t.name as tenant_name, s.name as scope_name, 
//        ud.name as table_schema, ut.name as table_name, ut.state as table_state
// from user_table ut inner join user_database on ut.user_database_id = ud.user_database_id
// left outer join scope s on s.scope_table_id = ut.table_id
// left outer join tenant t on s.scope_tenant_id = t.id
// where ud.multitenant_mode = 'adaptive';
public class ScopesInformationSchemaTable extends LogicalInformationSchemaTable {

	private final CatalogInformationSchemaTable scopeTable;
	private final CatalogInformationSchemaTable tableTable;
	private final CatalogInformationSchemaTable tenantTable;
	private final CatalogInformationSchemaTable databaseTable;
	
	private final DelegatingInformationSchemaColumn tableName;
	private final DelegatingInformationSchemaColumn databaseName;
	private final DelegatingInformationSchemaColumn tableStateName;
	private final DelegatingInformationSchemaColumn tenantName;
	private final DelegatingInformationSchemaColumn scopeName;
	
	public ScopesInformationSchemaTable(LogicalInformationSchema logical) {
		super(new UnqualifiedName("scopes"));

		scopeTable = (CatalogInformationSchemaTable) logical.lookup("table_visibility");
		tableTable = (CatalogInformationSchemaTable) logical.lookup("table");
		tenantTable = (CatalogInformationSchemaTable) logical.lookup("tenant");
		databaseTable = (CatalogInformationSchemaTable) logical.lookup("database");
		
		tableName = new DelegatingInformationSchemaColumn(tableTable.lookup("name"), new UnqualifiedName("table_name"));
		databaseName = new DelegatingInformationSchemaColumn(databaseTable.lookup("name"), new UnqualifiedName("table_schema"));
		tableStateName = new DelegatingInformationSchemaColumn(tableTable.lookup("state"), new UnqualifiedName("table_state"));
		tenantName = new DelegatingInformationSchemaColumn(tenantTable.lookup("name"), new UnqualifiedName("tenant_name"));
		scopeName = new DelegatingInformationSchemaColumn(scopeTable.lookup("local_name"), new UnqualifiedName("scope_name"));

		addColumn(null, tableName);
		addColumn(null, databaseName);
		addColumn(null, tableStateName);
		addColumn(null, tenantName);
		addColumn(null, scopeName);
	}

	@Override
	public boolean requiresRawExecution() {
		return true;
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}

	@Override
	public LogicalQuery explode(SchemaContext sc, LogicalQuery lq) {
		// find the table reference
		SelectStatement in = lq.getQuery();
		FromTableReference anchor = null;
		for(Iterator<FromTableReference> iter = in.getTablesEdge().iterator(); iter.hasNext();) {
			FromTableReference ftr = iter.next();
			if (ftr.getBaseTable() != null) {
				if (ftr.getBaseTable().getTable() == this) {
					if (!ftr.getTableJoins().isEmpty())
						throw new InformationSchemaException("Use of table joins not supported on table " + getName());
					anchor = ftr;
					iter.remove();
					break;
				}
			}
		}
		if (anchor == null)
			throw new InformationSchemaException("Unable to find table " + getName() + " within info schema query");
		AliasInformation aliases = in.getAliases();
		
		TableInstance scopeInstance = new TableInstance(scopeTable,null,aliases.buildNewAlias(new UnqualifiedName("s")),
				sc.getNextTable(),false);
		TableInstance tableInstance = new TableInstance(tableTable,null,aliases.buildNewAlias(new UnqualifiedName("ut")),
				sc.getNextTable(),false);
		TableInstance databaseInstance = new TableInstance(databaseTable,null,aliases.buildNewAlias(new UnqualifiedName("ud")),
				sc.getNextTable(),false);
		TableInstance tenantInstance = new TableInstance(tenantTable,null,aliases.buildNewAlias(new UnqualifiedName("t")),
				sc.getNextTable(),false);
		
		HashMap<LogicalInformationSchemaTable,TableInstance> forwarding = new HashMap<LogicalInformationSchemaTable,TableInstance>();
		forwarding.put(scopeTable,scopeInstance);
		forwarding.put(tableTable, tableInstance);
		forwarding.put(databaseTable, databaseInstance);
		forwarding.put(tenantTable, tenantInstance);
		
		FromTableReference ftr = new FromTableReference(tableInstance);
		ftr.addJoinedTable(new JoinedTable(databaseInstance,
				makeEquijoin(tableInstance,tableTable.lookup("database"),databaseInstance,databaseTable.lookup("id")),
				JoinSpecification.INNER_JOIN));
		ftr.addJoinedTable(new JoinedTable(scopeInstance,
				makeEquijoin(tableInstance,tableTable.lookup("id"),scopeInstance,scopeTable.lookup("user_table")),
				JoinSpecification.LEFT_OUTER_JOIN));
		ftr.addJoinedTable(new JoinedTable(tenantInstance,
				makeEquijoin(tenantInstance,tenantTable.lookup("id"),scopeInstance,scopeTable.lookup("tenant")),
				JoinSpecification.LEFT_OUTER_JOIN));
		in.getTablesEdge().clear();
		in.getTablesEdge().add(ftr);
	
		// add on the filter on database type
		List<ExpressionNode> decompAnd = null;
		if (in.getWhereClause() == null)
			decompAnd = new ArrayList<ExpressionNode>();
		else
			decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(databaseTable.lookup("multitenant"),databaseInstance),
				LiteralExpression.makeStringLiteral(MultitenantMode.ADAPTIVE.getPersistentValue())));
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		
		ColumnReplacementTraversal.replace(in,forwarding);
		for(Iterator<TableKey> iter = in.getDerivedInfo().getLocalTableKeys().iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (tk.getTable() == this)
				iter.remove();
		}
		
		return new LogicalQuery(lq,in);
	}
	
}
