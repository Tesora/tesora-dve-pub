// OS_STATUS: public
package com.tesora.dve.sql.infoschema.logical;

import java.util.HashMap;
import java.util.Iterator;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.DelegatingInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
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
// select ud.name as schema_name
//        ut.name as table_name,
//        uc.name as column_name,
//        uc.hash_position as vector_position
//        dm.name as model_type
//        coalesce(dr.name,c.name) as model_name
// from user_table ut
// inner join user_database ud on ut.user_database_id = ud.user_database_id
// inner join distribution_model dm on ut.distribution_model_id = dm.id
// left outer join user_column uc on uc.user_table_id = ut.table_id and uc.hash_position > 0
// left outer join range_table_relation rtr on ut.table_id = rtr.table_id
// left outer join distribution_range dr on rtr.range_id = dr.range_id
public class DistributionLogicalTable extends LogicalInformationSchemaTable {

	private final CatalogInformationSchemaTable tableTable;
	private final CatalogInformationSchemaTable databaseTable;
	private final CatalogInformationSchemaTable columnTable;
	private final CatalogInformationSchemaTable modelTable;
	private final CatalogInformationSchemaTable rangeTable;
	private final CatalogInformationSchemaTable rangeTabRelTable;
	
	private final DelegatingInformationSchemaColumn columnName;
	private final DelegatingInformationSchemaColumn dvPosition;
	private final DelegatingInformationSchemaColumn tableName;
	private final DelegatingInformationSchemaColumn databaseName;
	private final DelegatingInformationSchemaColumn modelName;
	private final DelegatingInformationSchemaColumn rangeName;
	
	public DistributionLogicalTable(LogicalInformationSchema logical) {
		super(new UnqualifiedName("distributions"));
		tableTable = (CatalogInformationSchemaTable) logical.lookup("table");
		databaseTable = (CatalogInformationSchemaTable) logical.lookup("database");
		columnTable = (CatalogInformationSchemaTable) logical.lookup("user_column");
		modelTable = (CatalogInformationSchemaTable) logical.lookup("distribution_model");
		rangeTable = (CatalogInformationSchemaTable) logical.lookup("distribution_range");
		rangeTabRelTable = (CatalogInformationSchemaTable) logical.lookup("range_table_relation");

		databaseName = new DelegatingInformationSchemaColumn(databaseTable.lookup("name"),new UnqualifiedName("database_name"));
		tableName = new DelegatingInformationSchemaColumn(tableTable.lookup("name"),new UnqualifiedName("table_name"));
		columnName = new DelegatingInformationSchemaColumn(columnTable.lookup("name"),new UnqualifiedName("column_name"));
		dvPosition = new DelegatingInformationSchemaColumn(columnTable.lookup("hash_position"), new UnqualifiedName("dvposition"));
		modelName = new DelegatingInformationSchemaColumn(modelTable.lookup("name"),new UnqualifiedName("model_name"));
		rangeName = new DelegatingInformationSchemaColumn(rangeTable.lookup("name"),new UnqualifiedName("range_name"));
		addColumn(null,databaseName);
		addColumn(null,tableName);
		addColumn(null,columnName);
		addColumn(null,dvPosition);
		addColumn(null,modelName);
		addColumn(null,rangeName);
		
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
		TableInstance udTable = new TableInstance(databaseTable,null,aliases.buildNewAlias(new UnqualifiedName("ud")),sc.getNextTable(),false);
		TableInstance utTable = new TableInstance(tableTable,null,aliases.buildNewAlias(new UnqualifiedName("ut")),sc.getNextTable(),false);
		TableInstance ucTable = new TableInstance(columnTable,null,aliases.buildNewAlias(new UnqualifiedName("uc")),sc.getNextTable(),false);
		TableInstance dmTable = new TableInstance(modelTable,null,aliases.buildNewAlias(new UnqualifiedName("dm")),sc.getNextTable(),false);
		TableInstance rtrTable = new TableInstance(rangeTabRelTable,null,aliases.buildNewAlias(new UnqualifiedName("rtr")),sc.getNextTable(),false);
		TableInstance rTable = new TableInstance(rangeTable,null,aliases.buildNewAlias(new UnqualifiedName("rt")),sc.getNextTable(),false);
		HashMap<LogicalInformationSchemaTable, TableInstance> forwarding = new HashMap<LogicalInformationSchemaTable, TableInstance>();
		forwarding.put(databaseTable,udTable);
		forwarding.put(tableTable,utTable);
		forwarding.put(columnTable,ucTable);
		forwarding.put(modelTable,dmTable);
		forwarding.put(rangeTabRelTable, rtrTable);
		forwarding.put(rangeTable,rTable);

		FromTableReference ftr = new FromTableReference(utTable);
		ftr.addJoinedTable(new JoinedTable(udTable,
				makeEquijoin(utTable,tableTable.lookup("database"),udTable,databaseTable.lookup("id")),
				JoinSpecification.INNER_JOIN));
		ftr.addJoinedTable(new JoinedTable(dmTable,
				makeEquijoin(utTable,tableTable.lookup("model"),dmTable,modelTable.lookup("id")),
				JoinSpecification.INNER_JOIN));
		ftr.addJoinedTable(new JoinedTable(rtrTable,
				makeEquijoin(utTable,tableTable.lookup("id"),rtrTable,rangeTabRelTable.lookup("user_table")),
				JoinSpecification.LEFT_OUTER_JOIN));
		ftr.addJoinedTable(new JoinedTable(rTable,
				makeEquijoin(rtrTable,rangeTabRelTable.lookup("distribution_range"),rTable,rangeTable.lookup("id")),
				JoinSpecification.LEFT_OUTER_JOIN));
		ftr.addJoinedTable(new JoinedTable(ucTable,
				new FunctionCall(FunctionName.makeAnd(),
						makeEquijoin(utTable,tableTable.lookup("id"),ucTable,columnTable.lookup("user_table")),
						new FunctionCall(FunctionName.makeNotEquals(),new ColumnInstance(columnTable.lookup("hash_position"),ucTable),LiteralExpression.makeAutoIncrLiteral(0))),
						JoinSpecification.LEFT_OUTER_JOIN));
		
		in.getTablesEdge().clear();
		in.getTablesEdge().add(ftr);
		
		ColumnReplacementTraversal.replace(in, forwarding);
		for(Iterator<TableKey> iter = in.getDerivedInfo().getLocalTableKeys().iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (tk.getTable() == this)
				iter.remove();
		}
		
		return new LogicalQuery(lq,in);
	}
	
}
