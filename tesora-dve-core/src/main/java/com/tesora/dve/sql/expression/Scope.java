// OS_STATUS: public
package com.tesora.dve.sql.expression;



import java.util.List;
import java.util.Set;

import com.tesora.dve.sql.node.expression.Alias;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.NameInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SubqueryTable;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.ListSet;

public interface Scope {

	public ScopeParsePhase getPhase();
	public void setPhase(ScopeParsePhase spp);
	
	public List<Scope> getNested();
	
	public ListSet<ProjectingStatement> getNestedQueries();
	
	public ListSet<VariableInstance> getVariables();
	
	
	public TableInstance buildTableInstance(Name inTableName, UnqualifiedName alias, Schema<?> inSchema, SchemaContext sc, LockInfo info);
	
	public void pushVirtualTable(SubqueryTable sqt, UnqualifiedName alias, SchemaContext sc);
	
	public void insertTable(TableInstance ti);

	public ExpressionNode buildColumnInstance(SchemaContext sc, Name given);

	public TableInstance lookupTableInstance(Name given, boolean required);
	
	public void resolveProjection(SchemaContext sc);

	// this also moves the phase to RESOLVING_CURRENT
	public void storeProjection(List<ExpressionNode> projection);
	
	public void insertColumn(UnqualifiedName alias, ExpressionAlias e);
	
	public ExpressionNode buildExpressionAlias(ExpressionNode e, Alias alias, SourceLocation sloc);
		
	public Set<String> getAllAliases();

	public ListSet<TableKey> getLocalTables();

	public ListSet<TableKey> getAllVisibleTables();
	
	public ListSet<FunctionCall> getFunctions();
	
	public void registerFunction(FunctionCall fc);
	
	public PEColumn registerColumn(PEColumn c);
	
	public void registerAlterColumns(SchemaContext sc, PETable tab);
	
	public PEColumn lookupInProcessColumn(Name n);
	
	public Table<?> getAlteredTable();	
	
	public ListSet<NameInstance> getUnresolvedChildren();
}
