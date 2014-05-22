package com.tesora.dve.sql.node.test;

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


import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.parser.TokenTypes;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.ListSet;

public class EngineConstant {
	
	public static final IdentityNodeTest FUNCTION = new IdentityNodeTest("function",FunctionCall.class,true) {
		@Override
		public boolean has(LanguageNode in, EngineToken tok) {
			if (!has(in)) return false;
			FunctionCall ifc = (FunctionCall) in;
			return ifc.getFunctionName().getTokenID() == tok.getTokenID();
		}
	};
	public static final IdentityNodeTest AGGFUN = new IdentityNodeTest("aggfun",FunctionCall.class,true) {
		@Override
		public boolean has(LanguageNode in) {
			if (!EngineConstant.FUNCTION.has(in)) return false;
			FunctionCall ifc = (FunctionCall)in;
			return ifc.getFunctionName().isAggregate();
		}
	};
	public static final IdentityNodeTest CONSTANT = new IdentityNodeTest("constant",ConstantExpression.class,false);
	public static final IdentityNodeTest COLUMN = new IdentityNodeTest("column",ColumnInstance.class,false);
	public static final IdentityNodeTest TABLE = new IdentityNodeTest("table",TableInstance.class,false);
	public static final IdentityNodeTest ALIAS_INSTANCE = new IdentityNodeTest("aliasInstance",AliasInstance.class,false);
	public static final IdentityNodeTest VARIABLE = new IdentityNodeTest("variable",VariableInstance.class,false);
	public static final IdentityNodeTest PARAMETER = new IdentityNodeTest("parameter",Parameter.class,false);
	
	public static final EdgeTest WHERECLAUSE = new EdgeTest(EdgeName.WHERECLAUSE);
	public static final EdgeTest PROJECTION = new EdgeTest(EdgeName.PROJECTION);
	public static final EdgeTest UPDATECLAUSE = new EdgeTest(EdgeName.UPDATE_EXPRS);
	public static final EdgeTest ORDERBY = new EdgeTest(EdgeName.ORDERBY);
	public static final EdgeTest LIMIT = new EdgeTest(EdgeName.LIMIT);	
	public static final EdgeTest GROUPBY = new EdgeTest(EdgeName.GROUPBY);
	public static final EdgeTest FROMCLAUSE = new EdgeTest(EdgeName.TABLES);
	public static final EdgeTest HAVINGCLAUSE = new EdgeTest(EdgeName.HAVING);
	
	public static final EngineToken AND = new EngineToken("AND",TokenTypes.AND);
	public static final EngineToken OR = new EngineToken("OR",TokenTypes.OR);
	public static final EngineToken EQUALS = new EngineToken("=",TokenTypes.Equals_Operator);
	public static final EngineToken EXISTS = new EngineToken("EXISTS",TokenTypes.EXISTS);
	public static final EngineToken DATABASE = new EngineToken("DATABASE",TokenTypes.DATABASE);
	public static final EngineToken IN = new EngineToken("IN",TokenTypes.IN);
	public static final EngineToken NOTIN = new EngineToken("NOT IN",TokenTypes.NOTIN);
	public static final EngineToken SUM = new EngineToken("SUM",TokenTypes.SUM);
	public static final EngineToken RAND = new EngineToken("RAND", TokenTypes.RAND);
	public static final EngineToken COUNT = new EngineToken("COUNT",TokenTypes.COUNT);
	public static final EngineToken NOT = new EngineToken("NOT",TokenTypes.NOT);
	public static final EngineToken UUID = new EngineToken("UUID",TokenTypes.UUID);
	public static final EngineToken CURRENT_USER = new EngineToken("CURRENT_USER", TokenTypes.CURRENT_USER);
	public static final EngineToken IFNULL = new EngineToken("IFNULL", TokenTypes.IFNULL);
	public static final EngineToken LAST_INSERT_ID = new EngineToken("LAST_INSERT_ID", TokenTypes.LAST_INSERT_ID);
	
	public static final DerivedAttribute<ListSet<FunctionCall>> PROJ_AGGREGATE_FUNCTIONS = new ProjectionAggregateFunctions();
	public static final DerivedAttribute<ListSet<LanguageNode>> PROJ_COLUMNS =
		new CollectingDerivedAttribute(Order.POSTORDER,PROJECTION, COLUMN);
	public static final DerivedAttribute<Boolean> EQUIJOIN = new Equijoin();
	public static final DerivedAttribute<ListSet<FunctionCall>> EQUIJOINS = new Equijoins();
	// this tables computation does not cross subquery boundaries
	public static final DerivedAttribute<ListSet<TableKey>> TABLES = new Tables(Boolean.FALSE);
	// and this one does
	public static final DerivedAttribute<ListSet<TableKey>> TABLES_INC_NESTED = new Tables(Boolean.TRUE);
	public static final DerivedAttribute<CollapsedJoinGraph> PARTITIONS = new Partitions();
	public static final DerivedAttribute<ListSet<PEStorageGroup>> GROUPS = new Groups();
	public static final DerivedAttribute<ListSet<DistributionVector>> DISTRIBUTION_VECTORS = new DistributionVectors();
	public static final DerivedAttribute<DistributionVector> BROADEST_DISTRIBUTION_VECTOR = new BroadestDistributionVector();
	public static final DerivedAttribute<ListSet<TableKey>> DEPENDENT = new Dependent();
	public static final DerivedAttribute<ListSet<ProjectingStatement>> NESTED = new NestedQueries();
	public static final DerivedAttribute<ListSet<VariableInstance>> VARIABLES = new Variables();
	public static final DerivedAttribute<ListSet<FunctionCall>> FUNCTIONS = new Functions(); 
	public static final DerivedAttribute<ListSet<Database<?>>> DBS = new Databases();
	
}
