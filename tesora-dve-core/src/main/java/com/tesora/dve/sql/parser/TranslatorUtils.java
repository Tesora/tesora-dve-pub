package com.tesora.dve.sql.parser;

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

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.antlr.runtime.Lexer;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.charset.NativeCollation;
import com.tesora.dve.charset.NativeCollationCatalog;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentTemplate;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.ValueConverter;
import com.tesora.dve.distribution.DistributionRange;
import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockManager;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.siteprovider.SiteProviderPlugin;
import com.tesora.dve.siteprovider.SiteProviderPlugin.SiteProviderFactory;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.Scope;
import com.tesora.dve.sql.expression.ScopeParsePhase;
import com.tesora.dve.sql.expression.ScopeStack;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.direct.DirectShowStatusInformation;
import com.tesora.dve.sql.infoschema.direct.DirectShowVariablesTable;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.MigrationException;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.Alias;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.CastFunctionCall;
import com.tesora.dve.sql.node.expression.CharFunctionCall;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConvertFunctionCall;
import com.tesora.dve.sql.node.expression.Default;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.ExpressionSet;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.GroupConcatCall;
import com.tesora.dve.sql.node.expression.IdentifierLiteralExpression;
import com.tesora.dve.sql.node.expression.IndexHint;
import com.tesora.dve.sql.node.expression.IndexHint.HintTarget;
import com.tesora.dve.sql.node.expression.IndexHint.HintType;
import com.tesora.dve.sql.node.expression.IntervalExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.NameInstance;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.node.expression.RandFunctionCall;
import com.tesora.dve.sql.node.expression.StringLiteralAlias;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.expression.TriggerTableInstance;
import com.tesora.dve.sql.node.expression.ValueSource;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinClauseType;
import com.tesora.dve.sql.node.structural.JoinClauseType.ClauseType;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.ParserOptions.Option;
import com.tesora.dve.sql.schema.Comment;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.ContainerDistributionVector;
import com.tesora.dve.sql.schema.ContainerPolicyContext;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.ExplainOptions.ExplainOption;
import com.tesora.dve.sql.schema.Capability;
import com.tesora.dve.sql.schema.FloatSizeTypeAttribute;
import com.tesora.dve.sql.schema.ForeignKeyAction;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.GrantScope;
import com.tesora.dve.sql.schema.LoadDataInfileColOption;
import com.tesora.dve.sql.schema.LoadDataInfileLineOption;
import com.tesora.dve.sql.schema.LoadDataInfileModifier;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEForwardForeignKeyColumn;
import com.tesora.dve.sql.schema.PEForwardKeyColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumn;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEPolicy;
import com.tesora.dve.sql.schema.PEPolicyClassConfig;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.PERawPlan;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.PETrigger;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.PEView;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.PolicyClass;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SizeTypeAttribute;
import com.tesora.dve.sql.schema.SubqueryTable;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TableComponent;
import com.tesora.dve.sql.schema.TableResolver;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.UnresolvedDistributionVector;
import com.tesora.dve.sql.schema.UserScope;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.modifiers.AutoincTableModifier;
import com.tesora.dve.sql.schema.modifiers.CharsetTableModifier;
import com.tesora.dve.sql.schema.modifiers.ChecksumModifier;
import com.tesora.dve.sql.schema.modifiers.CollationTableModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifierKind;
import com.tesora.dve.sql.schema.modifiers.CommentTableModifier;
import com.tesora.dve.sql.schema.modifiers.DefaultValueModifier;
import com.tesora.dve.sql.schema.modifiers.EngineTableModifier;
import com.tesora.dve.sql.schema.modifiers.InsertModifier;
import com.tesora.dve.sql.schema.modifiers.MaxRowsModifier;
import com.tesora.dve.sql.schema.modifiers.RowFormatTableModifier;
import com.tesora.dve.sql.schema.modifiers.StringTypeModifier;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.modifiers.UnknownTableModifier;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TenantColumn;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.DBEnumType;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementTraits;
import com.tesora.dve.sql.statement.ddl.AddGlobalVariableStatement;
import com.tesora.dve.sql.statement.ddl.AddStorageSiteStatement;
import com.tesora.dve.sql.statement.ddl.AlterDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.AlterDatabaseTemplateStatement;
import com.tesora.dve.sql.statement.ddl.GrantStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterExternalServiceStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterGroupProviderStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterPersistentSite;
import com.tesora.dve.sql.statement.ddl.PEAlterPolicyStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterRawPlanStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterSiteInstanceStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTableStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTemplateStatement;
import com.tesora.dve.sql.statement.ddl.PECreateDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.PECreateExternalServiceStatement;
import com.tesora.dve.sql.statement.ddl.PECreateGroupProviderStatement;
import com.tesora.dve.sql.statement.ddl.PECreateRawPlanStatement;
import com.tesora.dve.sql.statement.ddl.PECreateSiteInstanceStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStorageSiteStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableAsSelectStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTriggerStatement;
import com.tesora.dve.sql.statement.ddl.PECreateUserStatement;
import com.tesora.dve.sql.statement.ddl.PECreateViewStatement;
import com.tesora.dve.sql.statement.ddl.PEDropContainerStatement;
import com.tesora.dve.sql.statement.ddl.PEDropExternalServiceStatement;
import com.tesora.dve.sql.statement.ddl.PEDropGroupProviderStatement;
import com.tesora.dve.sql.statement.ddl.PEDropRangeStatement;
import com.tesora.dve.sql.statement.ddl.PEDropRawPlanStatement;
import com.tesora.dve.sql.statement.ddl.PEDropStatement;
import com.tesora.dve.sql.statement.ddl.PEDropStorageGroupStatement;
import com.tesora.dve.sql.statement.ddl.PEDropStorageSiteStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTableStatement;
import com.tesora.dve.sql.statement.ddl.PEDropUserStatement;
import com.tesora.dve.sql.statement.ddl.PEDropViewStatement;
import com.tesora.dve.sql.statement.ddl.PEGroupProviderDDLStatement;
import com.tesora.dve.sql.statement.ddl.RenameTableStatement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.ddl.SetPasswordStatement;
import com.tesora.dve.sql.statement.ddl.ShowPlanCacheStatement;
import com.tesora.dve.sql.statement.ddl.alter.AddColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.AddIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeKeysStatusAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeTableDistributionAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeTableModifierAction;
import com.tesora.dve.sql.statement.ddl.alter.ConvertToAction;
import com.tesora.dve.sql.statement.ddl.alter.DropColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.DropIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.RenameTableAction;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.MysqlSelectOption;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.statement.dml.compound.CaseStatement;
import com.tesora.dve.sql.statement.dml.compound.CompoundStatement;
import com.tesora.dve.sql.statement.dml.compound.CompoundStatementList;
import com.tesora.dve.sql.statement.dml.compound.StatementWhenClause;
import com.tesora.dve.sql.statement.session.AnalyzeKeysStatement;
import com.tesora.dve.sql.statement.session.AnalyzeTablesStatement;
import com.tesora.dve.sql.statement.session.DeallocatePStmtStatement;
import com.tesora.dve.sql.statement.session.ExecutePStmtStatement;
import com.tesora.dve.sql.statement.session.ExternalServiceControlStatement;
import com.tesora.dve.sql.statement.session.FlushPrivilegesStatement;
import com.tesora.dve.sql.statement.session.KillStatement;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.sql.statement.session.LockStatement;
import com.tesora.dve.sql.statement.session.LockType;
import com.tesora.dve.sql.statement.session.PreparePStmtStatement;
import com.tesora.dve.sql.statement.session.RollbackTransactionStatement;
import com.tesora.dve.sql.statement.session.SavepointStatement;
import com.tesora.dve.sql.statement.session.SessionSetVariableStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;
import com.tesora.dve.sql.statement.session.SetExpression;
import com.tesora.dve.sql.statement.session.SetTransactionIsolationExpression;
import com.tesora.dve.sql.statement.session.SetVariableExpression;
import com.tesora.dve.sql.statement.session.ShowErrorsWarningsStatement;
import com.tesora.dve.sql.statement.session.ShowPassthroughStatement;
import com.tesora.dve.sql.statement.session.ShowPassthroughStatement.PassThroughCommandType;
import com.tesora.dve.sql.statement.session.ShowProcesslistStatement;
import com.tesora.dve.sql.statement.session.ShowSitesStatusStatement;
import com.tesora.dve.sql.statement.session.StartTransactionStatement;
import com.tesora.dve.sql.statement.session.TableMaintenanceStatement;
import com.tesora.dve.sql.statement.session.TableMaintenanceStatement.MaintenanceCommandType;
import com.tesora.dve.sql.statement.session.TableMaintenanceStatement.MaintenanceOptionType;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.statement.session.UseContainerStatement;
import com.tesora.dve.sql.statement.session.XABeginTransactionStatement;
import com.tesora.dve.sql.statement.session.XACommitTransactionStatement;
import com.tesora.dve.sql.statement.session.XAEndTransactionStatement;
import com.tesora.dve.sql.statement.session.XAPrepareTransactionStatement;
import com.tesora.dve.sql.statement.session.XARecoverTransactionStatement;
import com.tesora.dve.sql.statement.session.XARollbackTransactionStatement;
import com.tesora.dve.sql.template.TemplateManager;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.PassThroughCommand.Command;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.sql.transform.strategy.NaturalJoinRewriter;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryProcedure;
import com.tesora.dve.variable.VariableConstants;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.worker.SiteManagerCommand;
import com.tesora.dve.worker.WorkerGroup;

// holds the bridge methods from antlr tree nodes to our nodes
public class TranslatorUtils extends Utils implements ValueSource {
	
	static Logger logger = Logger.getLogger(TranslatorUtils.class);

	static public final String PERSISTENT_GROUP_TAG = "PERSISTENT GROUP";
	static public final String PERSISTENT_SITE_TAG = "PERSISTENT SITE";
	static public final String PERSISTENT_INSTANCE = "PERSISTENT INSTANCE";
	static public final String DYNAMIC_SITE_PROVIDER = "DYNAMIC SITE PROVIDER";
	static public final String DYNAMIC_SITE_PROVIDER_SITES = DYNAMIC_SITE_PROVIDER + " SITES";
	static public final String DYNAMIC_SITE_POLICY_TAG = "DYNAMIC SITE POLICY";
	
	private final String MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG = "Command was not parsed correctly "
			+ "(can occur if certain keywords are used as identifiers)";

	private static final String UPDATABLE_VIEWS = "No support for updatable views";
	
	SchemaContext pc;
	private ParserOptions opts;
	private boolean resolveColumnsAsIdentifiers;
	private ScopeStack scope;
	private ListOfPairs<DelegatingLiteralExpression,Object> literals;
	private List<Parameter> parameters;
	// as soon as we know the lock type, we register it
	private LockInfo lockInfo;
	// if there is a regular insert, this is the first part of it - everything except the values
	private InsertIntoValuesStatement insertSkeleton;
	// if this is a continuation, the initial offset
	private int initialOffset;
	// this is set if enough is bumped
	private int finalOffset;
	// for big inserts
	private final long continuationThreshold;

	private NativeCharSetCatalog supportedCharSets = null;
	private NativeCollationCatalog supportedCollations = null;
	
	private static final TableResolver basicResolver =
			new TableResolver().withMTChecks()
			.withQualifiedMissingDBFormat("No such database '%s'.");
	
	public static PEAbstractTable<?> getTable(final SchemaContext sc, final Name fullName, final LockInfo lockInfo) {
		TableInstance ti = basicResolver.lookupTable(sc, fullName, lockInfo);
		if (ti == null) return null;
		return ti.getAbstractTable();
	}

	public static UnqualifiedName getDatabaseNameForTable(final SchemaContext sc, final Name tableName) {
		return (tableName.isQualified()) ? ((QualifiedName) tableName).getNamespace() : getCurrentDatabaseName(sc);
	}

	public static UnqualifiedName getCurrentDatabaseName(final SchemaContext sc) {
		final Database<?> currentDatabase = sc.getCurrentDatabase();

		return (currentDatabase != null) ? currentDatabase.getName().getUnqualified() : null;
	}

	public TranslatorUtils(ParserOptions opts, SchemaContext pc, InputState state) {
		super(opts);
		this.pc = pc;
		if (this.pc == null)
			throw new SchemaException(Pass.FIRST, "TranslatorUtils no longer accepts null SchemaContext");
		this.opts = opts;
		scope = new ScopeStack();
		this.lockInfo = null;
		resolveColumnsAsIdentifiers = false;
		literals = new ListOfPairs<DelegatingLiteralExpression,Object>();
		parameters = new ArrayList<Parameter>();
		this.initialOffset = state.getCurrentPosition();
		this.insertSkeleton = state.getInsertSkeleton();
		finalOffset = -1;
		continuationThreshold = state.getThreshold();
		
		if ((this.insertSkeleton != null) && (state instanceof ContinuationInputState)) {
			scope.pushScope();
			scope.insertTable(this.insertSkeleton.getTableInstance());
		}
	}

	public void setContext(SchemaContext sc) {
		pc = sc;
	}
	
	public String getInputSQL() {
		return pc.getOrigStmt();
	}
	
	public void pushScope() {
		scope.pushScope();
	}

	public void popScope() {
		scope.popScope();
	}

	public void pushUnresolvingScope() {
		scope.pushUnresolvingScope();
	}

	public void resolveProjection() {
		scope.resolveProjection(pc);
	}

	public void storeProjection(List<ExpressionNode> l) {
		scope.storeProjection(l);
	}

	public void setGroupByNamespace() {
		scope.setPhase(ScopeParsePhase.GROUPBY);
	}
	
	public void setHavingNamespace() {
		scope.setPhase(ScopeParsePhase.HAVING);
	}
	
	public void setTrailingNamespace() {
		scope.setPhase(ScopeParsePhase.TRAILING);
	}
	
	public int getLastPoppedScope() {
		return scope.getLastPoppedScopeID();
	}
	
	public void repushScope(int id) {
		scope.pushScopeID(id);
	}
	
	public void pushUnresolving() {
		opts = opts.unsetResolve();
	}
	
	public void popUnresolving() {
		opts = opts.setResolve();
	}
	
	// call this during parsing to indicate that we are handling ddl
	public void ddl() {
		if (pc.getCapability() != Capability.PARSING_ONLY) pc.forceMutableSource();
		if (opts == null || opts.getLockOverride() == null) 
			lockInfo = new LockInfo(com.tesora.dve.lockmanager.LockType.EXCLUSIVE, "ddl");
		else if (opts != null && opts.getLockOverride() != null) 
			lockInfo = opts.getLockOverride();
	}

	// call this during parsing to indicate that we are not handling ddl - used during temp table operations
	public void notddl() {
		if (pc.getCapability() != Capability.PARSING_ONLY) pc.forceImmutableSource();
		if (opts == null || opts.getLockOverride() == null) 
			lockInfo = new LockInfo(com.tesora.dve.lockmanager.LockType.EXCLUSIVE, "ddl");
		else if (opts != null && opts.getLockOverride() != null) 
			lockInfo = opts.getLockOverride();
	}
	
	protected void forceUncacheable(ValueManager.CacheStatus status) {
		if (pc.getCapability() != Capability.PARSING_ONLY)
			pc.getValueManager().markUncacheable(status);
	}
	
	public void assignPositions() {
		if (pc.getCapability() == Capability.PARSING_ONLY) return;
		if (!parameters.isEmpty()) {
			TreeMap<SourceLocation, Parameter> map = new TreeMap<SourceLocation, Parameter>();
			for(Parameter p : parameters)
				map.put(p.getSourceLocation(), p);
			if (map.size() != parameters.size())
				throw new SchemaException(Pass.SECOND, "Lost parameters while doing position assignment");
			int i = 0;
			for(Parameter p : map.values()) {
				p.setPosition(i);
				pc.getValueManager().registerParameter(pc, p);
				i++;
			}
		}
		if (literals.size() > KnownVariables.CACHED_PLAN_LITERALS_MAX.getValue(pc.getConnection().getVariableSource()).intValue()) { 
			forceUncacheable(ValueManager.CacheStatus.NOCACHE_TOO_MANY_LITERALS);
		} else {
			TreeMap<SourceLocation, DelegatingLiteralExpression> map = new TreeMap<SourceLocation, DelegatingLiteralExpression>();
			for(Pair<DelegatingLiteralExpression,Object> p : literals) {
				map.put(p.getFirst().getSourceLocation(), p.getFirst());
			}
			if (map.size() != literals.size())
				throw new SchemaException(Pass.SECOND, "Lost literals while doing position assignment");
			int i = 0;
			for(DelegatingLiteralExpression dle : map.values()) {
				pc.getValueManager().addLiteralValue(pc,i, literals.get(dle.getPosition()).getSecond(), dle);
				dle.setPosition(i,true);
				i++;
			}
		}
	}
		
	@SuppressWarnings("unchecked")
	public ProjectingStatement buildSelectStatement(Map<String,Object> components, List<ExpressionNode> projection, List<Object> selectOptions,
			Object tree) {
		return buildSelectStatement((List<FromTableReference>)components.get(EdgeName.TABLES),
				projection,
				(ExpressionNode)components.get(EdgeName.WHERECLAUSE),
				(List<SortingSpecification>)components.get(EdgeName.ORDERBY),
				(LimitSpecification)components.get(EdgeName.LIMIT),
				selectOptions,
				(List<SortingSpecification>)components.get(EdgeName.GROUPBY),
				(ExpressionNode)components.get(EdgeName.HAVING),
				(Boolean)components.get(Statement.SELECT_LOCK_ATTRIBUTE),
				tree);
	}
	
	public ProjectingStatement buildSelectStatement(List<FromTableReference> tableRefs,
			List<ExpressionNode> projection, ExpressionNode whereClause,
			List<SortingSpecification> orderbys, LimitSpecification limit,
			List<Object> selectOptions, List<SortingSpecification> groupbys,
			ExpressionNode havingExpr, Boolean locking, Object tree) {
		// sort the set quantifier and select options
		SetQuantifier sq = null;
		List<MysqlSelectOption> options = new ArrayList<MysqlSelectOption>();
		for(Object opt : selectOptions) {
			if (opt instanceof SetQuantifier)
				sq = (SetQuantifier) opt;
			else 
				options.add((MysqlSelectOption) opt);
		}
		SelectStatement ss = new SelectStatement(tableRefs, projection, 
				whereClause, orderbys, limit, sq, options, groupbys,
				havingExpr, locking, new AliasInformation(scope), SourceLocation.make(tree));

		/*
		 * The NATURAL [LEFT] JOIN are rewritten to an INNER JOIN or a LEFT JOIN
		 * with a USING clause.
		 * The rewrite must take place after ColumnInstance resolution (to
		 * prevent premature failure on ambiguous column names), but before
		 * USING-to-ON clause conversion and wildcard expansion which affect the
		 * projection coalescing and ordering.
		 */
		if (tableRefs != null) {
			for (final FromTableReference ftr : tableRefs) {
				final TableInstance base = ftr.getBaseTable();
				final ListSet<JoinedTable> naturalJoins = NaturalJoinRewriter.collectNaturalJoins(ftr.getTableJoins());
				for (final JoinedTable join : naturalJoins) {
					NaturalJoinRewriter.rewriteToInnerJoin(this.pc, base, join);
				}

				convertUsingColSpecToOnSpec(base, naturalJoins);
			}
		}

		ss.getDerivedInfo().takeScope(scope);
		Scope ps = scope.getParentScope();
		if (ps != null)
			ps.getNestedQueries().add(ss);
		
		shouldSetTimestampVariable(ss, null, null);
		
		return ss;
	}

	public Statement buildUpdateStatement(List<FromTableReference> tableRefs,
			List<ExpressionNode> updateExprs, ExpressionNode whereClause,
			List<SortingSpecification> orderbys, LimitSpecification limit,
			boolean ignore, Object tree) {

		PEAbstractTable<?> tab = tableRefs.get(0).getBaseTable().getAbstractTable();
		if (tab != null && tab.isView())
			throw new SchemaException(Pass.SECOND, UPDATABLE_VIEWS);

		UpdateStatement us = new UpdateStatement(tableRefs, updateExprs,
				whereClause, orderbys, limit, new AliasInformation(scope), SourceLocation.make(tree));
		us.setIgnore(ignore);
		us.getDerivedInfo().takeScope(scope);

		if (tab != null)
			shouldSetTimestampVariable(us, tab.asTable(), updateExprs);
		
		return us;
	}

	public Statement buildDeleteStatement(List<FromTableReference> tableRefs,
			List<Name> explicitRefs,
			ExpressionNode whereClause, List<SortingSpecification> orderbys, 
			LimitSpecification limit, Object sloc) {
		List<TableInstance> explicitDeletes = null;
		if (explicitRefs == null || explicitRefs.isEmpty()) {
			// ignore
		} else {
			explicitDeletes = new ArrayList<TableInstance>();
			for(Name r : explicitRefs) {
				if (pc.getCapability() == Capability.PARSING_ONLY) {
					explicitDeletes.add(new TableInstance(null,r,null,false));
				} else {
					Name actual = r;
					// if it has a trailing * strip that off
					List<UnqualifiedName> parts = r.getParts();
					if (parts.size() > 1 && parts.get(parts.size() - 1).isAsterisk()) {
						ArrayList<UnqualifiedName> nparts = new ArrayList<UnqualifiedName>(parts);
						nparts.remove(nparts.size() - 1);
						if (nparts.size() == 1)
							actual = nparts.get(0);
						else
							actual = new QualifiedName(nparts);
					}
					explicitDeletes.add(scope.lookupTableInstance(pc, actual, true));
				}
			}
		}
		DeleteStatement ds = new DeleteStatement(explicitDeletes, tableRefs, whereClause,
				orderbys, limit, false, new AliasInformation(scope), SourceLocation.make(sloc));
		ds.getDerivedInfo().takeScope(scope);
		
		shouldSetTimestampVariable(ds, null, null);
		
		return ds;
	}

	@SuppressWarnings("unchecked")
	public void pushSkeletonInsert(ExpressionNode tab, boolean replace, Object tree) {
		TableInstance intoTable = (TableInstance) tab;
		InsertIntoValuesStatement is = null;
		SourceLocation sloc = SourceLocation.make(tree);
		if (replace)
			is = new ReplaceIntoValuesStatement(intoTable, Collections.EMPTY_LIST, Collections.EMPTY_LIST, new AliasInformation(scope), sloc);
		else
			is = new InsertIntoValuesStatement(intoTable, Collections.EMPTY_LIST, Collections.EMPTY_LIST, null, new AliasInformation(scope), sloc);
		is.getDerivedInfo().addLocalTable(intoTable.getTableKey());
		is.getDerivedInfo().takeScope(scope);
		insertSkeleton = is;
	}
	
	public ExpressionNode pushInsertSkeletonField(ExpressionNode field) {
		insertSkeleton.getColumnSpecificationEdge().add(field);
		return field;
	}
	
	public InsertIntoValuesStatement buildInsertStatement(List<List<ExpressionNode>> values, boolean copy, TransactionStatement.Kind txnal, InsertModifier im, boolean ignore) {
		return buildInsertStatement(null,values, copy, txnal, im, ignore);
	}
	
	private InsertIntoValuesStatement buildInsertStatement(List<ExpressionNode> columnSpec, List<List<ExpressionNode>> values, boolean copy, TransactionStatement.Kind txnal, InsertModifier im, boolean ignore) {
		InsertIntoValuesStatement is = null;
		if (copy)
			is = CopyVisitor.copy(insertSkeleton);
		else
			is = insertSkeleton;
		if (columnSpec != null)
			is.setColumnSpecification(columnSpec);
		is.setValues(values);
		is.setTxnFlag(txnal);
		is.setIgnore(ignore);
		if (im != null) {
			if (opts.isResolve() && im.equals(InsertModifier.DELAYED)) {
				// replication slave always strips the DELAYED keyword
				if (!pc.getConnection().originatedFromReplicationSlave()) {
					EngineTableModifier tem = is.getTableInstance().getAbstractTable().asTable().getEngine();
					if (tem.isMyISAM())
						is.setModifier(im);
					else
						throw new SchemaException(Pass.SECOND,
								"Insert modifier '" + im.getSQL() + "' option not supported on table of type " + tem.getEngine().getSQL()); 
				}
			} else {
				is.setModifier(im);
			}
		}

		shouldSetTimestampVariable(is,
				is.getTableInstance().getAbstractTable(),
				is.getColumnSpecification(),
				values);
		
		return is;
	}
	
	public Statement buildInsertStatement(List<List<ExpressionNode>> values, List<ExpressionNode> onDupKey, InsertModifier im, boolean ignore) {
		InsertIntoValuesStatement is = buildInsertStatement(values,false,(initialOffset > 0 ? TransactionStatement.Kind.COMMIT : null), im, ignore);
		is.setOnDuplicateKey(onDupKey);
		// clear the skeleton
		insertSkeleton = null;
		
		return is;
	}
	
	/**
	 * @param select
	 * @param ignore
	 * @param onDupKey
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Statement buildInsertIntoSelectStatement(ExpressionNode select, boolean ignore, List<ExpressionNode> onDupKey) {
		Subquery sq = (Subquery) select;
		ProjectingStatement selectStatement = sq.getStatement();
		// copy the table, etc. out of the skeleton, then clear it
		InsertIntoSelectStatement iiss = null;
		if (insertSkeleton.isReplace()) {
			iiss = new ReplaceIntoSelectStatement(insertSkeleton.getTableInstance(),insertSkeleton.getColumnSpecification(),
					selectStatement, sq.isGrouped(), new AliasInformation(scope), insertSkeleton.getSourceLocation());
		} else {
			iiss = new InsertIntoSelectStatement(insertSkeleton.getTableInstance(),insertSkeleton.getColumnSpecification(),
					selectStatement, sq.isGrouped(), onDupKey, new AliasInformation(scope), insertSkeleton.getSourceLocation());
			iiss.setIgnore(ignore);
		}
		// the select is nested
		iiss.getDerivedInfo().addNestedStatements(
				Collections.singleton(selectStatement));
		iiss.getDerivedInfo().addLocalTable(
				insertSkeleton.getTableInstance().getTableKey());
		iiss.getDerivedInfo().takeScope(scope);
	
		shouldSetTimestampVariable(iiss,
				insertSkeleton.getTableInstance().getAbstractTable(), insertSkeleton.getColumnSpecification(), Collections.EMPTY_LIST);

		insertSkeleton = null;
		return iiss;
	}

	public void enough(PE_MySQL parser, List<List<ExpressionNode>> insertValues) {
		Lexer lexer = (Lexer) parser.getTokenStream().getTokenSource(); 
		if (lexer.getCharIndex() - initialOffset > continuationThreshold) {
			finalOffset = lexer.getCharIndex();
			throw new EnoughException(buildInsertStatement(insertValues,true, (initialOffset == 0 ? TransactionStatement.Kind.START : null),null,false));
		}
	}

	public void reportContinuationOnDupKey() {
		throw new ParserException(Pass.FIRST, "Statement is too large. Consider increasing the '" + VariableConstants.LARGE_INSERT_THRESHOLD_NAME
				+ "' value.", null);
	}

	public InputState getInputState(InputState in) {
		if (finalOffset == -1)
			return null;
		if (finalOffset > -1) {
			in.setCurrentPosition(finalOffset);
		}
		if (insertSkeleton != null && in.getInsertSkeleton() == null)
			in.setInsertSkeleton(insertSkeleton);
		return in;
	}
	
	/**
	 * @param updateExprs
	 * @param ignore
	 * @param onDupKey
	 * @param im
	 * @return
	 */
	public Statement buildInsertIntoSetStatement(List<ExpressionNode> updateExprs, boolean ignore, List<ExpressionNode> onDupKey,InsertModifier im) {
		// use insert skeleton as is, since we cannot match the extended insert in the parser
		// but we have to unpack the update exprs to build the column spec and values
		List<ExpressionNode> columnSpec = new ArrayList<ExpressionNode>();
		List<ExpressionNode> values = new ArrayList<ExpressionNode>();
		for(ExpressionNode en : updateExprs) {
			FunctionCall fc = (FunctionCall) en;
			columnSpec.add(fc.getParametersEdge().get(0));
			values.add(fc.getParametersEdge().get(1));
		}
		return buildInsertStatement(columnSpec, Collections.singletonList(values), false, null, im, ignore);
	}
	
	private boolean shouldSetTimestampVariable(DMLStatement dmls, PETable tab,
			List<ExpressionNode> updateExprs) {

		// separate the update set column=value expression into columns and
		// values lists
		List<ExpressionNode> fields = new ArrayList<ExpressionNode>();
		List<List<ExpressionNode>> allValues = new ArrayList<List<ExpressionNode>>();
		List<ExpressionNode> rowValues = new ArrayList<ExpressionNode>();
		if (updateExprs != null) {
			for (ExpressionNode node : updateExprs) {
				// update set column= are function calls
				FunctionCall f = (FunctionCall) node;
				Pair<ColumnInstance, ExpressionNode> params = decomposeUpdateAssignment(f);
				fields.add(params.getFirst());
				rowValues.add(params.getSecond());
			}
		}
		allValues.add(rowValues);

		return shouldSetTimestampVariable(dmls, tab, fields, allValues);
	}

	private boolean shouldSetTimestampVariable(DMLStatement dmls, PEAbstractTable<?> tab,
			List<ExpressionNode> fields, List<List<ExpressionNode>> values) {

		// check if the now or current_timestamp function is used
		boolean ret = TimestampVariableUtils.isNowFunctionCallSpecified(dmls
				.getDerivedInfo().getFunctions());

		// can't do anything if petable is null
		if (tab == null) {
			// could have been set by above call so save in our statement
			// save the set timestamp variable flag in our statement
			dmls.getDerivedInfo().setSetTimestampVariable(ret);
			
			return ret;
		}

		if (!ret) {

			// determine which columns in the table are specified and which are
			// not
			List<PEColumn> specifiedColumns = new ArrayList<PEColumn>();
			List<PEColumn> unspecifiedColumns = new ArrayList<PEColumn>(
					tab.getColumns(pc));
			if (fields == null || fields.isEmpty()) {
				specifiedColumns.addAll(unspecifiedColumns);
				unspecifiedColumns.clear();				
			} else {
				List<PEColumn> temp = new ArrayList<PEColumn>();
				for (ExpressionNode field : fields) {
					temp.add(((ColumnInstance) field).getPEColumn());
				}
				unspecifiedColumns.removeAll(temp);
				for (ExpressionNode col : fields) {
					PEColumn c = ((ColumnInstance) col).getPEColumn();
					specifiedColumns.add(c);
				}
			}

			for (PEColumn c : unspecifiedColumns) {
				ret = TimestampVariableUtils
						.setTimestampVariableForUnspecifiedColumn(pc,dmls, c);
				if (ret) {
					// set the variable so break loop
					break;
				}
			}

			// we need values to check against
			if (!ret && (values != null)) {
				// haven't set the timestamp variable yet
				// so check if the specified columns need to set it
				for (int i = 0; i < specifiedColumns.size(); ++i) {
					PEColumn c = specifiedColumns.get(i);
					for (List<ExpressionNode> v : values) {
						// make sure the ___mtid is skipped
						// by checking the specified column count doesn't
						// exceed the values
						if (i >= v.size()) {
							continue;
						}
						ExpressionNode e = v.get(i);
						ExpressionNode r = e;
						if (e instanceof Default) {
							ExpressionNode defaultValue = c.getDefaultValue();
							if (defaultValue == null) {
								if (c.isNullable()) {
									r = LiteralExpression.makeNullLiteral();
								}
							} else {
								r = (ExpressionNode) ((LiteralExpression) defaultValue).copy(null);
							}
						}
						ret = TimestampVariableUtils
								.setTimestampVariableForSpecifiedValue(c, r);
						if (ret) {
							break;
						}
					}
					if (ret) {
						break;
					}
				}
			}
		}

		// save the set timestamp variable flag in our statement
		dmls.getDerivedInfo().setSetTimestampVariable(ret);

		return ret;
	}

	private Pair<ColumnInstance, ExpressionNode> decomposeUpdateAssignment(
			FunctionCall fc) {
		ColumnInstance ci = null;
		ExpressionNode le = null;
		// for update assignment, we know exactly what we have - so just access
		// by index
		if (fc.getFunctionName().isEquals()) {
			List<ExpressionNode> params = fc.getParameters();
			if (params.get(0) instanceof ColumnInstance)
				ci = (ColumnInstance) params.get(0);
			else
				return null;
			le = params.get(1);
			return new Pair<ColumnInstance, ExpressionNode>(ci, le);
		}
		return null;
	}

	public Statement buildCreateTable(Name tableName, Name oldTableName, Boolean ine) {
		if (( tableName == null ) || ( oldTableName == null )) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		PECreateTableStatement pecs = null;
		UnqualifiedName tabName = tableName.getUnqualified();
		TableInstance ti = pc.getCurrentPEDatabase().getSchema().buildInstance(pc, tabName, lockInfo);
		if (Boolean.TRUE.equals(ine) && opts.isResolve()) {
			// see if the table already exists
			// UnqualifiedName dbName = null;
			if (ti != null) {
				if (ti.getAbstractTable().isView())
					throw new SchemaException(Pass.FIRST, tabName + " is a view, cannot create like");
				pecs = new PECreateTableStatement(ti.getAbstractTable().asTable(), ine, true);
				return pecs;
			}
		}

		PEAbstractTable<?> tab = null;
		if (ti != null)
			tab = ti.getAbstractTable();
		
		// see if the table already exists
		if (tab == null) {
			Name dbName = null;
			// now need to determine if source or old table exists too
			if (oldTableName.isQualified()) {
				dbName = ((QualifiedName)oldTableName).getNamespace();
			}
			if (dbName == null) {
				dbName = (pc.getCurrentDatabase() == null) ? null : pc.getCurrentDatabase().getName();
			}
			PEDatabase db = pc.findPEDatabase(dbName);
			if (db == null)
				throw new SchemaException(Pass.FIRST, "No such database: '" + dbName + "'");
			
			TableInstance otab = db.getSchema().buildInstance(pc, oldTableName.getUnqualified(), lockInfo);
			if (otab == null) {
				throw new SchemaException(Pass.FIRST, "No source table: '" + oldTableName + "'");
			}
			if (otab.getAbstractTable().isView())
				throw new SchemaException(Pass.FIRST, "Source table is a view");
			PETable oldTab = otab.getAbstractTable().asTable();
			
			String cts = oldTab.getDeclaration();
			if (cts == null)
				throw new SchemaException(Pass.FIRST, "Unable to obtain source create table statement");
			tab = oldTab.recreate(pc, cts, lockInfo);
			tab.setName(tabName);
			tab.asTable().removeForeignKeys(pc);
			tab.asTable().setDeclaration(pc, tab.asTable());
		}
		pecs = new PECreateTableStatement(tab.asTable(), ine, false);
		
		return pecs;
	}
	
	public Statement buildCreateTable(Name tableName,
			List<TableComponent<?>> fieldsAndKeys, UnresolvedDistributionVector indv,
			Name groupName, List<TableModifier> modifiers, Boolean ine, 
			Pair<UnqualifiedName,List<UnqualifiedName>> discriminator,
			ProjectingStatement ctas,
			boolean temporary) {
		if ( tableName == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		PECreateTableStatement pecs = null;
		if (Boolean.TRUE.equals(ine) && opts.isResolve()) {
			// see if the table already exists
			// UnqualifiedName dbName = null;
			TableInstance ti =
					new TableResolver().withMTChecks().lookupTable(pc, tableName, lockInfo);
			if (ti != null) {
				if (ti.getAbstractTable().isView())
					throw new SchemaException(Pass.FIRST,tableName + " is a view (cannot recreate)");
				pecs = new PECreateTableStatement(ti.getAbstractTable().asTable(), ine, true);
			} 
		}
		PEPersistentGroup pesg = null;
		if (groupName != null) {
			pesg = pc.findStorageGroup(groupName);
			if (pesg == null)
				throw new SchemaException(Pass.SECOND,
						"No such persistent group: " + groupName.getSQL());
		}
		
		List<TableComponent<?>> actualFieldsAndKeys = null;
		
		ListOfPairs<PEColumn,Integer> ctaProjectionOffsets = null;
		
		if (ctas != null) {
			ctaProjectionOffsets = new ListOfPairs<PEColumn,Integer>();
			// the columns are ordered like so:
			// first all columns that are only explicitly declared
			// then all columns that are only implicitly or both declared
			// we will determine projection column types at runtime, and use a placeholder in the meantime
			
			ListSet<PEColumn> inproj = new ListSet<PEColumn>();

			ProjectionInfo pmd = ctas.getProjectionMetadata(pc);
			for(int i = 1; i <= pmd.getWidth(); i++) {
				int offset = i - 1;
				ColumnInfo ci = pmd.getColumnInfo(i);
				UnqualifiedName cname = new UnqualifiedName(ci.getAlias());
				PEColumn matching = lookupInProcessColumn(cname, true);
				if (matching != null) {
					ctaProjectionOffsets.add(matching,offset);
					inproj.add(matching);
					continue;
				}
				// declare the column with a placeholder type
				PEColumn viaCTA = PECreateTableAsSelectStatement.createColumnFromExpression(pc,ci,ctas.getProjections().get(0).get(offset));
				scope.registerColumn(viaCTA);
				ctaProjectionOffsets.add(viaCTA, offset);
				inproj.add(viaCTA);
			}
			
			actualFieldsAndKeys = new ArrayList<TableComponent<?>>();
			// first do the decl only bits
			for(Iterator<TableComponent<?>> iter = fieldsAndKeys.iterator(); iter.hasNext();) {
				TableComponent<?> tc = iter.next();
				if (tc instanceof PEKey)
					continue;
				PEColumn pec = (PEColumn) tc;
				if (!inproj.contains(pec)) 
					actualFieldsAndKeys.add(pec);
				iter.remove();
			}
			// now all the projection fields
			actualFieldsAndKeys.addAll(inproj);
			// now we have all columns declared, resolve anything that was forward
			for(Iterator<TableComponent<?>> iter = fieldsAndKeys.iterator(); iter.hasNext();) {
				TableComponent<?> tc = iter.next();
				if (tc instanceof PEKey) {
					PEKey pek = (PEKey) tc;
					actualFieldsAndKeys.add(pek.resolve(pc,scope));
				} else {
					actualFieldsAndKeys.add(tc);
				}
			}
		} else {
			actualFieldsAndKeys = fieldsAndKeys;
		}
		
		// resolve the distribution vector
		DistributionVector dv = null;
		if (indv != null)
			dv = indv.resolve(pc, this);
		
		if (pecs == null) {
			// unpack the dbstuff
			PETable newTab = null;
			// we want to inject when both the dist vect and the discriminator are null; if either is non-null the dist info
			// was specified
			if (dv == null && discriminator == null) {
				newTab = buildTable(tableName, actualFieldsAndKeys, null, pesg, modifiers, ctaProjectionOffsets != null, temporary);
				if ((pc.getCapability() == Capability.PARSING_ONLY) || (opts != null && opts.isOmitMetadataInjection())) {
					dv = new DistributionVector(pc, null, DistributionVector.Model.RANDOM);
					newTab.setDistributionVector(pc,dv);				
				} else
					try {
						if (!TemplateManager.inject(pc, newTab.getPEDatabase(pc), newTab)) {
							if (newTab.getPEDatabase(pc).hasStrictTemplateMode()) {
								throw new SchemaException(Pass.SECOND,"No matching template found for table " + newTab.getName().getSQL());
							}
							dv = new DistributionVector(pc, null, DistributionVector.Model.RANDOM);
							newTab.setDistributionVector(pc,dv);
						}
					} catch (Exception e) {
						throw new SchemaException(Pass.SECOND, e);
					}
			} else if (dv != null) {
				newTab = buildTable(tableName, actualFieldsAndKeys, dv, pesg, modifiers, ctaProjectionOffsets != null, temporary);
			} else if (discriminator != null) {
				// newTab will be the base table on the container - so change the dist vect on it to be the container
				PEContainer container = pc.findContainer(discriminator.getFirst());
				if (container == null)
					throw new SchemaException(Pass.SECOND, "No such container: " + discriminator.getFirst().getSQL());

				if (container.hasBaseTable()) {
					throw new SchemaException(Pass.SECOND, "Cannot set table '" + tableName + "' as a base table because container '" + discriminator.getFirst().getSQL() + "' already has a base table.");
				}

				dv = new ContainerDistributionVector(pc,container,false);
				newTab = buildTable(tableName, fieldsAndKeys, dv, pesg, modifiers, ctaProjectionOffsets != null, temporary);
				// newTab is actually the container base table - so go resolve the columns now and so mark them
				List<UnqualifiedName> colNames = discriminator.getSecond();
				for(int i = 0; i < colNames.size(); i++) {
					UnqualifiedName un = colNames.get(i);
					PEColumn pec = newTab.lookup(pc, un);
					if (pec == null)
						throw new SchemaException(Pass.SECOND, "No such column: " + un.getSQL() + " - cannot build discriminator");
					pec.setContainerDistributionValuePosition(i + 1);
				}
				container.setBaseTable(pc, newTab);
			} else {
				throw new SchemaException(Pass.SECOND, "Unable to determine declared distribution for table " + tableName.getSQL());
			}
			if (ctas == null)
				pecs = new PECreateTableStatement(newTab, ine, false);
			else
				pecs = new PECreateTableAsSelectStatement(newTab, ine, false, ctas, ctaProjectionOffsets);
		}
		Statement out = pecs;
		if (pc.getCapability() != Capability.PARSING_ONLY && !pc.getOptions().isTSchema()) {
			// containers don't generally have a separate policy at creation time - check for it on
			// the dist vect
			if (pecs.getCreated().get().getDistributionVector(pc).isContainer()) {
				out = ContainerPolicyContext.modifyCreateTable(pc, pecs);
			} else {
				out = pc.getPolicyContext().modifyCreateTable(pecs);
			}
		}
		return out;
	}

	public Statement buildDropTableStatement(List<Name> givenNames, Boolean ifExists, boolean tempTabs) {
		if ( givenNames == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}
		
		TableResolver resolver = 
				new TableResolver().withMTChecks().withDatabaseFunction(new UnaryProcedure<Database<?>>() {

					@Override
					public void execute(Database<?> object) {
						if (!(object instanceof PEDatabase))
							throw new SchemaException(Pass.SECOND,
									"Invalid database for drop table: '" + object.getName()
											+ "'");						
					}
					
				});
		
		List<TableKey> tblKeys = new ArrayList<TableKey>();
		List<Name> unknownTables = new ArrayList<Name>();
		for(Name givenName : givenNames) {
			TableInstance ti = resolver.lookupTable(pc, givenName, lockInfo);
			if (ti == null) {
				unknownTables.add(givenName);
			} else {
				if (tempTabs && !ti.getTableKey().isUserlandTemporaryTable())
					throw new SchemaException(new ErrorInfo(DVEErrors.UNKNOWN_TABLE,givenName.getUnquotedName().get()));
				tblKeys.add(ti.getTableKey());
			}
		}
		
		// we can throw if there is no valid existing table
		if ((tblKeys.size() == 0) && unknownTables.size() > 0) {
			if (!Boolean.TRUE.equals(ifExists))
				throw new SchemaException(Pass.SECOND, "No such table(s) '"
						+ StringUtils.join(unknownTables, ",") + "'");
		}
		
		PEDropTableStatement stmt = new PEDropTableStatement(pc,tblKeys, unknownTables, ifExists, tempTabs);
		return pc.getPolicyContext().modifyDropTable(stmt);
	}

	private Database<?> findDatabase(Name givenName) {
		Database<?> ondb = pc.getCurrentDatabase(false);
		if (ondb == null || givenName.isQualified()) {
			if (!givenName.isQualified())
				pc.getCurrentDatabase(true);
			QualifiedName qname = (QualifiedName) givenName;
			UnqualifiedName dbName = qname.getNamespace();
			if (ondb == null || !ondb.getName().equals(dbName)) {
				ondb = pc.findDatabase(dbName);
				if (ondb == null)
					return ondb;
			}
		}
		return ondb;
	}

	public Statement buildDropDatabaseStatement(Name dbName, Boolean ifExists,
			boolean dropmt, String tag) {
		if ( dbName == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		return pc.getPolicyContext().buildDropDatabaseStatement(dbName,
				ifExists, dropmt, tag);
	}

	public Statement buildShowCreateDatabaseQuery(String onInfoSchemaTable,
			Name objectName, Boolean ifNotExists) {
        ShowSchemaBehavior ist = Singletons.require(HostService.class).getInformationSchema()
				.lookupShowTable(new UnqualifiedName(onInfoSchemaTable));
		if (ist == null)
			throw new MigrationException("Need to add info schema table for "
					+ onInfoSchemaTable);
		ShowOptions opts = new ShowOptions();
		if (Boolean.TRUE.equals(ifNotExists))
			opts = opts.withIfNotExists();
		return ist.buildUniqueStatement(pc, objectName, opts);
	}

	public Statement buildUseDatabaseStatement(Name firstName) {
		if (firstName == null)
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new SessionStatement("use " + firstName.getSQL()) {
				@Override
				public boolean isPassthrough() {
					return false;
				}
			};
		return pc.getPolicyContext().buildUseDatabaseStatement(firstName);					
	}
	
	/**
	 * @param projName
	 * @return
	 */
	public Statement buildUseProjectStatement(Name projName) {
		// no longer supported (just for the persistent version)
		throw new ParserException(Pass.FIRST,
				"No support for use project for persistent schema");
	}

	/**
	 * @param jdbcURL
	 * @return
	 */
	public Statement buildCreateCatalog(Token jdbcURL) {
		throw new SchemaException(Pass.SECOND, "No support for create catalog.");
	}

	@SuppressWarnings("unchecked")
	public Statement buildCreateDatabase(Name dbName, Boolean ifNotExists, String tag, MultitenantMode mm, 
			List<Pair<?,?>> consolidatedDefs) {
		if ( dbName == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		String charSetValue = null;
		String collationValue = null;
		Name pgName = null;
		FKMode fkMode = null;
		Pair<Name, TemplateMode> templateDecl = null;
		for(Pair<?,?> p : consolidatedDefs) {
			if (p.getFirst() instanceof String) {
				String key = (String) p.getFirst();
				if ("fkmode".equals(key)) {
					fkMode = (FKMode) p.getSecond();
					continue;
				}
				Name value = (Name)p.getSecond();
				if ("charset".equals(key)) {
					charSetValue = value.get();
				} else if ("collate".equals(key)) {
					collationValue = value.get();
				} else if ("pers_group".equals(key)) {
					pgName = value;
				} else {
					throw new SchemaException(Pass.FIRST,"Unknown create db attribute: " + key);
				}
			} else if (p.getSecond() instanceof TemplateMode) {
				templateDecl = (Pair<Name, TemplateMode>) p;
			}
		}
		
		if (templateDecl == null) {
			templateDecl = new Pair<Name, TemplateMode>(null, TemplateMode.getCurrentDefault(pc.getConnection()));
		}

		final Pair<String, String> charSetCollationPair = getCharSetCollationPair(charSetValue, collationValue);

		if (pc.getCapability() == Capability.PARSING_ONLY) {
			PEDatabase pdb = new PEDatabase(null, dbName.getUnquotedName(), null, templateDecl, mm, fkMode, charSetCollationPair.getFirst(),
					charSetCollationPair.getSecond());
			PECreateStatement<PEDatabase, UserDatabase> cdb = new PECreateDatabaseStatement(
					pdb, false, ifNotExists, tag, false);
			return cdb;
		}

		return pc.getPolicyContext().buildCreateDatabase(dbName,
				pgName,
				templateDecl,
				ifNotExists, tag, mm, fkMode, charSetCollationPair.getFirst(), charSetCollationPair.getSecond());
	}

	public Statement buildAlterDatabaseStatement(final Name dbName, final Name charSetName, final Name collationName) {
		final PEDatabase db = getAlterDatabase(dbName);

		if ((charSetName == null) && (collationName == null)) {
			throw new SchemaException(Pass.SECOND, "Can't alter database '" + dbName.getSQL() + "'; syntax error");
		}

		final Pair<String, String> charSetCollationPair = getCharSetCollationPair(charSetName, collationName);

		return new AlterDatabaseStatement(db, charSetCollationPair.getFirst(), charSetCollationPair.getSecond());
	}

	public Statement buildAlterDatabaseStatement(final Name dbName, final Pair<Name, TemplateMode> templateDeclaration) {
		final PEDatabase db = getAlterDatabase(dbName);
		final Pair<Name, TemplateMode> checkedTemplateDecl = TemplateManager.findTemplateForDatabase(pc, dbName, templateDeclaration.getFirst(),
				templateDeclaration.getSecond());
		
		return new AlterDatabaseTemplateStatement(db, checkedTemplateDecl);
	}

	public Pair<Name, TemplateMode> buildTemplateDeclaration(final Name templateName, TemplateMode mode) {
		final String templateNameIdentifier = templateName.get();
		if (TemplateMode.hasModeForName(templateNameIdentifier)) {
			if (templateNameIdentifier.equals(TemplateMode.OPTIONAL.toString())) {
				if (mode == null) {
					return new Pair<Name, TemplateMode>(null, TemplateMode.OPTIONAL);
				}
			}

			throw new SchemaException(Pass.SECOND, "Redundant mode specification '" + mode.toString() + "'; syntax error");
		}

		if ((mode != null) && !mode.requiresTemplate()) {
			throw new SchemaException(Pass.SECOND, "Redundant template specification '" + templateName.getSQL() + "' for " + VariableConstants.TEMPLATE_MODE_NAME
					+ " '" + mode.toString() + "'; syntax error");
		}

		if (mode == null) {
			mode = TemplateMode.getCurrentDefault(pc.getConnection());
		}

		return new Pair<Name, TemplateMode>(templateName, mode);
	}

	private PEDatabase getAlterDatabase(final Name dbName) {
		final Database<?> db = (dbName != null) ? pc.findDatabase(dbName) : pc.getCurrentDatabase();
		if (db == null) {
			throw new SchemaException(Pass.SECOND, "Can't alter database '" + dbName.getSQL() + "'; database doesn't exist");
		} else if (!(db instanceof PEDatabase)) {
			throw new SchemaException(Pass.SECOND, "Can't alter database '" + dbName.getSQL() + "'; target is not alterable");
		}

		return (PEDatabase) db;
	}

	private Pair<String, String> getCharSetCollationPair(final Name charSetName, final Name collationName) {
		final String charSetValue = (charSetName != null) ? charSetName.get() : null;
		final String collationValue = (collationName != null) ? collationName.get() : null;
		return getCharSetCollationPair(charSetValue, collationValue);
	}

	/**
	 * Find a valid charset-collation combination.
	 * 
	 * CASES:
	 * a) Both values left unspecified => use server-wide defaults.
	 * b) Charset, but no collation => use default collation for the charset.
	 * c) Collation, but no charset => find charset the collation belongs to.
	 * d) Both values specified => just validate they are compatible.
	 */
	private Pair<String, String> getCharSetCollationPair(final String charSetValue, final String collationValue) {
        final DBNative db = Singletons.require(HostService.class).getDBNative();
		if ((charSetValue == null) && (collationValue == null)) { // Use server defaults.
			String defaultCharSet;
			try {
				defaultCharSet = db.getDefaultServerCharacterSet();
			} catch (final Exception e) {
				throw new SchemaException(Pass.FIRST, "No default server character set");
			}
			
			String defaultCollation;
			try {
				defaultCollation = db.getDefaultServerCollation();
			} catch (final Exception e) {
				throw new SchemaException(Pass.FIRST, "No default server collation set");
			}
			
			return new Pair<String, String>(defaultCharSet, defaultCollation);
		}
		
		if ((charSetValue != null) && (collationValue == null)) {  // Use default for the charset.
			NativeCollation nc = lookupDefaultCollationForCharsetInCatalog(charSetValue);
			if (nc == null)
				throw new SchemaException(Pass.FIRST, "Unable to build plan - Unsupported CHARACTER SET " + charSetValue);
			return new Pair<String, String>(charSetValue, nc.getName());
		} else if ((charSetValue == null) && (collationValue != null)) {  // Use an appropriate charset.
			try {
				final NativeCharSet charSet = getNativeCharSetCatalog().findCharSetByCollation(collationValue, true);
				return new Pair<String, String>(charSet.getName(), collationValue);
			} catch (final PEException e) {
				throw new SchemaException(Pass.FIRST, e.getMessage());
			}
		} else { // Just check the values for mutual compatibility.
			final NativeCharSet charSet = lookupCharsetInCatalog(charSetValue, getNativeCharSetCatalog());
			if (charSet.isCompatibleWith(collationValue)) {
				return new Pair<String, String>(charSetValue, collationValue);
			}
			throw new SchemaException(Pass.FIRST, "COLLATION '" + collationValue + "' is not valid for CHARACTER SET '" + charSetValue + "'");
		}
	}

	private NativeCharSet lookupCharsetInCatalog(final String name, final NativeCharSetCatalog supportedCharSets) {
		try {
			return supportedCharSets.findCharSetByName(name, true);
		} catch (final PEException e) {
			throw new SchemaException(Pass.FIRST, e.getMessage());
		}
	}

	private NativeCollation lookupDefaultCollationForCharsetInCatalog(final String charsetName) {
		try {
			return getNativeCollationCatalog().findDefaultCollationForCharSet(charsetName, true);
		} catch (final PEException e) {
			throw new SchemaException(Pass.FIRST, e.getMessage());
		}
	}

	public Statement buildCreatePersistentInstance(Name persistentInstanceName,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("create a persistent instance");
		
		if (persistentInstanceName == null)
			throw new SchemaException(Pass.SECOND,
					"Persistent instance name must be specified.");

		PESiteInstance pesi = pc.findSiteInstance(persistentInstanceName);
		if (pesi != null)
			throw new SchemaException(Pass.SECOND, "Persistent instance "
					+ persistentInstanceName + " already exists.");
		return PECreateSiteInstanceStatement.build(pc,persistentInstanceName,options);
	}

	public Statement buildAlterPersistentInstanceStatement(Name persistentInstanceName,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("change a persistent instance");
		PESiteInstance pesi = pc.findSiteInstance(persistentInstanceName);
		if (pesi == null)
			throw new SchemaException(Pass.SECOND, "No such persistent instance: "
					+ persistentInstanceName.getUnqualified().get());
		return new PEAlterSiteInstanceStatement(pesi, options);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Statement buildDropPersistentInstanceStatement(Name persistentInstanceName) {
		pc.getPolicyContext().checkRootPermission("drop a persistent instance");
		PESiteInstance pesi = pc.findSiteInstance(persistentInstanceName);
		if (pesi == null)
			throw new SchemaException(Pass.SECOND, "No such persistent instance: "
					+ persistentInstanceName.getUnqualified().get());
		return new PEDropStatement(PESiteInstance.class, null, true, pesi,
				PERSISTENT_INSTANCE);
	}

	public Statement buildCreatePersistentSite(Name persistentSiteName, List<Pair<Name,LiteralExpression>> opts) {
		pc.getPolicyContext().checkRootPermission("create a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess != null)
			throw new SchemaException(Pass.SECOND, "Persistent site "
					+ persistentSiteName.getSQL() + " already exists.");
		return PECreateStorageSiteStatement.build(pc, persistentSiteName,opts);
	}

	public Statement buildCreatePersistentSite(Name persistentSiteName, String url, String user, String password) {
		pc.getPolicyContext().checkRootPermission("create a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess != null)
			throw new SchemaException(Pass.SECOND, "Persistent site "
					+ persistentSiteName.getSQL() + " already exists.");
		// the jdbcURL probably still has the quotes on - have to strip those
        String stripped = Singletons.require(HostService.class).getDBNative().getValueConverter().convertStringLiteral(url);
		// convert it into the other format
		List<Pair<Name,LiteralExpression>> opts = new ArrayList<Pair<Name,LiteralExpression>>();
		opts.add(buildConfigOption(new UnqualifiedName("URL"),LiteralExpression.makeStringLiteral(stripped)));
		opts.add(buildConfigOption(new UnqualifiedName("USER"),LiteralExpression.makeStringLiteral(user)));
		opts.add(buildConfigOption(new UnqualifiedName("PASSWORD"), LiteralExpression.makeStringLiteral(password)));
		return PECreateStorageSiteStatement.build(pc,persistentSiteName,opts);
	}

	@SuppressWarnings("unchecked")
	public Statement buildCreatePersistentSite(Name persistentSiteName, String haType,
			Name masterName, List<Name> siteInstances) {
		pc.getPolicyContext().checkRootPermission("create a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess != null)
			throw new SchemaException(Pass.SECOND, "Persistent site "
					+ persistentSiteName.getSQL() + " already exists.");

		PESiteInstance masterPersistentInstance = pc.findSiteInstance(masterName);
		if (masterPersistentInstance == null) {
			throw new SchemaException(Pass.SECOND, "No such persistent instance: "
					+ masterName.getQuoted());
		}

		List<PESiteInstance> peSiteInstances = null;
		if (siteInstances == null)
			peSiteInstances = Collections.EMPTY_LIST;
		else
			peSiteInstances = Functional.apply(siteInstances,
					new UnaryFunction<PESiteInstance, Name>() {

						@Override
						public PESiteInstance evaluate(Name object) {
							PESiteInstance pesi = pc.findSiteInstance(object);
							if (pesi == null)
								throw new SchemaException(Pass.SECOND,
										"No such persistent instance: "
												+ object.getQuoted());
							return pesi;
						}

					});

		return new PECreateStorageSiteStatement(new PEStorageSite(pc, persistentSiteName, haType, masterPersistentInstance,
						peSiteInstances));
	}

	public Statement buildAlterPersistentSite(Name persistentSiteName, String haType) {
		pc.getPolicyContext().checkRootPermission("alter a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess == null)
			throw new SchemaException(Pass.SECOND, "No such persistent site: "
					+ persistentSiteName.getSQL());
		return new PEAlterPersistentSite(pess, haType);
	}

	public Statement buildAlterPersistentSite(Name persistentSiteName, Name masterName) {
		pc.getPolicyContext().checkRootPermission("alter a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess == null)
			throw new SchemaException(Pass.SECOND, "No such persistent site: "
					+ persistentSiteName.getSQL());
		PESiteInstance masterSiteInstance = pc.findSiteInstance(masterName);
		if (masterSiteInstance == null)
			throw new SchemaException(Pass.SECOND, "No such persistent instance: "
					+ masterName.getUnqualified().get());
		return new PEAlterPersistentSite(pess, masterSiteInstance);
	}

	public Statement buildAlterPersistentSite(Name persistentSiteName, Boolean addOperation,
			List<Name> siteInstanceNames) {
		pc.getPolicyContext().checkRootPermission("alter a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess == null)
			throw new SchemaException(Pass.SECOND, "No such persistent site: "
					+ persistentSiteName.getSQL());
		ArrayList<PESiteInstance> siteInstances = new ArrayList<PESiteInstance>();
		for (Name n : siteInstanceNames) {
			PESiteInstance pesi = pc.findSiteInstance(n);
			if (pesi == null)
				throw new SchemaException(Pass.SECOND,
						"No such persistent instance: " + n.getUnqualified().get());
			siteInstances.add(pesi);
		}
		return new PEAlterPersistentSite(pess, addOperation, siteInstances);
	}

	public Statement buildDropPersistentSiteStatement(Name persistentSiteName) {
		pc.getPolicyContext().checkRootPermission("drop a persistent site");
		PEStorageSite pess = pc.findStorageSite(persistentSiteName);
		if (pess == null)
			throw new SchemaException(Pass.SECOND, "No such persistent site: "
					+ persistentSiteName.getSQL());
		PEDropStorageSiteStatement out = new PEDropStorageSiteStatement(pess);
		out.ensureUnreferenced(pc);
		return out;
	}

	@SuppressWarnings("unchecked")
	public Statement buildCreatePersistentGroup(Name groupName, List<Name> sites) {
		pc.getPolicyContext().checkRootPermission("create a persistent group");
		PEPersistentGroup pesg = pc.findStorageGroup(groupName);
		if (pesg != null)
			throw new SchemaException(Pass.SECOND, "Persistent group "
					+ groupName.getSQL() + " already exists");
		List<PEStorageSite> pesites = null;
		if (sites == null)
			pesites = Collections.EMPTY_LIST;
		else
			pesites = Functional.apply(sites,
					new UnaryFunction<PEStorageSite, Name>() {

						@Override
						public PEStorageSite evaluate(Name object) {
							PEStorageSite pess = pc.findStorageSite(object);
							if (pess == null)
								throw new SchemaException(Pass.SECOND,
										"No such persistent site: "
												+ object.getQuoted());
							return pess;
						}

					});
		return new PECreateStatement<PEPersistentGroup, PersistentGroup>(new PEPersistentGroup(pc, groupName, pesites), true,
				PERSISTENT_GROUP_TAG, false);
	}

	public PEColumn lookupInProcessColumn(Name n, boolean missingOk) {
		PEColumn c = scope.lookupInProcessColumn(n);
		if (c == null)
			c = scope.lookupInProcessColumn(n.getCapitalized());
		if (c == null && !missingOk)
			throw new SchemaException(Pass.SECOND, "No such column: "
					+ n.getSQL());
		return c;
	}

	public UnresolvedDistributionVector buildDistributionVector(
			DistributionVector.Model model, List<Name> columnNames,
			Name rangeOrContainer) {
		return new UnresolvedDistributionVector(model,columnNames,rangeOrContainer);
	}

	public ColumnModifier buildColumnModifier(ColumnModifierKind tag) {
		return new ColumnModifier(tag);
	}
	
	public ColumnModifier buildDefaultValue(ExpressionNode v) {
		return new DefaultValueModifier(v);
	}

	public ColumnModifier buildEnumDefaultValue(Type typeDef, ExpressionNode value) {
		final DBEnumType enumValues = (DBEnumType) typeDef;
		final ActualLiteralExpression defaultValue = (ActualLiteralExpression) value;

		/*
		 * Integral default values are treated as 1-based indices into the ENUM.
		 */
		if (defaultValue.isIntegerLiteral()) {
			final int valuePositionIndex = ((Long) defaultValue.getValue()).intValue();
			try {
				final LiteralExpression valueAtIndex = enumValues.getValueAt(valuePositionIndex);
				return new DefaultValueModifier(valueAtIndex);
			} catch (final IndexOutOfBoundsException e) {
				throw new SchemaException(Pass.SECOND, "No value at position " + valuePositionIndex + " in the "
						+ enumValues.getEnumerationTypeName().toUpperCase(), e);
			}
		}
		return new DefaultValueModifier(value);
	}

	public TypeModifier buildCollationSpec(Name collated) {
		return new StringTypeModifier(TypeModifierKind.COLLATE,collated.getUnquotedName().get());
	}

	public TypeModifier buildCharsetSpec(Name spec) {
		return new StringTypeModifier(TypeModifierKind.CHARSET,spec.getUnquotedName().get());
	}

	public ColumnModifier buildOnUpdate() {
		return new ColumnModifier(ColumnModifierKind.ONUPDATE);
//				buildIdentifierLiteral("CURRENT_TIMESTAMP"));
	}

	public Name buildKeywordName(String in) {
		return new UnqualifiedName(in);
	}

	public Name buildIdentifier(String in, Object tree) {
		Name out = new UnqualifiedName(in, SourceLocation.make(tree));
		return out;
	}

	public Name buildIdentifier(Token tok) {
		Name out = new UnqualifiedName(tok.getText(), SourceLocation.make(tok));
		return out;
	}

	public Object buildLiteralValue(String in) {
		return in;
	}

	public Integer buildIntegerLiteral(String in) {
		try {
			return Integer.parseInt(in);
		} catch (NumberFormatException nfe) {
			throw new ParserException(Pass.SECOND, "Bad integer value: '" + in
					+ "': " + nfe.getMessage(), nfe);
		}
	}

	private LiteralExpression asLiteral(ExpressionNode e) {
		if (e instanceof LiteralExpression) {
			return (LiteralExpression) e;
		} else if (e == null) {
			return null;
		} else {
			throw new ParserException(Pass.SECOND, "Expecting literal, got: "
					+ e);
		}
	}

	private Integer asIntegralLiteral(ExpressionNode e) {
		LiteralExpression integLit = asLiteral(e);
		Object lv = null;
		if (integLit instanceof DelegatingLiteralExpression) {
			DelegatingLiteralExpression dle = (DelegatingLiteralExpression) integLit;
			lv = literals.get(dle.getPosition()).getSecond();
		} else {
			lv = integLit.getValue(pc);
		}
		if (lv instanceof Long) {
			return ((Long) lv).intValue();
		}
		throw new ParserException(Pass.SECOND,
				"Expecting integral literal, got: " + lv);
	}

	public SizeTypeAttribute buildSizeTypeAttribute(ExpressionNode a,
			ExpressionNode b) {
		// i.e. (x, y)
		if (b == null)
			return new SizeTypeAttribute(asIntegralLiteral(a));
		Integer precision = asIntegralLiteral(a);
		Integer scale = asIntegralLiteral(b);
		return new FloatSizeTypeAttribute(precision, precision, scale);
	}

	@SuppressWarnings("unchecked")
	public BasicType buildType(List<Name> typeNames, SizeTypeAttribute sizing,
			List<TypeModifier> modifiers) {
		return BasicType.buildType(typeNames, Collections.singletonList(sizing),
				(modifiers == null ? Collections.EMPTY_LIST : modifiers),
				pc.getTypes());
	}

	public BasicType buildEnum(boolean isSet, List<LiteralExpression> values, List<TypeModifier> modifiers) {
		return DBEnumType.make(isSet, values, modifiers,pc.getTypes());
	}

	public TypeModifier buildTypeModifier(TypeModifierKind tmk) {
		return new TypeModifier(tmk);
	}
	
	public TypeModifier buildComparisonModifier(String className) {
        String stripped = Singletons.require(HostService.class).getDBNative().getValueConverter()
				.convertStringLiteral(className);
		return new StringTypeModifier(TypeModifierKind.COMPARISON, stripped);
	}

	public List<TableComponent<?>> buildFieldDefinition(Name fieldName, Type type,
			List<ColumnModifier> attrs, String commentText) throws SchemaException {
		Comment comment = null;
		if (commentText != null) {
			comment = buildTableFieldComment(fieldName, commentText);
		}
		List<ColumnKeyModifier> inlineKeys = new ArrayList<ColumnKeyModifier>();
		for(Iterator<ColumnModifier> iter = attrs.iterator(); iter.hasNext();) {
			ColumnModifier cm = iter.next();
			if (cm.getTag() == ColumnModifierKind.INLINE_KEY) {
				inlineKeys.add((ColumnKeyModifier)cm);
				iter.remove();
			}
		}
		List<TableComponent<?>> out = new ArrayList<TableComponent<?>>();
		PEColumn nc = null;
		if (pc.getCapability() == Capability.PARSING_ONLY
				|| !(pc.getPolicyContext().allowTenantColumnDeclaration() && TenantColumn.TENANT_COLUMN
						.equals(fieldName.get())))
			nc = scope.registerColumn(
					PEColumn.buildColumn(pc, fieldName, type, attrs, comment, inlineKeys));
		else
			nc = scope.registerColumn(new TenantColumn(pc));
		out.add(nc);
		// collapse the case where we see UNIQUE, KEY
		if (inlineKeys.size() > 1) {
			int uniqued = -1;
			int keyed = -1;
			for(int i = 0; i < inlineKeys.size(); i++) {
				if (inlineKeys.get(i).getConstraint() == ConstraintType.UNIQUE && uniqued == -1)
					uniqued = i;
				else if (inlineKeys.get(i).getConstraint() == null && keyed == -1)
					keyed = i;
			}
			if (uniqued > -1 && keyed > uniqued)
				inlineKeys.remove(keyed);
		}
		for(ColumnKeyModifier ckm : inlineKeys) {
			// first build the key
			@SuppressWarnings("unchecked")
			PEKey pek = buildKey(null,null,Collections.singletonList((PEKeyColumnBase)new PEKeyColumn(nc,null,-1L)),Collections.EMPTY_LIST);
			if (ckm.getConstraint() != null)
				pek = withConstraint(ckm.getConstraint(), null, pek);
			out.add(pek);
		}
		return out;
	}

	public PEKey buildKey(IndexType type, Name name, List<PEKeyColumnBase> cols, List<Object> options) throws SchemaException {
		// unpack the options in case we have anything lurking
		IndexType postSpecifiedType = null;
		Comment anyComment = null;
		for(Object o : options) {
			if (o instanceof String) {
				anyComment = buildTableFieldComment(name, (String) o);
			} else if (o instanceof IndexType) {
				postSpecifiedType = (IndexType) o;
			} else {
				throw new SchemaException(Pass.SECOND, "Unknown key option: " + o);
			}
		}
		IndexType actualType = type;
		if (actualType == null) actualType = postSpecifiedType;
		if (actualType == null) actualType = IndexType.BTREE;
		return new PEKey(name,actualType,cols, anyComment);
	}
	
	public PEKey withConstraint(ConstraintType ct, Name symbolName, PEKey pek) {
		pek.setConstraint(ct);
		if (symbolName != null)
			pek.setSymbol(symbolName.getUnqualified());
		return pek;
	}
	
	public ColumnModifier buildInlineKeyModifier(ConstraintType ct) {
		return new ColumnKeyModifier(ct);
	}
	
	public PEKeyColumnBase buildPEKeyColumn(Name identifier, ExpressionNode length, ExpressionNode cardinality) {
		PEColumn c = lookupInProcessColumn(identifier, true);
		Integer keyLength = (length == null ? null : asIntegralLiteral(length));
		long keyCardinality = (cardinality == null ? -1L : asIntegralLiteral(cardinality));
		
		if (c == null)
			return new PEForwardKeyColumn(null,identifier.getUnqualified(), keyLength, keyCardinality);
		else
			return new PEKeyColumn(c,keyLength,keyCardinality);		
	}

	@SuppressWarnings("cast")
	public PEKey buildForeignKey(Name name, List<PEKeyColumnBase> mycols, Name targetTableName, List<UnqualifiedName> targetColumns,
			ForeignKeyAction deleteAction, ForeignKeyAction updateAction) {
		// are unknown tables ok?
		boolean required = (pc.getCapability() != Capability.PARSING_ONLY && 
				KnownVariables.FOREIGN_KEY_CHECKS.getSessionValue(pc.getConnection().getVariableSource()).booleanValue());
		
		// figure out whether the target table is known or not
		PETable targetTab = null;
		Database<?> db = null;
		UnqualifiedName candidateName = null;
		if (targetTableName.isQualified()) {
			QualifiedName qn = (QualifiedName) targetTableName;
			UnqualifiedName ofdb = qn.getNamespace();
			candidateName = qn.getUnqualified();
			db = pc.findPEDatabase(ofdb);
			if (db == null && required)
				throw new SchemaException(Pass.FIRST, "No such database: " + ofdb);
		} else {
			if (pc.getCapability() != Capability.PARSING_ONLY) db = pc.getCurrentDatabase();
			if (db == null && required)
				throw new SchemaException(Pass.FIRST, "No current database");
			candidateName = (UnqualifiedName) targetTableName;
		}
		if (db != null) {
			boolean mtchecks = (pc.getCapability() != Capability.PARSING_ONLY && !pc.getOptions().isDisableMTLookupChecks());
			TableInstance ti = db.getSchema().buildInstance(pc, candidateName, lockInfo, mtchecks);
			if (ti == null && required && mtchecks)
				throw new SchemaException(Pass.FIRST, "No such table: " + targetTableName);
			if (ti != null)
				targetTab = ti.getAbstractTable().asTable();
		}
		Name fullyQualifiedTargetName = null;
		if (targetTab == null) {
			if (targetTableName.isQualified())
				fullyQualifiedTargetName = targetTableName;
			else if (db != null)
				fullyQualifiedTargetName = targetTableName.postfix(db.getName());
			else if (pc.getCapability() == Capability.PARSING_ONLY)
				fullyQualifiedTargetName = targetTableName;
			else
				throw new SchemaException(Pass.FIRST, "No current database");
		}
		// regardless of whether the target table is known, we need to convert the PEKeyColumns to PEForeignKeyColumns
		if (mycols.size() != targetColumns.size())
			throw new SchemaException(Pass.FIRST,"Invalid foreign key declaration: source table names " + mycols.size() + " columns but references " + targetColumns.size());
		List<PEKeyColumnBase> fkcols = new ArrayList<PEKeyColumnBase>();
		for(int i = 0; i < mycols.size(); i++) {
			PEKeyColumnBase mine = mycols.get(i);
			UnqualifiedName targName = targetColumns.get(i);
			PEKeyColumnBase pefk = null;
			if (targetTab == null) {
				if (mine.isUnresolved())
					pefk = new PEForwardForeignKeyColumn(null,mine.getName(),targName);
				else
					pefk = new PEForeignKeyColumn(mine.getColumn(), targName);
			} else {
				PEColumn targCol = targetTab.lookup(pc, targName);
				if (targCol == null)
					throw new SchemaException(Pass.SECOND, "No such column " + targName + " in " + targetTableName);
				if (mine.isUnresolved()) 
					pefk = new PEForwardForeignKeyColumn(null,mine.getName(),targCol);
				else
					pefk = new PEForeignKeyColumn(mine.getColumn(), targCol);
			}
			fkcols.add(pefk);
		}
		return new PEForeignKey(pc,
				name,
				targetTab,fullyQualifiedTargetName,fkcols,
				updateAction == null ? ForeignKeyAction.RESTRICT : updateAction,
				deleteAction == null ? ForeignKeyAction.RESTRICT : deleteAction);
	}
	
	public PETable buildTable(Name tableName,
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv,
			PEPersistentGroup sg, List<TableModifier> modifiers,
			boolean nascent, 
			boolean temporary) {
		if ( tableName == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		Database<?> cdb = null;
		Name unqualifiedTableName = tableName;
		if (pc.getCapability() != Capability.PARSING_ONLY) {
			cdb = pc.getCurrentDatabase(false);
			if (cdb == null || tableName.isQualified()) {
				if (cdb == null && !tableName.isQualified())
					throw new SchemaException(Pass.SECOND,
							"Current database not set");
				QualifiedName qname = (QualifiedName) tableName;
				UnqualifiedName leading = qname.getNamespace();
				if (cdb == null || !cdb.getName().equals(leading)) {
					cdb = pc.findDatabase(leading);
					if (cdb == null)
						throw new SchemaException(Pass.SECOND,
								"No such database: '" + leading + "'");
				}
				unqualifiedTableName = tableName.getUnqualified();
			}
			if (!(cdb instanceof PEDatabase))
				throw new SchemaException(Pass.SECOND,
						"Invalid database for table creation: '"
								+ cdb.getName() + "'");
		}
		// the dv should have the container here, if applicable
		PETable newtab = null;
		if (nascent || temporary) {
			ComplexPETable ctab =
					new ComplexPETable(pc, unqualifiedTableName, fieldsAndKeys,
							dv, modifiers, sg, (PEDatabase) cdb, TableState.SHARED);
			if (nascent)
				ctab.withCTA();
			if (temporary)
				ctab.withTemporaryTable(pc);
			newtab = ctab;
		}
		else
			newtab = new PETable(pc, unqualifiedTableName, fieldsAndKeys,
				dv, modifiers, sg, (PEDatabase) cdb, TableState.SHARED);
		if (pc.getCapability() != Capability.PARSING_ONLY) {
			pc.getPolicyContext().modifyTablePart(newtab);
		}
		newtab.setDeclaration(pc,newtab);
		return newtab;
	}

	public ExpressionNode buildBooleanLiteral(Boolean v, Token orig) {
		if (opts.isActualLiterals()) {
			return new ActualLiteralExpression(v,orig.getType(),SourceLocation.make(orig), null);
		} else {
			DelegatingLiteralExpression litex= new DelegatingLiteralExpression(TokenTypes.BOOLEAN,
					SourceLocation.make(orig),this,literals.size(),null);
			literals.add(litex,v);
			return litex;
		}
	}

	public ExpressionNode buildColumnReference(Name parsedName) {
		if (resolveColumnsAsIdentifiers)
			return buildIdentifierLiteral(parsedName);
		if (opts.isResolve())
			return scope.buildColumnInstance(pc,parsedName);
		return new ColumnInstance(parsedName, null, null);
	}

	protected ExpressionNode buildLiteral(Token o) {
		String t = o.getText();
		int tok = o.getType();
		if ((tok == TokenTypes.NULL) || (tok == TokenTypes.GLOBAL))			
			return new ActualLiteralExpression(t,tok, SourceLocation.make(o),null);
		// if it's a string literal, strip off any charset hint for later
		UnqualifiedName charsetHint = null;
		if (tok == TokenTypes.Character_String_Literal && t.length() > 0) {
			if (t.charAt(0) == '_') {
				int firstQuote = t.indexOf("'");
				if (firstQuote == -1) firstQuote = t.indexOf("\"");
				String hint = t.substring(0,firstQuote);
				charsetHint = new UnqualifiedName(hint,false);
				t = t.substring(firstQuote);
			}
			
			if ((t.length() > 2) && 
					(t.startsWith("\"") && (t.endsWith("\"")))) {
				t = PEStringUtils.escapeSingleQuoteIfNecessary(t);
			}
		}
		if (PEStringUtils.isHexNumber(t)) {
			tok = TokenTypes.Hex_String_Literal;
		}
		ExpressionNode ex = null;
		if (opts.isActualLiterals() || (pc.getCapability() != Capability.PARSING_ONLY && pc.isMutableSource()))
            ex = new ActualLiteralExpression(ValueConverter.INSTANCE.convertLiteral(t,tok),
            		tok, SourceLocation.make(o),charsetHint);
		else {
			DelegatingLiteralExpression litex = new DelegatingLiteralExpression(tok, SourceLocation.make(o),this,literals.size(), charsetHint);
            literals.add(litex, ValueConverter.INSTANCE.convertLiteral(t,tok)); 
			ex = litex;
		}
		return ex;
	}
		
	public ExpressionNode buildIdentifierLiteral(String ident) {
		return buildIdentifierLiteral(new UnqualifiedName(ident));
	}

	public ExpressionNode buildIdentifierLiteral(Name ident) {
		return new IdentifierLiteralExpression(ident);
	}

	public ExpressionNode buildNullLiteral() {
		return LiteralExpression.makeNullLiteral();
	}

	public ExpressionNode buildFunctionCall(FunctionName givenName,
			List<ExpressionNode> params, SetQuantifier sq, Object tree) {
		FunctionName unqualifiedName = givenName;
		if (pc.getCapability() != Capability.PARSING_ONLY) {
			if (unqualifiedName.isPipes()) {
				forceUncacheable(ValueManager.CacheStatus.NOCACHE_DYNAMIC_FUNCTION);
				SQLMode mode = 
						KnownVariables.SQL_MODE.getSessionValue(pc.getConnection().getVariableSource()); 
				if (mode.isPipesAsConcat()) {
					// rewrite to use the concat function call
					unqualifiedName = new FunctionName("CONCAT",-1,false);
				} else {
					// rewrite using or
					unqualifiedName = FunctionName.makeOr();
				}				
			} else if (unqualifiedName.isDoubleAmpersand()) {
				unqualifiedName = FunctionName.makeAnd();
			}
		}
		@SuppressWarnings("unchecked")
		FunctionCall fc = new FunctionCall(unqualifiedName,
				(params == null ? Collections.EMPTY_LIST : params), sq,
				SourceLocation.make(tree));
		if (EngineConstant.FUNCTION.has(fc, EngineConstant.DATABASE)) {
			forceUncacheable(ValueManager.CacheStatus.NOCACHE_DYNAMIC_FUNCTION);
			Database<?> peds = pc.getCurrentDatabase(false);
			if (peds == null)
				return LiteralExpression.makeNullLiteral();
			return LiteralExpression.makeStringLiteral(peds.getName().get());
		} else if (EngineConstant.FUNCTION.has(fc, EngineConstant.CURRENT_USER)) {
			forceUncacheable(ValueManager.CacheStatus.NOCACHE_DYNAMIC_FUNCTION);
			PEUser peu = pc.getCurrentUser().get(pc);
			if (peu == null)
				return LiteralExpression.makeNullLiteral();
			// not entirely correct
			return LiteralExpression.makeStringLiteral(peu.getUserScope().getSQL());
		} else if (EngineConstant.FUNCTION.has(fc, EngineConstant.UUID)) {
			forceUncacheable(ValueManager.CacheStatus.NOCACHE_DYNAMIC_FUNCTION);
			// rewrite the function with a literal
            UUID uuid = Singletons.require(HostService.class).getUuidGenerator().generate();
			return LiteralExpression
					.makeStringLiteral(uuid.toString());
		} else if (EngineConstant.FUNCTION.has(fc, EngineConstant.LAST_INSERT_ID)) {
			forceUncacheable(ValueManager.CacheStatus.NOCACHE_DYNAMIC_FUNCTION);
			return LiteralExpression
					.makeLongLiteral(pc.getConnection().getLastInsertedId());
		} else if (EngineConstant.FUNCTION.has(fc, EngineConstant.RAND)) {
			return buildRandCall(fc.getParameters());
		}

		if (unqualifiedName.isAggregate() || isFunctionThatRequiresSetTimestampVariable(fc.getFunctionName()
				.getUnqualified().get(), params)) {
			scope.registerFunction(fc);
		}

		return fc;
	}

	private boolean isFunctionThatRequiresSetTimestampVariable(String name,
			List<ExpressionNode> params) {
		boolean ret = false;

		if (TimestampVariableUtils.isTimestampFunction(name)) {
			// unix_timestamp also takes a parameter
			// so we only need to set the timestamp variable if there are no
			// parameter(s)
			if (TimestampVariableUtils.isFunctionUnixTimestamp(name)) {
				if (params != null && params.size() > 0) {
					ret = false;
				} else {
					ret = true;
				}
			} else {
				ret = true;
			}
		}

		return ret;
	}

	private UnqualifiedName castAlias(Name alias) {
		if (alias == null)
			return null;
		if (alias instanceof UnqualifiedName)
			return (UnqualifiedName) alias;
		throw new ParserException(Pass.SECOND,
				"Use of qualified identifier for alias: '" + alias.getSQL()
						+ "'");
	}

	public ExpressionNode buildTableInstance(Name name, Name alias) {
		return buildTableInstance(name,alias,null);
	}
	
	public ExpressionNode buildTableInstance(Name name, Name alias, List<IndexHint> hints) {
		if ( name == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		TableInstance ti = null;
		if (opts.isResolve()) {
			ti = scope.buildTableInstance(
					name,
					castAlias(alias),
					pc,
					lockInfo);
		} else {
			ti = new TableInstance(null, name, castAlias(alias), 0, opts.isResolve());
		}
		if (hints != null)
			ti.setHints(hints);
		return ti;
	}

	public IndexHint buildIndexHint(HintType type, boolean isKey, HintTarget targ, List<UnqualifiedName> indexNames) {
		return new IndexHint(type, isKey, targ, indexNames);
	}
	
	public ExpressionNode buildWildcard(Token orig) {
		return new Wildcard(SourceLocation.make(orig));
	}

	public ExpressionNode buildQuestionMark(CommonTree orig) {
		return new Parameter(SourceLocation.make(orig));
	}

	public LimitSpecification buildLimitSpecification(ExpressionNode rowcount,
			ExpressionNode offset) {
		return new LimitSpecification(rowcount, offset);
	}

	/**
	 * @param a
	 * @param asc
	 * @param desc
	 * @return
	 */
	public SortingSpecification buildSortingSpecification(ExpressionNode a,
			Object asc, Object desc) {
		// default is asc, so as long as desc is non-null it is descending
		boolean direction = (desc == null);
		return new SortingSpecification(a, direction);
	}

	public FunctionName buildOperatorName(CommonTree ct) {
		return new FunctionName(ct.getText(), ct.getType(), true);
	}

	public FunctionName buildOperatorName(Token t) {
		return new FunctionName(t.getText(), t.getType(), true);
	}

	public FunctionName buildFunctionName(String v, int tokenKind) {
		return new FunctionName(v, tokenKind, false);
	}

	public FunctionName buildFunctionName(Token tok, boolean op) {
		return new FunctionName(tok.getText(), tok.getType(), op,
				SourceLocation.make(tok));
	}

	public JoinSpecification buildJoinType(String primary) {
		return buildJoinType(primary, null, null);
	}

	public JoinSpecification buildJoinType(String primary, String natural, String outer) {
		StringBuilder buf = new StringBuilder();
		if (natural != null)
			buf.append(natural).append(" ");
		if (primary != null)
			buf.append(primary).append(" ");
		if (outer != null)
			buf.append(outer);
		return JoinSpecification.makeJoinSpecification(buf.toString()
				.toUpperCase());
	}

	public JoinClauseType buildJoinClauseType(ExpressionNode node, List<Name> namedCols) {
		return new JoinClauseType(node, namedCols, ((namedCols != null) ? ClauseType.USING : ClauseType.ON));
	}

	public JoinedTable buildJoinedTable(ExpressionNode target,
			JoinClauseType joinOn, JoinSpecification injoinType) {
		// JOIN is INNER JOIN
		JoinSpecification joinType = (injoinType == null ? buildJoinType("INNER") : injoinType);
		return new JoinedTable(target, (joinOn == null ? null : joinOn.getNode()), joinType, (joinOn == null ? null : joinOn.getColumnIdentifiers()));
	}

	public TableJoin buildTableJoin(ExpressionNode factor, List<JoinedTable> joins) {
		return new TableJoin(factor,joins);
	}
	
	public FromTableReference buildFromTableReference(ExpressionNode factor,
			List<JoinedTable> joins) {
		
		if (joins.isEmpty()) {
			return new FromTableReference(factor);
		}

		convertUsingColSpecToOnSpec(factor, joins);
		
		return new FromTableReference(new TableJoin(factor,joins));		
	}

	private void convertUsingColSpecToOnSpec(ExpressionNode factor, List<JoinedTable> joins) {
		if (!opts.isResolve()) return;
		List<UnqualifiedName> visibleAliases = new LinkedList<UnqualifiedName>();
		if (factor instanceof TableInstance)
			visibleAliases.add(0, ((TableInstance) factor).getReferenceName(pc).getUnqualified());
		for (JoinedTable jt : joins) {
			List<Name> usingColSpec = jt.getUsingColSpec();
			if (usingColSpec != null && usingColSpec.size() > 0 ) {
				ExpressionNode jtTable = jt.getJoinedTo();

				visibleAliases.add(0, ((TableInstance)jtTable).getReferenceName(pc).getUnqualified());
				
				// turn USING column spec to equivalent ON t1.fieldn = t2.fieldn 
				List<Name> unqparts = new ArrayList<Name>();
				List<ExpressionNode> params = new ArrayList<ExpressionNode>();
				List<ExpressionNode> equals = new ArrayList<ExpressionNode>();
				
				// build list of t1.fieldn = t2.fieldn function(s) first
				List<ExpressionNode> functions = new ArrayList<ExpressionNode>();
				for(Name name : usingColSpec) {
					params.clear();
					
					// try to find the right table to hang the thing on
					int foff = -1;
					SchemaException any = null;
					int offset = -1;
					for(UnqualifiedName un : visibleAliases) {
						offset++;
						if (params.size() == 2)
							break;
						unqparts.clear();
						unqparts.add(un);
						unqparts.add(name.getUnqualified());
						try {
							ExpressionNode en = buildColumnReference(buildQualifiedName(unqparts));
							if (params.isEmpty()) {
								params.add(en);
								foff = offset;
							} else if (foff > offset)
								params.add(en);
							else params.add(0,en);
						} catch (SchemaException se) {
							any = se;
						}
					}
					if (params.size() != 2) {
						if (any != null) throw any;
						throw new SchemaException(Pass.SECOND, "Failed to resolve using clause");
					}
					
					equals.add(buildFunctionCall(FunctionName.makeEquals(), params, null, null));
				}
				
				if (equals.size() > 0) {
					// build function(s) of AND'ed t1.fieldn = t2.fieldn
					functions.clear();
					ExpressionNode current = null;
					ExpressionNode last = null;
					for(int i=0; i < equals.size(); ++i) {
						current = equals.get(i);
						if (i > 0) {
							params.clear();
							params.add(last);
							params.add(current);
							current = buildFunctionCall(FunctionName.makeAnd(), params, null, null);
						}
						last = current;
					}
					// just set using the last function
					Edge<?, ExpressionNode> edge = jt.getJoinOnEdge();
					edge.add(last);

					// clear the using column spec
					// usingColSpec.clear();
				}
			}
		}
	}

	public Name buildQualifiedName(List<Name> parts) {
		if (parts.size() == 1)
			return parts.get(0);
		ArrayList<UnqualifiedName> unqparts = new ArrayList<UnqualifiedName>();
		for (Name n : parts) {
			if (n == null) 
				continue;
			unqparts.addAll(n.getParts());
		}
		Name out = new QualifiedName(unqparts);
		// verify that none of the parts (other than the last), is an asterisk
		for (Iterator<UnqualifiedName> iter = unqparts.iterator(); iter
				.hasNext();) {
			UnqualifiedName un = iter.next();
			if (un.isAsterisk()) {
				if (iter.hasNext())
					throw new SchemaException(Pass.FIRST,
							"Invalid qualified name: " + out.getSQL());
			}
		}
		return out;
	}

	public Name buildNameFromStringLiteral(Object in) {
		CommonTree tree = (CommonTree) in;
		return new UnqualifiedName(PEStringUtils.dequote(tree.getText()));
	}

	public Name buildName(Token tok) {
		return new UnqualifiedName(tok.getText(), SourceLocation.make(tok));
	}

	public Name buildName(Object in) {
		CommonTree tree = (CommonTree) in;
		return new UnqualifiedName(tree.getText(), SourceLocation.make(tree));
	}

	public Name buildName(String in) {
		return new UnqualifiedName(in, null);
	}

	public ExpressionNode addGrouping(ExpressionNode in) {
		in.setGrouped();
		return in;
	}
	
	public ProjectingStatement addGrouping(ProjectingStatement in) {
		in.setGrouped(true);
		return in;
	}

	// only 1 of the first four args will be non null.
	/**
	 * @param plusSign
	 * @param minusSign
	 * @param binary
	 * @param tilde
	 * @param in
	 * @param collate
	 * @param collation
	 * @return
	 */
	public ExpressionNode applyExprFlags(Token plusSign, Token minusSign, Token binary, Token tilde, ExpressionNode in, Token collate, Name collation) {
		ExpressionNode out = in;
		if (collation != null) {
			// collation is an operator - it has higher precedence than the other stuff; hack it in
			FunctionName fn = new FunctionName(collate.getText(), collate.getType(),true);
			FunctionCall fc = new FunctionCall(fn,out,new IdentifierLiteralExpression(collation));
			out = fc;
		}
		if (minusSign != null) {
			out.setNegated();
		} else if (tilde != null) {
			out.setBitNegated();
		} else if (binary != null) {
			// this is a function call
			FunctionName fn = new FunctionName(binary.getText(),binary.getType(),true);
			FunctionCall fc = new FunctionCall(fn,out);
			out = fc;
		}
		return out;
	}
	
	public ExpressionNode buildDefaultKeywordExpr(Token orig) {
		return new Default(SourceLocation.make(orig));
	}

	public ExpressionNode buildUpdateExpression(FunctionName eqop,
			Name columnRef, ExpressionNode expr, Object in) {
		CommonTree tree = (CommonTree) in;
		// this is just a function call, but we have to wrap the column ref
		ArrayList<ExpressionNode> params = new ArrayList<ExpressionNode>();
		params.add(buildColumnReference(columnRef));
		params.add(expr);
		return buildFunctionCall(eqop, params, null, tree);
	}

	public VariableInstance buildLHSVariableInstance(VariableScope vs, Name n, Object tree) {
		return captureVariable(new VariableInstance(n.getUnqualified(), vs, SourceLocation.make(tree), false));
	}
	
	public VariableInstance buildRHSVariableInstance(Object fat, Object sat, VariableScopeKind vsk, Name n, Object tree) {
		VariableScopeKind kind = vsk;
		if (kind == null) {
			if (fat != null) {
				if (sat != null) {
					/*
					 * MySQL returns the session value if it exists and the
					 * global value otherwise.
					 */
					final String varName = n.getUnqualified().getUnquotedName().get();
					final VariableHandler<?> exists =
							Singletons.require(HostService.class).getVariableManager().lookup(varName);
					if (exists != null) {
						if (exists.getScopes().contains(VariableScopeKind.SESSION)) {
							kind = VariableScopeKind.SESSION;
						} else {
							kind = VariableScopeKind.GLOBAL;
						}
					}
				} else {
					kind = VariableScopeKind.USER;
				}
			} else {
				kind = VariableScopeKind.SESSION;
			}
		}
		return captureVariable(new VariableInstance(n.getUnqualified(), new VariableScope(kind), SourceLocation.make(tree), true));
	}
	
	private VariableInstance captureVariable(VariableInstance vi) {
		if (!scope.isEmpty())
			scope.getVariables().add(vi);
		return vi;
	}
	
	public VariableScope buildVariableScope(VariableScopeKind vsk) {
		return new VariableScope(vsk);
	}
	
	public VariableScope buildVariableScope(Name scoped) {
		if (scoped == null)
			return new VariableScope(VariableScopeKind.SCOPED);		
		return new VariableScope(scoped.getUnquotedName().get());
	}
	
	public SetExpression buildSetVariableExpression(VariableInstance v, ExpressionNode en) {
		return buildSetVariableExpression(v,Collections.singletonList(en));
	}
	
	public SetExpression buildSetVariableExpression(VariableInstance v,
			List<ExpressionNode> l) {

		if (StringUtils.endsWithIgnoreCase(v.getVariableName().get(), "NAMES")) {
			// validate NAME variable is one of our supported ones
			// should only be one item in l
			if (l.get(0) instanceof LiteralExpression) {
				LiteralExpression le = (LiteralExpression) l.get(0);
				if (le.isNullLiteral()) {
					throw new SchemaException(Pass.FIRST, "Must specify a character set");
				}
				String value = (String)le.getValue(pc);
				try {
                    if (Singletons.require(HostService.class).getCharSetNative().getCharSetCatalog().findCharSetByName(value, false) == null) {
						// character set not supported
						throw new SchemaException(Pass.FIRST, "Cannot set an unsupported character set: " + value);
					}
				} catch (PEException e) {
					throw new SchemaException(Pass.FIRST, "Unable to validate character set: " + value);
				}
			}
		}
		

		if (l.size() == 1)
			return new SetVariableExpression(v, l.get(0));
		return new SetVariableExpression(v, l);
	}

	public SetExpression buildSetTransactionIsolation(
			SetTransactionIsolationExpression.IsolationLevel il, VariableScopeKind scopeKind) {
		return new SetTransactionIsolationExpression(il, scopeKind);
	}

	public SessionSetVariableStatement buildSessionSetVarStatement(
			List<SetExpression> l) {
		return new SessionSetVariableStatement(l);
	}

	public SessionSetVariableStatement buildAlterPersistentVariable(
			VariableInstance vi, ExpressionNode expr) {
		ArrayList<SetExpression> sets = new ArrayList<SetExpression>();
		sets.add(new SetVariableExpression(vi, expr));
		return new SessionSetVariableStatement(sets);
	}

	public Statement buildAddVariable(Name newName, List<Pair<Name,LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("create a new system variable");
		String varName = newName.getUnqualified().getUnquotedName().get();
		VariableHandler exists =
				Singletons.require(HostService.class).getVariableManager().lookup(varName);
		if (exists != null)
			throw new SchemaException(Pass.SECOND,"Variable " + newName + " already exists");
		return AddGlobalVariableStatement.decode(pc, varName, options);
	}
	
	private Comment buildComment(String c) {
		return new Comment(PEStringUtils.dequote(c));
	}

	public TableModifier buildTableModifier(List<ExpressionNode> l) {
		return new UnknownTableModifier(l);
	}

	public TableModifier buildTableModifier(ExpressionNode lhs,
			boolean hasEquals, ExpressionNode rhs) {
		ArrayList<ExpressionNode> out = new ArrayList<ExpressionNode>();
		if (hasEquals) {
			out.add(buildFunctionCall(FunctionName.makeEquals(), Arrays.asList(new ExpressionNode[] { lhs, rhs }), null, null));
		} else {
			out.add(lhs);
			out.add(rhs);
		}
		return new UnknownTableModifier(out);
	}

	public TableModifier buildAutoincTableModifier(ExpressionNode litval) {
		LiteralExpression lit = (LiteralExpression) litval;
		Object value = lit.getValue(pc);
		if (value instanceof Number) {
			Number n = (Number)value;
			return new AutoincTableModifier(n.longValue());
		}
		throw new SchemaException(Pass.SECOND, "Unknown autoinc value kind: " + value.getClass().getSimpleName());
	}

	public TableModifier buildEngineTableModifier(Name name) {
		EngineTableModifier.EngineTag actualTag = EngineTableModifier.EngineTag.findEngine(name.getUnqualified().getUnquotedName().get());
		if (actualTag == null)
			actualTag = EngineTableModifier.EngineTag.INNODB;
		return new EngineTableModifier(actualTag);
	}
	
	public TableModifier buildCommentTableModifier(final Name tableName, final String s) throws SchemaException {
		final long maxAllowedLength = Singletons.require(HostService.class).getDBNative().getMaxTableCommentLength();
		if (s.length() > maxAllowedLength) {
			throw new SchemaException(new ErrorInfo(DVEErrors.TOO_LONG_TABLE_COMMENT, tableName.getUnqualified().getUnquotedName().get(), maxAllowedLength));
		}

		return new CommentTableModifier(buildComment(s));
	}
	
	public Comment buildTableFieldComment(final Name fieldName, final String s) throws SchemaException {
		final long maxAllowedLength = Singletons.require(HostService.class).getDBNative().getMaxTableFieldCommentLength();
		if (s.length() > maxAllowedLength) {
			throw new SchemaException(new ErrorInfo(DVEErrors.TOO_LONG_TABLE_FIELD_COMMENT, fieldName.getUnquotedName().get(), maxAllowedLength));
		}

		return buildComment(s);
	}

	public TableModifier buildCollationTableModifier(Name collationName) {
		return new CollationTableModifier(collationName.getUnqualified());
	}
	
	public TableModifier buildCharsetTableModifier(Name charsetName) {
		return new CharsetTableModifier(charsetName.getUnqualified());
	}
	
	public TableModifier buildRowFormatTableModifier(Name n) {
		return new RowFormatTableModifier(n.getUnqualified());
	}
	
	public TableModifier buildMaxRowsModifier(ExpressionNode en) {
		LiteralExpression litex = asLiteral(en);
		Object v = litex.getValue(pc);
		if (v instanceof Number) {
			Number n = (Number) v;
			return new MaxRowsModifier(n.longValue());
		}
		throw new SchemaException(Pass.FIRST, "Invalid max rows value - not a number");
	}
	
	public TableModifier buildChecksumModifier(ExpressionNode en) {
		Integer v = asIntegralLiteral(en);
		return new ChecksumModifier(v.intValue());
	}
	
	public TableModifier buildDelayKeyWriteTableModifier(ExpressionNode en) {
		throw new SchemaException(Pass.FIRST, "No support for DELAY_KEY_WRITE table option");
	}

	public Statement buildCommitTransactionStatement() {
		return new TransactionStatement(TransactionStatement.Kind.COMMIT);
	}

	public Statement buildStartTransactionStatement(boolean snapshot) {
		return new StartTransactionStatement(snapshot);
	}

	public Statement buildSavepointTransactionStatement(Name n) {
		return new SavepointStatement(n, false);
	}

	public Statement buildReleaseSavepointTransactionStatement(Name n) {
		return new SavepointStatement(n, true);
	}

	public Statement buildRollbackTransactionStatement(Name n) {
		return new RollbackTransactionStatement(n);
	}

	public Statement buildTruncateStatement(Name name, Object tree) {
		final TableInstance ti = (TableInstance) buildTableInstance(name, null);
		if (ti.getAbstractTable().isTable()) {
			final TruncateStatement ts = new TruncateStatement(ti, SourceLocation.make(tree));
			ts.getDerivedInfo().takeScope(scope);
			return ts;
		}
		UnqualifiedName encName = null;
		UnqualifiedName tname = null;
		if (name.isQualified()) {
			QualifiedName qn = (QualifiedName) name;
			encName = qn.getNamespace();
			tname = qn.getUnqualified();
		} else {
			tname = (UnqualifiedName) name;
			encName = pc.getCurrentDatabase().getName().getUnqualified();
		}
			
		throw new SchemaException(new ErrorInfo(DVEErrors.TABLE_DNE,encName.getUnquotedName().get(),tname.getUnquotedName().get()));
	}

	public ExpressionNode buildSubquery(Statement in, Name alias, Object orig, boolean makeVirtualTable) {
		ProjectingStatement dmls = (ProjectingStatement) in;
		Subquery sq = new Subquery(dmls, alias,SourceLocation.make(orig));
		if (makeVirtualTable) {
			SubqueryTable sqt = SubqueryTable.make(pc,dmls);
			sq.setTable(sqt);
			scope.pushVirtualTable(sqt, alias.getUnqualified(), pc);
			scope.getNestedQueries().add(dmls);
		}
		
		// if an alias was specified, then enter a fake table into the scope.
		return sq;
		
	}

	public ExpressionNode buildExists(Token exists, Statement ss, Object orig) {
		ExpressionNode param = buildSubquery(ss,null,orig,false);
		return buildFunctionCall(buildFunctionName(exists,false),Collections.singletonList(param),null,null);
	}
	
	public Statement buildCreateRangeStatement(Name rangeName,
			Name persistentGroup, List<BasicType> types, Boolean ifNotExists) {
		pc.getPolicyContext().checkRootPermission("create a range");
		RangeDistribution rd = pc.findRange(rangeName, persistentGroup);
		if (rd != null && !getOptions().isAllowDuplicates()) {
			if (Boolean.TRUE.equals(ifNotExists)) {
				return buildEmptyStatement("Create an existing range.");
			}
			throw new SchemaException(Pass.SECOND, "Range "
					+ rangeName.getSQL() + " already exists.");
		}
		PEPersistentGroup onGroup = pc.findStorageGroup(persistentGroup);
		if (onGroup == null)
			throw new SchemaException(Pass.SECOND, "No such persistent group: "
					+ persistentGroup.getSQL());
		RangeDistribution nrd = new RangeDistribution(pc, rangeName, types, onGroup);
		if (rd != null) try {
			// it is an error if the redeclaration doesn't match the original
			rd.verifySame(pc,nrd);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND,pe);
		}
		return new PECreateStatement<RangeDistribution, DistributionRange>((rd == null ? nrd : rd), true, "RANGE", rd != null);
	}

	public Statement buildDropRangeStatement(Name rangeName, Boolean ifExists, Name storageGroup) {
		pc.getPolicyContext().checkRootPermission("drop a range");
		RangeDistribution rd = null;
		if (storageGroup == null) {
			List<DistributionRange> allsuch = pc.getCatalog().findDistributionRange(rangeName.getUnquotedName().get());
			if (allsuch.size() > 1) 
				throw new SchemaException(Pass.SECOND, "More than one range named " + rangeName + " please specify group (add persistent group <name>)");
			else if (allsuch.isEmpty()) {
				if (Boolean.TRUE.equals(ifExists)) {
					return buildEmptyStatement("Drop a non existing range.");					
				}
				throw new SchemaException(Pass.SECOND, "No such range: "
						+ rangeName.getSQL());				
			}
			rd = pc.findRange(rangeName, new UnqualifiedName(allsuch.get(0).getStorageGroup().getName()));
		} else {
			rd = pc.findRange(rangeName, storageGroup);
			if (rd == null) {
				if (Boolean.TRUE.equals(ifExists)) {
					return buildEmptyStatement("Drop a non existing range.");
				}
				throw new SchemaException(Pass.SECOND, "No such range: "
						+ rangeName.getSQL());
			}
		}
		PEDropRangeStatement out = new PEDropRangeStatement(rd);
		out.ensureUnreferenced(pc);
		return out;
	}

	public Statement buildDropPersistentGroupStatement(Name groupName) {
		pc.getPolicyContext().checkRootPermission("drop a persistent group");
		PEPersistentGroup peg = pc.findStorageGroup(groupName);
		if (peg == null)
			throw new SchemaException(Pass.SECOND, "No such persistent group: "
					+ groupName.getSQL());
		PEDropStorageGroupStatement out = new PEDropStorageGroupStatement(peg);
		out.ensureUnreferenced(pc);
		return out;
	}

	public Statement buildAddPersistentSiteStatement(Name sgName, List<Name> siteNames, boolean rebalance) {
		pc.getPolicyContext().checkRootPermission("add a persistent site");
		PEPersistentGroup pesg = pc.findStorageGroup(sgName);
		if (pesg == null)
			throw new SchemaException(Pass.SECOND, "No such persistent group: "
					+ sgName.getSQL());
		ArrayList<PEStorageSite> sites = new ArrayList<PEStorageSite>();
		for (Name n : siteNames) {
			PEStorageSite pess = pc.findStorageSite(n);
			if (pess == null)
				throw new SchemaException(Pass.SECOND, "No such persistent site: "
						+ n.getSQL());
			sites.add(pess);
		}
		return new AddStorageSiteStatement(pesg, sites, rebalance);
	}

	public Statement buildShowSingularQuery(String onInfoSchemaTable,
			Name objectName) {
		if (pc.getCapability() == Capability.PARSING_ONLY && !pc.getCatalog().isPersistent())
			return new EmptyStatement("no catalog queries with transient execution engine");

		// TODO:
		// handle !resolve
        ShowSchemaBehavior ist = Singletons.require(HostService.class).getInformationSchema()
				.lookupShowTable(new UnqualifiedName(onInfoSchemaTable));
		if (ist == null)
			throw new MigrationException("Need to add info schema table for "
					+ onInfoSchemaTable);
		return ist.buildUniqueStatement(pc, objectName, new ShowOptions());
	}

	public void push_info_schema_scope(Name n) {
		// parsing only - just add the darn thing
		if (pc.getCapability() == Capability.PARSING_ONLY) {
			pushScope();
			scope.buildTableInstance(n, new UnqualifiedName("a"), null, pc, null);
		} else {
			ShowSchemaBehavior sst = Singletons.require(HostService.class).getInformationSchema()
					.lookupShowTable((UnqualifiedName) n);
			if (sst == null)
				throw new SchemaException(Pass.SECOND, "No such table: "					
						+ n.getSQL());
			InformationSchemaTable ist = (InformationSchemaTable) sst;
			pushScope();
			// we put in an alias anyways
			scope.buildTableInstance(ist.getName(), new UnqualifiedName("a"),
					Singletons.require(HostService.class).getInformationSchema().getShowSchema(), pc, null);
		}
	}

	public void push_info_schema_scope(String s) {
		push_info_schema_scope(buildName(s));
	}

	public Statement buildShowPluralQuery(String onInfoSchemaTable,
			List<Name> scoping, Pair<ExpressionNode, ExpressionNode> likeOrWhere) {
		return buildShowPluralQuery(onInfoSchemaTable, scoping, likeOrWhere, null);
	}

	public Statement buildShowPluralQuery(String onInfoSchemaTable,
			List<Name> scoping, Pair<ExpressionNode, ExpressionNode> likeOrWhere, Token full) {
		if (pc.getCapability() == Capability.PARSING_ONLY && !pc.getCatalog().isPersistent())
			return new EmptyStatement("no catalog queries with transient execution engine");

        ShowSchemaBehavior ist = Singletons.require(HostService.class).getInformationSchema()
				.lookupShowTable(new UnqualifiedName(onInfoSchemaTable));
		if (ist == null)
			throw new MigrationException("Need to add info schema table for "
					+ onInfoSchemaTable);
		ExpressionNode likeExpr = (likeOrWhere == null ? null : likeOrWhere
				.getFirst());
		ExpressionNode whereExpr = (likeOrWhere == null ? null : likeOrWhere
				.getSecond());
		
		ShowOptions so = new ShowOptions();
		if (full != null) so.withFull();
		
		// break up the scoping value into parts if qualified
		List<Name> scopingParts = new ArrayList<Name>();
		if (scoping != null) {
			for (Name name : scoping) {
				if (name instanceof QualifiedName) {
					QualifiedName qn = (QualifiedName)name;
					scopingParts.add(qn.getParts().get(1)); // table name
					scopingParts.add(qn.getParts().get(0)); // database
				} else {
					// assumed UnqualifiedName
					scopingParts.add(name);
				}
			}
		}
		return ist.buildShowPlural(pc, scopingParts, likeExpr, whereExpr, so);	
	}

	public Statement buildReloadLogging() {
		return new SessionStatement("RELOAD LOGGING") {
			@Override
			public boolean isPassthrough() {
				return false;
			}

			@Override
			public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
				es.append(new TransientSessionExecutionStep("RELOAD LOGGING", new AdhocOperation() {


							@Override
							public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
									throws Throwable {
								try {
									// Servers should have been started with a log4j system property
									// e.g. -Dlog4j.configuration=<something>log4j.properties 
									
									String name = System.getProperty("log4j.configuration");
									if(StringUtils.isBlank(name))
										throw new PEException("log4j.configuration property not set");
									
									URL resource = TranslatorUtils.class.getClassLoader().getResource(name);
									if(resource == null) 
										throw new PEException("Couldn't locate file '" + name + "' on the classpath");
									
									PropertyConfigurator.configure(resource);
									
									logger.info("log4j configuration reloaded from '" + name + "'");
								} catch (Exception e) {
									logger.error("Unable to reload logging configuation: " + e.getMessage(), e);
									
									throw new PEException("Unable to reload logging configuration: " + e.getMessage(),e);
								}
							}

						}));
			}
		};
	}

	
	public PEPersistentGroup lookupPersistentGroup(Name n) {
		PEPersistentGroup pesg = pc.findStorageGroup(n);
		if (pesg == null)
			throw new SchemaException(Pass.SECOND, "No such persistent group "
					+ n.getSQL());
		return pesg;
	}

	public Statement setExplainFlag(Statement in,
			List<Pair<Name, LiteralExpression>> options) {
		ExplainOptions opts = ExplainOptions.NONE;
		if (options != null) {
			for(Pair<Name, LiteralExpression> p : options) {
				ExplainOption opt = ExplainOption.find(p.getFirst().getUnqualified());
				if (opt == null)
					throw new SchemaException(Pass.FIRST, "No such explain option: " + p.getFirst().getSQL());
				opts = opts.addSetting(opt, p.getSecond().getValue(pc));
			}
		}
		in.setExplain(opts);
		return in;
	}

	public SetQuantifier lookupSetQuantifier(String in) {
		SetQuantifier sq = SetQuantifier.fromSQL(in);
		if (sq == null)
			throw new SchemaException(Pass.SECOND, "Unknown set quantifier: "
					+ in);
		return sq;
	}

	public Statement buildKillStatement(ExpressionNode expr, Boolean isKillConnection) {
		return new KillStatement(asIntegralLiteral(expr), isKillConnection.booleanValue());
	}

	public Statement buildEmptyStatement(String text) {
		return new EmptyStatement(text);
	}

	public LockType lookupLockType(String l, String r) {
		LockType lt = LockType.fromSQL(l, r);
		if (lt == null)
			throw new SchemaException(Pass.SECOND, "No such lock type "
					+ (l == null ? "" : l) + " " + (r == null ? "" : r));
		return lt;
	}

	public Statement buildUnlockTablesStatement() {
		return new LockStatement();
	}

	public Statement buildLockTablesStatement(
			List<Pair<ExpressionNode, LockType>> in) {
		ListOfPairs<TableInstance, LockType> tabs = new ListOfPairs<TableInstance, LockType>();
		for (Pair<ExpressionNode, LockType> p : in) {
			TableInstance ti = (TableInstance) p.getFirst();
			tabs.add(ti, p.getSecond());
		}
		return new LockStatement(tabs);
	}

	public void startSessionVarsScope() {
		resolveColumnsAsIdentifiers = true;
	}

	public void endSessionVarsScope() {
		resolveColumnsAsIdentifiers = false;
	}

	public UserScope findCurrentUser() {
		if (pc.getCapability() == Capability.PARSING_ONLY) return null;
		return pc.getCurrentUser().get(pc).getUserScope();
	}
	
	public UserScope buildUserScope(String name, String scopeStr) {
		return new UserScope(PEStringUtils.dequote(name), PEStringUtils.dequote((scopeStr == null) ? "'%'" : scopeStr));
	}

	public PEUser buildUserSpec(UserScope us, String pword) {
		return new PEUser(us, pword == null ? null : PEStringUtils.dequote(pword), false);
	}

	public Statement buildCreateUserStatement(List<PEUser> specs) {
		pc.getPolicyContext().checkRootPermission("create a user");
		// should check to see whether the users already exist
		if (pc.getCapability() != Capability.PARSING_ONLY && pc.isPersistent()) {
			for (PEUser peu : specs) {
				List<User> users = pc.getCatalog().findUsers(
						peu.getUserScope().getUserName(),
						peu.getUserScope().getScope());
				if (!users.isEmpty())
					throw new SchemaException(Pass.SECOND, "User "
							+ peu.getUserScope().getSQL() + " already exists");

			}
		}
		return new PECreateUserStatement(specs, false);
	}

	public Statement buildSetPasswordStatement(UserScope us, String text) {
		pc.getPolicyContext().checkRootPermission("set a password");
		PEUser exists = pc.findUser(us.getUserName(), us.getScope());
		if (exists == null)
			throw new SchemaException(Pass.SECOND, "User " + us.getUserName()
					+ " does not exist");
		return new SetPasswordStatement(exists, PEStringUtils.dequote(text));
	}

	public Statement buildDropUserStatement(UserScope us, Boolean ifExists) {
		pc.getPolicyContext().checkRootPermission("drop a user");
		PEUser peu = pc.findUser(us.getUserName(), us.getScope());
		if (peu == null) {
			if (Boolean.TRUE.equals(ifExists)) {
				// just do an empty statement
				return new EmptyStatement("drop nonexistent user");
			}
			throw new SchemaException(new ErrorInfo(DVEErrors.UNKNOWN_USER,us.getUserName(),us.getScope()));
		}
		return new PEDropUserStatement(peu);
	}

	public Statement buildAlterTableStatement(TableKey tab, AlterTableAction single) {
		return buildAlterTableStatement(tab, Collections.singletonList(single));
	}
	
	public Statement buildAlterTableStatement(TableKey tab, List<AlterTableAction> actions) {
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new PEAlterTableStatement(pc, tab, actions);
		PEAlterStatement<PETable> single = null;
		AlterTableAction singleAction = null;
		for(AlterTableAction aa : actions) { 
			single = aa.requiresSingleStatement(pc, tab);
			if (single != null) {
				singleAction = aa;
				break;
			}
		}
		if (single != null) {
			if (actions.size() > 1)
				throw new SchemaException(Pass.SECOND, "alter action of type " + singleAction.getClass().getSimpleName() + " must be the sole alter action");
			return single;
		}
		PEAlterTableStatement orig = new PEAlterTableStatement(pc,tab, actions);
		return pc.getPolicyContext().modifyAlterTableStatement(orig);
	}

	public Statement buildRenameTableStatement(final List<Pair<Name, Name>> sourceTargetNamePairs) {
		return RenameTableStatement.buildRenameTableStatement(pc, sourceTargetNamePairs);
	}

	public Statement buildRenameDatabaseStatement(final Pair<Name, Name> sourceTargetNames) {
		throw new SchemaException(Pass.FIRST, "This syntax is not supported as it has been deprecated.");
	}

	private List<AlterTableAction> wrapAlterAction(AlterTableAction ata) {
		return Collections.singletonList(ata);
	}
	
	public List<AlterTableAction> buildRenameTableAction(Name newName) {
		return wrapAlterAction(new RenameTableAction(newName));
	}

	/**
	 * If you specify CONVERT TO CHARACTER SET without a collation, the default
	 * collation for the character set is used.
	 */
	public List<AlterTableAction> buildTableConvertToAction(final Name charSet, final Name collation) {
		final Pair<String, String> charSetCollationPair = getCharSetCollationPair(charSet, collation);
		return wrapAlterAction(new ConvertToAction(charSetCollationPair.getFirst(), charSetCollationPair.getSecond()));
	}

	public List<AlterTableAction> buildChangeColumnAction(Name columnName,
			TableComponent<PEColumn> newDef, Pair<String, Name> firstOrAfterSpec) {
		return wrapAlterAction(new ChangeColumnAction(lookupAlteredColumn(columnName),
				(PEColumn) newDef, firstOrAfterSpec));
	}

	public List<AlterTableAction> buildChangeColumnAction(Name columnName,
			List<TableComponent<?>> defs, Pair<String, Name> firstOrAfterSpec) {
		ArrayList<AlterTableAction> out = new ArrayList<AlterTableAction>();
		for(TableComponent<?> tc : defs) {
			if (tc instanceof PEColumn) {
				PEColumn pec = (PEColumn) defs.get(0);
				out.add(new ChangeColumnAction(lookupAlteredColumn(columnName), pec, firstOrAfterSpec));
			} else if (tc instanceof PEKey) {
				out.add(new AddIndexAction((PEKey)tc));
			} else {
				throw new SchemaException(Pass.FIRST, "Unknown alter table change column target: " + tc);
			}
		}
		return out;
	}

	public List<AlterTableAction> buildModifyColumnAction(List<TableComponent<?>> defs, Pair<String, Name> firstOrAfterSpec) {
		if (defs.size() > 1)
			throw new SchemaException(Pass.SECOND, "No support for modifying column def with inline key def");
		PEColumn newDef = (PEColumn) defs.get(0);
		PEColumn existing = lookupAlteredColumn(newDef.getName());
		return wrapAlterAction(new ChangeColumnAction(existing, newDef, firstOrAfterSpec));
	}
	
	public List<AlterTableAction> buildAlterColumnAction(Name columnName,
			ExpressionNode litex) {
		return wrapAlterAction(new AlterColumnAction(lookupAlteredColumn(columnName),
				(LiteralExpression) litex));
	}

	public List<AlterTableAction> buildAddIndexAction(PEKey newIndex) {
		if (pc.getCapability() != Capability.PARSING_ONLY && newIndex.isUnresolved())
			throw new SchemaException(Pass.FIRST, "Invalid forward key during alter");
		return wrapAlterAction(new AddIndexAction((PEKey)newIndex));
	}

	public List<AlterTableAction> buildDropIndexAction(ConstraintType kt, Name indexName) {
		PEKey targ = lookupAlteredKey(kt,indexName);
		return wrapAlterAction(new DropIndexAction(targ));
	}

	public Statement buildExternalDropIndexStatement(Name indexName, Name tableName) {
		TableKey tk = lookupAlteredTable(tableName);
		AlterTableAction aa = buildDropIndexAction(null, indexName).get(0);
		return buildAlterTableStatement(tk,aa);
	}
	
	public List<AlterTableAction> buildDropColumnAction(Name columnName) {
		List<AlterTableAction> actions = new ArrayList<AlterTableAction>();
		PEColumn alteredCol = lookupAlteredColumn(columnName);

		// cannot drop column if it is the target in an FK
		if (pc.getCapability() == Capability.FULL) {
			for (UserTable ut : pc.getCatalog().findTablesWithFKSReferencing(alteredCol.getTable().getPersistentID())) {
				for(Key k : ut.getKeys()) {
					if (k.isForeignKey() && StringUtils.equals(k.getReferencedTable().getName(), alteredCol.getTable().getName().get())) {
						for(KeyColumn c : k.getColumns()) {
							if (StringUtils.equals(c.getTargetColumnName(), alteredCol.getName().get())) {
								throw new SchemaException(Pass.SECOND, "Cannot drop column '" + columnName.get() + "' because it is part of foreign key '" + k.getName() + "'");
							}
						}
					}
				}
			}

			for(PEKey key : alteredCol.getTable().getKeys(pc)) {
				if (key.containsColumn(alteredCol)) {
					if (key.isForeign()) {
						throw new SchemaException(Pass.SECOND, "Cannot drop column '" + columnName.get() + "' because it is part of foreign key '" + key.getName().get() + "'");
					}

					actions.add(buildDropIndexAction(null, key.getName()).get(0));

					if (key.getColumns(pc).size() > 1) {
						// rebuild index if multicol
						PEKey newPEKey = key.copy(pc, alteredCol.getTable().asTable());
						newPEKey.removeColumn(alteredCol);

						actions.add(buildAddIndexAction(newPEKey).get(0));
					}
				}
			}
		}
		
		actions.add(new DropColumnAction(alteredCol));
		return actions;
	}

	public List<AlterTableAction> buildTableOptionAction(TableModifier tm) {
		return wrapAlterAction(new ChangeTableModifierAction(tm));
	}
	
	public List<AlterTableAction> buildModifyDistributionAction(UnresolvedDistributionVector ndv) {
		return wrapAlterAction(new ChangeTableDistributionAction(ndv.resolve(pc, this)));
	}
	
	@SuppressWarnings("rawtypes")
	public List<AlterTableAction> buildAddColumnAction(List<TableComponent> newColumns) {
		return buildAddColumnAction(newColumns, null);
	}

	@SuppressWarnings("rawtypes")
	public List<AlterTableAction> buildAddColumnAction(List<TableComponent> newColumns, Pair<String,Name> firstOrAfterSpec) {
		if (firstOrAfterSpec != null) {
			// only valid when adding one column.  note that if we added a column & a key at the same time we should
			// ignore the key part (because keys doen't really have a position).
			int ncols = 0;
			for(TableComponent tc : newColumns)
				if (tc instanceof PEColumn)
					ncols++;
			if (ncols > 1) {
				// only valid when adding one column
				throw new SchemaException(Pass.FIRST,
						"Cannot specify FIRST or AFTER option with multicolumn ADD");
			}
		}
		
		ArrayList<PEColumn> casted = new ArrayList<PEColumn>();
		ArrayList<PEKey> keys = new ArrayList<PEKey>();
		for(TableComponent nc : newColumns) {
			if (nc instanceof PEColumn)
				casted.add((PEColumn)nc);
			else if (nc instanceof PEKey)
				keys.add((PEKey)nc);
		}
		List<AlterTableAction> out = new ArrayList<AlterTableAction>();
		if (!casted.isEmpty()) {
			out.add(new AddColumnAction(casted, firstOrAfterSpec));
		}
		for(PEKey pek : keys) {
			out.add(new AddIndexAction(pek));
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	private PEColumn lookupAlteredColumn(Name columnName) {
		PEColumn c = null;
		final Table<?> parentTable = scope.getAlteredTable();
		c = (PEColumn) parentTable.lookup(pc, columnName);
		if (c == null) {
			if (pc.getCapability() == Capability.PARSING_ONLY) {
				c = PEColumn.buildColumn(pc, columnName, TempColumnType.TEMP_TYPE, Collections.EMPTY_LIST, null,null);
			} else {
				throw new SchemaException(Pass.SECOND, "Unknown column '" + columnName + "' in '" + parentTable.getName().get() + "'");
			}
			
		}
		return c;
	}

	/**
	 * @param kt
	 * @param keyName
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private PEKey lookupAlteredKey(ConstraintType kt, Name keyName) {
		PEKey k = null;
		if (pc.getCapability() == Capability.PARSING_ONLY) {
			k = new PEKey(keyName, IndexType.BTREE, Collections.EMPTY_LIST, null);
		} else {		
			k = ((PETable)scope.getAlteredTable()).lookupKey(pc, keyName);
			if (k == null)
				throw new SchemaException(Pass.SECOND, "No such key: " + keyName);
		}
		return k;
	}
	
	@SuppressWarnings("unchecked")
	public TableKey lookupAlteredTable(Name tabName) {
		TableKey tab = null;
		if (pc.getCapability() == Capability.PARSING_ONLY) {
			tab = TableKey.make(pc,new PETable(pc, tabName, Collections.EMPTY_LIST, null, null, null),0);
		} else {
			TableResolver resolver = new TableResolver().withMTChecks()
					.withDatabaseFunction(new UnaryProcedure<Database<?>>() {

						@Override
						public void execute(Database<?> object) {
							if (!(object instanceof PEDatabase))
								throw new SchemaException(Pass.SECOND,
										"Invalid database for table alter: '" + object.getName()
												+ "'");
						}
						
					});
			TableInstance ti = resolver.lookupTable(pc, tabName, lockInfo);
			if (ti == null)
				throw new SchemaException(Pass.SECOND, "No such table: " + tabName.getSQL());
			tab = ti.getTableKey();
		}
		// now we have to push all the columns in case they are used
		pushScope();
		scope.registerAlterColumns(pc,tab.getAbstractTable().asTable());
		return tab;
	}

	public WhenClause buildWhenClause(ExpressionNode te, ExpressionNode re,
			Object orig) {
		return new WhenClause(te, re, SourceLocation.make(orig));
	}

	public ExpressionNode buildCaseExpression(ExpressionNode te,
			ExpressionNode ee, List<WhenClause> whens, Object orig) {
		return new CaseExpression(te, ee, whens, SourceLocation.make(orig));
	}

	public ExpressionNode buildCastCall(String castText, ExpressionNode toCast, String asText,
			Name tn) {
		return new CastFunctionCall(toCast, castText, tn, asText);
	}

	public Name buildCastToIntegralType(String signed, String integer) {
		if (integer == null)
			return new UnqualifiedName(signed);
		return new UnqualifiedName(signed + " " + integer);
	}
	
	public Name buildCastToSizedType(String theType, String theFirstSize, String theSecondSize) {
		if (theFirstSize == null && theSecondSize == null)
			return new UnqualifiedName(theType);
		StringBuilder buf = new StringBuilder();
		buf.append(theType).append("(").append(theFirstSize);
		if (theSecondSize != null)
			buf.append(",").append(theSecondSize);
		buf.append(")");
		return new UnqualifiedName(buf.toString());
	}
	
	public Name buildCastToBinaryOrChar(String theType, String maybeFirstSize, Name maybeCharset) {
		StringBuilder buf = new StringBuilder();
		buf.append(theType);
		if (maybeFirstSize != null) {
			buf.append("(").append(maybeFirstSize).append(")");
		} else if (maybeCharset != null) {
			buf.append(" charset ").append(maybeCharset.getSQL());
		}
		return new UnqualifiedName(buf.toString());
	}
	
	public ExpressionNode buildCharCall(List<ExpressionNode> charCodes, Name outputEncoding) {
		return new CharFunctionCall(charCodes, outputEncoding);
	}
	
	public ExpressionNode buildRandCall(List<ExpressionNode> args) {
		if (args.isEmpty()) {
			return new RandFunctionCall(null);
		} else if (args.size() == 1) {
			return new RandFunctionCall(args.get(0));
		} else {
			throw new SchemaException(new ErrorInfo(DVEErrors.INCORRECT_PARAM_COUNT_FUNCTION_CALL,"RAND"));
		}
	}

	public ExpressionNode buildConvertCall(ExpressionNode toConvert, Name transcodingName, Type castToType) {
		Name rhs = transcodingName;
		if (castToType != null) {
			StringBuilder buf = new StringBuilder();
            Singletons.require(HostService.class).getDBNative().getEmitter().emitConvertTypeDeclaration(castToType, buf);
			rhs = new UnqualifiedName(buf.toString());
		}
		return new ConvertFunctionCall(toConvert,rhs,(transcodingName != null));
	}
	
	public ExpressionNode buildTimestampDiffCall(String unit,
			ExpressionNode param1, ExpressionNode param2) {
		FunctionName fn = new FunctionName("TIMESTAMPDIFF", TokenTypes.TIMESTAMPDIFF, false);
		ArrayList<ExpressionNode> actuals = new ArrayList<ExpressionNode>();
		actuals.add(buildIdentifierLiteral(unit));
		actuals.add(param1);
		actuals.add(param2);
		return buildFunctionCall(fn, actuals, null, null);
	}

	public ExpressionNode buildInterval(ExpressionNode intervalExpression, String unit) {
		return new IntervalExpression(intervalExpression,unit,null);
	}
	
	public ExpressionNode buildMultivalueExpression(final List<ExpressionNode> values) {
		if (values.size() == 1) {
			return addGrouping(values.get(0));
		}
		return buildExpressionSet(values);
	}

	public ExpressionNode buildExpressionSet(List<ExpressionNode> values) {
		return new ExpressionSet(values, null);
	}

	public ExpressionNode buildGroupConcat(FunctionName fn, List<ExpressionNode> params, boolean distinct, String separator) {
		SetQuantifier sq = (distinct ? SetQuantifier.DISTINCT : null);
		return new GroupConcatCall(fn,params, sq, separator);
	}
	
	public Statement buildCreateTenantStatement(Name tenantName, Name onDB,
			String description) {
		PEDatabase peds = null;
		if (onDB == null) {
			peds = pc.findSingleMTDatabase();
			if (peds == null)
				throw new SchemaException(Pass.SECOND,
						"No multitenant database found");
		}
		if (peds == null)
			peds = pc.findPEDatabase(onDB);
		if (peds == null)
			throw new SchemaException(Pass.SECOND, "No such database: "
					+ (onDB == null ? "(blank)" : onDB.getSQL()));
		if (!peds.getMTMode().isMT())
			throw new SchemaException(Pass.SECOND, "Cannot create tenant on "
					+ peds.getMTMode().describe() + " database "
					+ (onDB == null ? "(blank)" : onDB.getSQL()));
		return pc.getPolicyContext()
				.buildCreateTenantStatement(peds, tenantName,
						(description == null ? null : PEStringUtils.dequote(description)));
	}

	public Statement buildSuspendTenantStatement(Name tenantName) {
		return pc.getPolicyContext().buildSuspendTenantStatement(tenantName);
	}

	public Statement buildResumeTenantStatement(Name tenantName) {
		return pc.getPolicyContext().buildResumeTenantStatement(tenantName);
	}

	public Statement buildDropTenantStatement(Name tenantName) {
		return pc.getPolicyContext().buildDropTenantStatement(tenantName, false);
	}

	public Name buildUnrestrictedName(List<Name> names) {
		ArrayList<UnqualifiedName> parts = new ArrayList<UnqualifiedName>();
		for (Name n : names) {
			// should not happen
			if (n.isQualified())
				throw new SchemaException(Pass.FIRST,
						"Qualified name found where unqualified name expected");
			parts.add(n.getUnqualified());
		}
		return new QualifiedName(parts);
	}

	public Pair<Name, LiteralExpression> buildConfigOption(Name key,
			ExpressionNode value) {
		return new Pair<Name, LiteralExpression>(key, asLiteral(value));
	}

	private static final Name pluginKey = new UnqualifiedName("PLUGIN");
	private static final Name activeKey = new UnqualifiedName("ACTIVE");

	public Statement buildCreateDynamicSiteProvider(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("create a dynamic site provider");
		// see if we have one already
		Provider extant = pc.getCatalog().findProvider(name.get());
		if (extant != null)
			throw new SchemaException(Pass.SECOND, "Dynamic site provider "
					+ name.getSQL() + " already exists");
		String plugin = null;
		Boolean isActive = null;
		for (Iterator<Pair<Name, LiteralExpression>> iter = options.iterator(); iter
				.hasNext();) {
			Pair<Name, LiteralExpression> p = iter.next();
			if (p.getFirst().getCapitalized().equals(pluginKey)) {
				plugin = p.getSecond().asString(pc);
				iter.remove();
			} else if (p.getFirst().getCapitalized().equals(activeKey)) {
				isActive = (Boolean) p.getSecond().getValue(pc);
				iter.remove();
			}
		}
		if (isActive == null)
			isActive = Boolean.TRUE;
		if (plugin == null)
			throw new SchemaException(Pass.SECOND,
					"Must specify plugin for CREATE DYNAMIC SITE PROVIDER");
		PEProvider provider = new PEProvider(pc, name, plugin, isActive);
		return new PECreateGroupProviderStatement(provider, options);
	}

	public Statement buildAlterDynamicSiteProviderStatement(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("alter a dynamic site provider");
		// see if we have one already
		Provider extant = pc.getCatalog().findProvider(name.get());
		if (extant == null)
			throw new SchemaException(Pass.SECOND, "No such dynamic site provider: "
					+ name.getSQL());
		PEProvider pep = PEProvider.load(extant, pc);
		return new PEAlterGroupProviderStatement(pep, options);
	}

	public Statement buildShowDynamicSiteProvider(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("show a dynamic site provider");
		// see if we have one already
		Provider extant = pc.getCatalog().findProvider(name.get());
		if (extant == null)
			throw new SchemaException(Pass.SECOND, "No such dynamic site provider: "
					+ name.getSQL());
		List<CatalogEntity> ents = null;
		// build the command block, use null for the action
		SiteManagerCommand smc = new SiteManagerCommand(Command.SHOW, extant,
				PEGroupProviderDDLStatement.convertOptions(pc,options));
		try {
			SiteProviderPlugin sm = SiteProviderFactory.getInstance(smc
					.getTarget().getName());
			ents = sm.show(smc);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to query provider "
					+ extant.getName(), pe);
		}
		return new SchemaQueryStatement(true, DYNAMIC_SITE_PROVIDER, ents,
				false, null);
	}

	public Statement buildShowDynamicSiteProvidersSites() {
		pc.getPolicyContext().checkRootPermission("show dynamic site providers sites");
		// in this case, we just find all the providers, then route the
		// appropriate command to each provider, aggregate results,
		// etc.
		ListOfPairs<String, Object> options = new ListOfPairs<String, Object>();
		// or whatever you like
		options.add("cmd", "sites");
		ArrayList<CatalogEntity> results = new ArrayList<CatalogEntity>();
		List<Provider> providers = pc.getCatalog().findAllProviders();
		for (Provider p : providers) {
			try {
				SiteManagerCommand smc = new SiteManagerCommand(Command.SHOW,
						p, options);
				SiteProviderPlugin sm = SiteProviderFactory.getInstance(smc
						.getTarget().getName());
				results.addAll(sm.show(smc));
			} catch (PEException pe) {
				throw new SchemaException(Pass.SECOND,
						"Unable to query provider " + p.getName(), pe);
			}
		}
		return new SchemaQueryStatement(true, DYNAMIC_SITE_PROVIDER_SITES,
				results, false, null);
	}

	@SuppressWarnings("unchecked")
	public Statement buildDropDynamicSiteProviderStatement(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("drop a dynamic site provider");
		// see if we have one already
		Provider extant = pc.getCatalog().findProvider(name.get());
		if (extant == null)
			throw new SchemaException(Pass.SECOND, "No such dynamic site provider: "
					+ name.getSQL());
		PEProvider pep = PEProvider.load(extant, pc);
		return new PEDropGroupProviderStatement(pep, (options == null ? Collections.EMPTY_LIST : options));
	}

	public PEPolicyClassConfig buildClassConfig(Name theClassName, ExpressionNode countLiteral, 
			ExpressionNode providerLiteral, ExpressionNode poolLiteral) {
		PolicyClass theClass = PolicyClass.findSQL(theClassName.getUnqualified().getUnquotedName().get());
		if (theClass == null)
			throw new SchemaException(Pass.SECOND, "No such policy type: " + theClassName);
		Integer theCount = null;
		String theProvider = null;
		String thePool = null;
		if (poolLiteral instanceof LiteralExpression)
			thePool = (String) ((LiteralExpression)poolLiteral).getValue(pc);
		if (providerLiteral instanceof LiteralExpression)
			theProvider = (String) ((LiteralExpression)providerLiteral).getValue(pc);
		if (countLiteral instanceof LiteralExpression) {
			LiteralExpression litex = (LiteralExpression) countLiteral;
			Number n = (Number) litex.getValue(pc);
			theCount = n.intValue();
		}
		return new PEPolicyClassConfig(theClass, theProvider, thePool, (theCount == null ? 0 : theCount));
	}
	
	public Statement buildCreateDynamicSitePolicyStatement(Name name,
			boolean strict,
			List<PEPolicyClassConfig> classes) {
		if ( name == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		pc.getPolicyContext().checkRootPermission("create a dynamic site policy");
		DynamicPolicy dp = pc.getCatalog().findPolicy(name.get());
		if (dp != null)
			throw new SchemaException(Pass.SECOND, "Dynamic site policy "
					+ name.getSQL() + " already exists");
		PEPolicy pep = new PEPolicy(pc, name, strict, classes);
		return new PECreateStatement<PEPolicy, DynamicPolicy>(pep, true,
				DYNAMIC_SITE_POLICY_TAG, false);
	}

	public Statement buildAlterDynamicSitePolicyStatement(Name name,
			Name newName, Boolean newStrict,
			List<PEPolicyClassConfig> configs) {
		pc.getPolicyContext().checkRootPermission("alter a dynamic site policy");
		DynamicPolicy dp = pc.getCatalog().findPolicy(name.get());
		if (dp == null)
			throw new SchemaException(Pass.SECOND, "No such dynamic site policy: "
					+ name.getSQL());
		PEPolicy pep = PEPolicy.load(dp, pc);
		return new PEAlterPolicyStatement(pep, newName, newStrict, configs);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Statement buildDropDynamicSitePolicyStatement(Name name) {
		pc.getPolicyContext().checkRootPermission("drop a dynamic site policy");
		DynamicPolicy dp = pc.getCatalog().findPolicy(name.get());
		if (dp == null)
			throw new SchemaException(Pass.SECOND, "Dynamic site policy "
					+ name.getSQL() + " does not exist");
		PEPolicy pep = PEPolicy.load(dp, pc);
		return new PEDropStatement(PEPolicy.class, null, true, pep,
				"DYNAMIC SITE POLICY");
	}

	public Statement buildCreateExternalServiceStatement(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		if ( name == null ) {
			throw new SchemaException(Pass.FIRST, MISSING_UNQUALIFIED_IDENTIFIER_ERROR_MSG);
		}

		// check permissions
		pc.getPolicyContext().checkRootPermission("create an external service");

		ExternalService es = pc.getCatalog().findExternalService(
				name.getUnqualified().get());
		if (es != null)
			throw new SchemaException(Pass.SECOND, "External Service "
					+ name.getSQL() + " already exists.");
		return new PECreateExternalServiceStatement(new PEExternalService(
				pc, name, options), true, "EXTERNAL SERVICE", false);
	}

	public Statement buildDropExternalServiceStatement(Name name) {
		pc.getPolicyContext().checkRootPermission("drop an external service");
		ExternalService es = pc.getCatalog().findExternalService(
				name.getUnqualified().get());
		if (es == null)
			throw new SchemaException(Pass.SECOND, "No such external service: "
					+ name.getSQL());
		return new PEDropExternalServiceStatement(PEExternalService.class,
				null, true, new PEExternalService(pc, es), "EXTERNAL SERVICE");
	}

	public Statement buildAlterExternalServiceStatement(Name name,
			List<Pair<Name, LiteralExpression>> options) {
		pc.getPolicyContext().checkRootPermission("alter an external service");
		ExternalService es = pc.getCatalog().findExternalService(
				name.getUnqualified().get());
		if (es == null)
			throw new SchemaException(Pass.SECOND, "No such external service: "
					+ name.getSQL());
		return new PEAlterExternalServiceStatement(new PEExternalService(pc, es, options));
	}

	public Statement buildStartExternalServiceStatement(Name name) {
		pc.getPolicyContext().checkRootPermission("start an external service");
		ExternalService es = pc.getCatalog().findExternalService(
				name.getUnqualified().get());
		if (es == null)
			throw new SchemaException(Pass.SECOND, "No such external service: "
					+ name.getSQL());
		return new ExternalServiceControlStatement(ExternalServiceControlStatement.Action.START, es);
	}

	public Statement buildStopExternalServiceStatement(Name name) {
		pc.getPolicyContext().checkRootPermission("stop an external service");
		ExternalService es = pc.getCatalog().findExternalService(
				name.getUnqualified().get());
		if (es == null)
			throw new SchemaException(Pass.SECOND, "No such external service: "
					+ name.getSQL());
		return new ExternalServiceControlStatement(ExternalServiceControlStatement.Action.STOP, es);
	}

	public Statement buildCreateContainerStatement(Name containerName, Name groupName,
			Pair<DistributionVector.Model, UnqualifiedName> backingModel, Boolean ifNotExists) {
//		// check permissions
//		pc.getPolicyContext().checkRootPermission("create a container");

		Container container = pc.getCatalog().findContainer(containerName.getUnqualified().get());
		if (container != null) {
			if (Boolean.TRUE.equals(ifNotExists)) {
				// just do an empty statement
				return new EmptyStatement("Create existing container");
			}
			throw new SchemaException(Pass.SECOND, "Container "
					+ containerName.getSQL() + " already exists.");
		}
		
		PEPersistentGroup pesg = null;
		if (groupName != null) {
			pesg = pc.findStorageGroup(groupName);
			if (pesg == null)
				throw new SchemaException(Pass.SECOND, "No such persistent group: " + groupName.getSQL());
		}

		if (backingModel == null) {
			throw new SchemaException(Pass.SECOND, "Missing distribution declaration on container "
					+ containerName.getSQL());
		}
		
		DistributionVector.Model model = backingModel.getFirst();
		RangeDistribution rd = null;
		if (model == DistributionVector.Model.RANGE) {
			UnqualifiedName rangeName = backingModel.getSecond();
			rd = pc.findRange(rangeName, groupName);
			if (rd == null)
				throw new SchemaException(Pass.SECOND, "No such range on group " + groupName + ": " + rangeName.getSQL());
		}
		
		return new PECreateStatement<PEContainer, Container>(new PEContainer(
				pc, containerName, pesg, model, rd), true, "CONTAINER", false);
	}

	public Statement buildDropContainerStatement(Name name, Boolean ifExists) {
		pc.getPolicyContext().checkRootPermission("drop a container");
		PEContainer cont = pc.findContainer(name);
		if (cont == null) {
			if (Boolean.TRUE.equals(ifExists)) {
				return buildEmptyStatement("Drop a non existing container.");
			}
			throw new SchemaException(Pass.SECOND, "No such container: "
					+ name.getSQL());
		}
		return new PEDropContainerStatement(cont);
	}
	
	// move this somewhere else
	public Statement buildUseContainerStatement(Name containerName, List<Pair<LiteralExpression,Name>> values) {
		for(Pair<LiteralExpression,Name> val : values) {
			LiteralExpression expr = val.getFirst();
			if (expr.isNullLiteral()) {
				if (values.size() == 1) {
					return new UseContainerStatement(pc,true);
				}
				throw new SchemaException(Pass.SECOND, "NULL context cannot be specified with container discriminant.");
			} else if (expr.isGlobalLiteral()) {
				if (values.size() == 1) {
					return new UseContainerStatement(pc,false);
				}
				throw new SchemaException(Pass.SECOND, "GLOBAL context cannot be specified with container discriminant.");
			}
		}
			
		PEContainer cont = pc.findContainer(containerName);
		if (cont == null)
			throw new SchemaException(Pass.SECOND, "No such container: " + containerName.getSQL());
		PETable bt = cont.getBaseTable(pc);
		if (bt == null)
			throw new SchemaException(Pass.SECOND, "Unable to use container " + containerName.getSQL() + ": no discriminator set");
		List<PEColumn> discCols = cont.getDiscriminantColumns(pc);
		if (discCols.size() != values.size()) 
			throw new SchemaException(Pass.SECOND, "Invalid discriminant value.  Found " + values.size() + " columns but require " + discCols.size() + " columns");
		return new UseContainerStatement(pc,cont,bt,discCols,values,false);
	}
	
	public Parameter buildParameter(Token tree) {
		Parameter param = new Parameter(SourceLocation.make(tree));
		parameters.add(param);
		return param;
	}

	public Statement buildShowProcesslistStatement(Boolean full) {
		return new ShowProcesslistStatement(full);
	}
	
	public Statement buildShowPassthroughStatement(String command) {
		if (StringUtils.equals(command, "PLUGINS")) {
			return new ShowPassthroughStatement(PassThroughCommandType.PLUGINS, false, null);
		} else if (StringUtils.equals(command, "MASTER LOGS")) {
			return new ShowPassthroughStatement(PassThroughCommandType.MASTERLOGS, false, null);
		} else if (StringUtils.equals(command, "MASTER STATUS")) {
			return new ShowPassthroughStatement(PassThroughCommandType.MASTERSTATUS, false, null);
		} else if (StringUtils.equals(command, "SLAVE STATUS")) {
			return new ShowPassthroughStatement(PassThroughCommandType.SLAVESTATUS, false, null);
		} else if (StringUtils.equals(command, "GRANTS")) {
			return new ShowPassthroughStatement(PassThroughCommandType.GRANTS, false, null);
		}
		throw new SchemaException(Pass.SECOND, "No SHOW support for command '" + command + "'");
	}

	public GrantScope buildGrantScope(Name n) {
		if (n == null)
			return new GrantScope();
		// otherwise it's a db.* or db.table
		// the grant scope might be a tenant
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new GrantScope(n);
		PEDatabase peds = pc.findPEDatabase(n);
		PETenant ten = null;
		if (peds == null)
			ten = pc.findTenant(n);
		if (peds == null && ten == null)
			throw new SchemaException(Pass.SECOND, "No such database: "
					+ n.getSQL());
		if (peds != null)
			return new GrantScope(peds);
		return new GrantScope(ten);
	}

	/**
	 * @param gs
	 * @param user
	 * @param whatToGrant
	 * @return
	 */
	public Statement buildGrant(GrantScope gs, PEUser user, String whatToGrant) {
		if (pc.getCapability() == Capability.PARSING_ONLY) {
			return new GrantStatement(gs.buildPriviledge(pc,user));
		}
		pc.getPolicyContext().checkRootPermission("grant privileges");
		// we're going to use the PEUser bit to look up the actual user
		PEUser actualUser = pc.findUser(user.getUserScope().getUserName(), user
				.getUserScope().getScope());
		if (actualUser == null)
			actualUser = user;
		// so, a grant statement always creates a priv, but optionally creates the user
		return new GrantStatement(gs.buildPriviledge(pc,actualUser));
	}

	public Statement buildFlushPrivileges() {
		return new FlushPrivilegesStatement();
	}


	protected DelegatingLiteralExpression replaceLiteral(DelegatingLiteralExpression oldLiteral, int valueType, SourceLocation sloc, Object value) {
		literals.remove(oldLiteral.getPosition());
		DelegatingLiteralExpression dle = new DelegatingLiteralExpression(valueType,sloc,this,oldLiteral.getPosition(),oldLiteral.getCharsetHint());
		literals.add(oldLiteral.getPosition(),new Pair<DelegatingLiteralExpression,Object>(dle,value));
		return dle;
	}
	
	public ExpressionNode maybeNegate(Token tok, ExpressionNode rhs) {
		if (tok == null)
			return rhs;
		FunctionName fn = buildFunctionName(tok, true);
		ArrayList<ExpressionNode> parts = new ArrayList<ExpressionNode>(1);
		parts.add(rhs);
		return buildFunctionCall(fn, parts, null, null);
	}

	public ExpressionNode buildFunctionOrIdentifier(Name name,
			SetQuantifier sq, List<ExpressionNode> params, Token asterisk,
			Object loc, Token leftParen, Token rightParen) {
		SourceLocation sloc = SourceLocation.make(loc);
		if (sq == null && params == null && asterisk == null && 
				leftParen==null && rightParen==null) {
			if (opts.isResolve())
				return buildColumnReference(name);
			return new NameInstance(name, sloc);
		}
		UnqualifiedName unq = name.getUnqualified();
		FunctionName fn = new FunctionName(unq, false);
		if (sq != null && !fn.isAggregate())
			throw new SchemaException(Pass.FIRST,
					"Illegal use of set quantifier");
		else if (asterisk != null && !fn.isCount())
			throw new SchemaException(Pass.FIRST,
					"Illegal use of '*' in a function");
		else if (asterisk != null && (params != null) && !params.isEmpty())
			throw new SchemaException(Pass.FIRST,
					"Mixed use of parameters and '*'");
		ArrayList<ExpressionNode> actuals = new ArrayList<ExpressionNode>();
		if (asterisk != null)
			actuals.add(new Wildcard(SourceLocation.make(asterisk)));
		else {
			if (params != null) {
				actuals.addAll(params);
			}
		}
		return buildFunctionCall(fn, actuals, sq, sloc);
	}

	public ExpressionNode maybeBuildExprAlias(ExpressionNode targ, Name alias, String stringAlias,
			Object tree) {
		SourceLocation sloc = SourceLocation.make(tree);
		
		if (alias == null && stringAlias == null)
			return targ;

		if (targ instanceof NameInstance && alias != null) {
			// Don't allow an alias to be the same name as the column
			// This will cause a stack overflow because the column will refer to an alias 
			// which refers back to the column.
			NameInstance colName = (NameInstance)targ;
			if (colName.getName().get().equals(alias.get())) {
				return targ;
			}
		}
		
		Alias a = null;
		if (alias != null)
			a = new NameAlias(castAlias(alias));
		else if (stringAlias != null)
			a = new StringLiteralAlias(PEStringUtils.dequote(stringAlias));
		
		return scope.buildExpressionAlias(targ, a, SourceLocation.make(tree));
	}

	public ProjectingStatement maybeBuildUnionStatement(List<ProjectingStatement> stmts,
			List<Boolean> unionOps) {
		ProjectingStatement singleReturn = null;
		if (stmts.size() == 1)
			singleReturn = stmts.get(0);
		else {
			// working from the back of the stmts list, build union clauses until
			// there is only one stmt left
			for (int op = unionOps.size() - 1; op > -1; op--) {
				ProjectingStatement rhs = stmts.get(op + 1);
				ProjectingStatement lhs = stmts.get(op);
				Boolean variety = unionOps.get(op);
				UnionStatement nus = new UnionStatement(lhs, rhs,
						variety.booleanValue(),lhs.getSourceLocation());
				stmts.remove(op + 1);
				stmts.remove(op);
				stmts.add(op, nus);
				nus.getDerivedInfo().addNestedStatements(
						Arrays.asList(new ProjectingStatement[] { lhs, rhs }));
			}
			singleReturn = stmts.get(0);
		}
		return singleReturn;
	}

	public Statement buildShowErrorsOrWarnings(String typeTag,
			LimitSpecification limit) {
		return new ShowErrorsWarningsStatement(typeTag, limit);
	}

	public Statement buildShowSitesStatus() {
		return new ShowSitesStatusStatement(); 
	}
	
	@SuppressWarnings("unchecked")
	public Statement buildShowStatus(Pair<ExpressionNode, ExpressionNode> likeOrWhere) {
		DirectShowStatusInformation ist = (DirectShowStatusInformation) Singletons.require(HostService.class).getInformationSchema().lookupShowTable(
				new UnqualifiedName("status"));

		ExpressionNode likeExpr = (likeOrWhere == null ? null : likeOrWhere
				.getFirst());
		ExpressionNode whereExpr = (likeOrWhere == null ? null : likeOrWhere
				.getSecond());
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new SchemaQueryStatement(false, "status", Collections.EMPTY_LIST, true, null);
		return ist.buildShowPlural(pc, null, likeExpr, whereExpr, null);
	}

	@SuppressWarnings("unchecked")
	public Statement buildShowVariables(VariableScope ivs,
			Pair<ExpressionNode, ExpressionNode> likeOrWhere) {
        DirectShowVariablesTable ist = (DirectShowVariablesTable) Singletons.require(HostService.class).
				getInformationSchema().lookupShowTable(
						new UnqualifiedName("variables"));
		VariableScope vs = ivs;
		if (vs == null)
			vs = new VariableScope(VariableScopeKind.SESSION);
		ExpressionNode likeExpr = (likeOrWhere == null ? null : likeOrWhere
				.getFirst());
		ExpressionNode whereExpr = (likeOrWhere == null ? null : likeOrWhere
				.getSecond());
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new SchemaQueryStatement(false, "variables", Collections.EMPTY_LIST, true, null);
		return ist.buildShow(pc, vs, likeExpr, whereExpr);
	}
	
	public Statement buildShowColumns(String onInfoSchemaTable,
			List<Name> scoping, Pair<ExpressionNode, ExpressionNode> likeOrWhere, Token full) {
		if (pc.getCapability() == Capability.PARSING_ONLY && !pc.getCatalog().isPersistent())
			return new EmptyStatement("no catalog queries with transient execution engine");
		ShowSchemaBehavior ist = Singletons.require(HostService.class).getInformationSchema()
				.lookupShowTable(new UnqualifiedName(onInfoSchemaTable));
		if (ist == null)
			throw new MigrationException("Need to add info schema table for "
					+ onInfoSchemaTable);
		ExpressionNode likeExpr = (likeOrWhere == null ? null : likeOrWhere
				.getFirst());
		ExpressionNode whereExpr = (likeOrWhere == null ? null : likeOrWhere
				.getSecond());
		ShowOptions so = new ShowOptions();
		if (full != null) so.withFull();
		// break up the scoping value into parts if qualified
		List<Name> scopingParts = new ArrayList<Name>();
		for (Name name : scoping) {
			if (name instanceof QualifiedName) {
				QualifiedName qn = (QualifiedName)name;
				scopingParts.add(qn.getParts().get(1)); // table name
				scopingParts.add(qn.getParts().get(0)); // database
			} else {
				// assumed UnqualifiedName
				scopingParts.add(name);
			}
		}
		return ist.buildShowPlural(pc, scopingParts, likeExpr, whereExpr, so);
	}

	public List<AlterTableAction> buildEnableKeysAction() {
		return wrapAlterAction(new ChangeKeysStatusAction(true));
	}
	
	public List<AlterTableAction> buildDisableKeysAction() {
		return wrapAlterAction(new ChangeKeysStatusAction(false));
	}

	public Statement buildShowMultitenantLocks() {
        return new SchemaQueryStatement(true,"multitenant locks", Singletons.lookup(LockManager.class).showState());
	}

	public Statement buildMaintenanceQuery(String operation, String option, List<Name> tables) {
		MaintenanceCommandType maintenanceCommand = MaintenanceCommandType.valueOf(operation.toUpperCase());
		if (maintenanceCommand == null) {
			throw new SchemaException(Pass.FIRST, "Invalid table maintenance command: " + operation);
		}
		
		MaintenanceOptionType maintenanceOption = MaintenanceOptionType.NONE;
		if (option != null) {
			maintenanceOption = MaintenanceOptionType.valueOf(option.toUpperCase());
			if (maintenanceOption == null) {
				throw new SchemaException(Pass.FIRST, "Invalid table maintenance option: " + option);
			}
		}
		
		List<TableInstance> tblInstances = lookupTables(tables);
		// restrict maintenance commands to a single Persistent Group
		Name persistentGroup = tblInstances.get(0).getAbstractTable().getPersistentStorage(pc).getName();
		for (TableInstance tableInstance : tblInstances) {
			if (! persistentGroup.equals(tableInstance.getAbstractTable().getPersistentStorage(pc).getName())) {
				throw new SchemaException(Pass.FIRST, "Table '" + tableInstance.getAbstractTable().getName(pc).get()
						+ "' in maintenance command is not in Persistent Group '" + persistentGroup.get() + "'");
			}
		}
		
		if (MaintenanceCommandType.ANALYZE == maintenanceCommand)
			return new AnalyzeTablesStatement(maintenanceOption, tblInstances);
		
		return new TableMaintenanceStatement(maintenanceCommand, maintenanceOption, tblInstances);
	}
	
	private List<TableInstance> lookupTables(List<Name> tableNames) {
		List<TableInstance> tblInstances = new ArrayList<TableInstance>();
		
		TableResolver resolver = new TableResolver().withMTChecks();
		
		// figure out whether the target table(s) are known or not
		for (Name targetTableName : tableNames) {
			/*
			Database<?> db = null;
			UnqualifiedName candidateName = null;
			if (targetTableName.isQualified()) {
				QualifiedName qn = (QualifiedName) targetTableName;
				UnqualifiedName ofdb = qn.getNamespace();
				candidateName = qn.getUnqualified();
				db = pc.findPEDatabase(ofdb);
				if (db == null)
					throw new SchemaException(Pass.FIRST, "No such database: " + ofdb);
			} else {
				db = pc.getCurrentDatabase();
				if (db == null)
					throw new SchemaException(Pass.FIRST, "No current database");
				candidateName = (UnqualifiedName) targetTableName;
			}
			TableInstance ti = db.getSchema().buildInstance(pc, candidateName, lockInfo, true);
			*/
			TableInstance ti = resolver.lookupTable(pc, targetTableName, lockInfo);
			if (ti == null)
				throw new SchemaException(Pass.FIRST, "No such table: " + targetTableName);

			tblInstances.add(ti.adapt(targetTableName.getUnqualified(), null, ti.getNode(), false));
		}
		return tblInstances;
	}
	
	public Statement buildAnalyzeKeys(List<Name> names) {
		return new AnalyzeKeysStatement(lookupTables(names));
	}
	
	@Override
	public Object getValue(SchemaContext sc, IParameter p) {
		throw new SchemaException(Pass.SECOND, "Attempt to access parameter value from non value source");
	}

	@Override
	public Object getLiteral(SchemaContext sc, IDelegatingLiteralExpression dle) {
		return literals.get(dle.getPosition()).getSecond();
	}

	@Override
	public Object getTenantID(SchemaContext sc) {
		throw new SchemaException(Pass.SECOND, "Attempt to access tenant id value from non tenant id source");
	}

	@Override
	public Object getAutoincValue(SchemaContext sc, IAutoIncrementLiteralExpression exp) {
		throw new SchemaException(Pass.SECOND, "Attempt to access autoinc value before planning");
	}
	
	public LoadDataInfileColOption buildLoadDataInfileColOption(String terminated, String enclosed, String escaped) {
		LoadDataInfileColOption option = new LoadDataInfileColOption(terminated, enclosed, escaped);
		return option;
	}
	
	public LoadDataInfileLineOption buildLoadDataInfileLineOption(String starting, String terminated) {
		LoadDataInfileLineOption option = new LoadDataInfileLineOption(starting, terminated);
		return option;
	}
	
	public Statement buildLoadDataInfileStatement(
			LoadDataInfileModifier modifier, boolean local, String fileName,
			boolean replace, boolean ignore, Name tempTableName, Name characterSet,
			LoadDataInfileColOption colOption, LoadDataInfileLineOption lineOption,
			ExpressionNode ignoredLines, List<Name> colOrVarList,
			List<ExpressionNode> updateExprs) {

		// validate this table is a qualified table or has a current database set
		Name dbName = null;
		Name tableName = tempTableName;
		if (tableName.isQualified()) {
			dbName = ((QualifiedName)tempTableName).getNamespace();
			tableName = ((QualifiedName)tempTableName).getUnqualified();
		}
		if (dbName == null) {
			dbName = (pc.getCurrentDatabase() == null) ? null : pc.getCurrentDatabase().getName();
		}
		if (dbName == null) {
			throw new SchemaException(Pass.FIRST, "No database has been specified.");
		}
		
		PEDatabase db = pc.findPEDatabase(dbName);
		if (db == null) {
			throw new SchemaException(Pass.FIRST, "No such database: '" + dbName + "'");
		}

		if (colOption != null) {
			if ((colOption.getEnclosed() != null) && (colOption.getEnclosed().length() > 1)) {
				throw new SchemaException(Pass.FIRST, "Fields enclosed by must be a single character.  Value is '" + colOption.getEnclosed() + "'");
			}
			if ((colOption.getEscaped() != null) && (colOption.getEscaped().length() > 1)) {
				throw new SchemaException(Pass.FIRST, "Fields escaped by must be a single character.  Value is '" + colOption.getEscaped() + "'");
			}
		}

		if (replace) {
			throw new SchemaException(Pass.FIRST, "REPLACE option is not currently supported.");
		}
	
		if (ignoredLines != null) {
			throw new SchemaException(Pass.FIRST, "Ignoring lines is not currently supported.");
		}
		
		if (updateExprs != null) {
			throw new SchemaException(Pass.FIRST, "SET option is not currently supported.");
		}
		
		LoadDataInfileStatement stmt = new LoadDataInfileStatement(modifier, local, PEStringUtils.dequote(fileName), replace, ignore, db, 
				tableName, characterSet, colOption, lineOption, 
				(ignoredLines == null) ? null : asIntegralLiteral(ignoredLines), colOrVarList, updateExprs);
		return stmt;
	}

	private static String[] unpackXmlParams(List<Pair<String,String>> params, String[] tags) {
		String[] out = new String[tags.length];
		for(Pair<String,String> p : params) {
			String t = p.getFirst();
			for(int i = 0; i < tags.length; i++) {
				if (tags[i].equals(t)) {
					if (out[i] != null)
						throw new SchemaException(Pass.SECOND, "Duplicate " + t + " specified");
					if (p.getSecond() != null)
						out[i] = PEStringUtils.dequote(p.getSecond());
				}
			}
		}
		return out;
	}
	
	// 0=xml, 1=match, 2=comment
	private String[] unpackTemplateParams(List<Pair<String,String>> params) {
		return unpackXmlParams(params,new String[] { "xml", "match", "comment" });
	}
	
	public Statement buildCreateTemplateStatement(Name name, boolean ine, List<Pair<String,String>> params) {
		PETemplate pet = pc.findTemplate(name);
		if (pet != null) {
			if (ine)
				return buildEmptyStatement("create existing template");
			throw new SchemaException(Pass.SECOND, "Template " + name.getSQL() + " already exists");
		}
		String[] p = unpackTemplateParams(params);
		if (p[0] == null)
			throw new SchemaException(Pass.SECOND, "Must specify template body for add template");
		pet = new PETemplate(pc,name.getUnqualified(),p[0],p[1],p[2]);
		return new PECreateStatement<PETemplate,PersistentTemplate>(pet,true,"TEMPLATE",false);
	}
	
	public Statement buildAlterTemplateStatement(Name name, List<Pair<String,String>> params) {
		PETemplate pet = pc.findTemplate(name);
		if (pet == null)
			throw new SchemaException(Pass.SECOND, "Template " + name.getSQL() + " does not exist");
		String[] p = unpackTemplateParams(params);
		return new PEAlterTemplateStatement(pet,p[0],p[1],p[2]);
	}
	
	public Statement buildDropTemplateStatement(Name name, boolean ifExists) {
		PETemplate pet= pc.findTemplate(name);
		if (pet == null) {
			if (!ifExists)
				throw new SchemaException(Pass.SECOND, "Template " + name.getSQL() + " does not exist");
			return new PEDropStatement<PETemplate,PersistentTemplate>(PETemplate.class,true,true,name,"TEMPLATE");
		}
		return new PEDropStatement<PETemplate,PersistentTemplate>(PETemplate.class,ifExists,true,pet,"TEMPLATE");
	}

	// 0=xml, 1=comment, 2=enabled
	private String[] unpackRawPlanParams(List<Pair<String,String>> params) {
		return unpackXmlParams(params,new String[] { "xml", "comment", "enabled" });
	}

	
	public Statement buildCreateRawPlanStatement(Name name, boolean ine, Name dbName, List<Pair<String,String>> params) {
		PERawPlan perp = pc.findRawPlan(name);
		if (perp != null) {
			if (ine)
				return buildEmptyStatement("create existing plan");
			throw new SchemaException(Pass.SECOND, "Raw plan " + name + " already exists");
		}
		PEDatabase pdb = pc.findPEDatabase(dbName);
		if (pdb == null)
			throw new SchemaException(Pass.SECOND, "No such database: " + dbName.getSQL());
		// unpack the params
		String[] fields = unpackRawPlanParams(params);
		perp = new PERawPlan(pc,name.getUnqualified(),pdb,fields[0],(fields[2] == null ? true : Boolean.valueOf(fields[2])),fields[1]);
		return new PECreateRawPlanStatement(perp,ine);
	}
	
	public Statement buildDropRawPlanStatement(Name name, boolean ie) {
		PERawPlan perp = pc.findRawPlan(name);
		if (perp == null) {
			if (ie)
				return buildEmptyStatement("drop nonexistent raw plan");
			throw new SchemaException(Pass.SECOND, "Raw plan " + name + " does not exist");
		}
		return new PEDropRawPlanStatement(perp,ie);
	}
	
	public Statement buildAlterRawPlanStatement(Name name, List<Pair<String,String>> params) {
		PERawPlan perp = pc.findRawPlan(name);
		if (perp == null)
			throw new SchemaException(Pass.SECOND,"No such raw plan: " + name);
		// 0=xml, 1=comment, 2=enabled
		String[] fields = unpackRawPlanParams(params);
		return new PEAlterRawPlanStatement(perp,fields[1],(fields[2] == null ? null : Boolean.valueOf(fields[2])),fields[0]);
	}
	
	public Statement buildCreateViewStatementKern(Name viewName, ProjectingStatement viewDef,
			List<UnqualifiedName> columnNames, String checkOption, List<TableComponent<?>> colDefs) {
		return PECreateViewStatement.build(pc,viewName,viewDef,columnNames,checkOption,colDefs);
	}
	
	public Statement buildDropViewStatement(Name viewName, boolean ifExists) {
		if (pc.getCapability() == Capability.PARSING_ONLY)
			return new PEDropViewStatement(viewName, ifExists);
		UnqualifiedName tableName = viewName.getUnqualified();
		Database<?> ondb = findDatabase(viewName);
		if (ondb == null)
			throw new SchemaException(Pass.SECOND,
					"No such database: '" + viewName + "'");
		if (!(ondb instanceof PEDatabase))
			throw new SchemaException(Pass.SECOND,
					"Invalid database for drop view: '" + ondb.getName()
							+ "'");
		PEAbstractTable<?> exists = pc.findTable(PEAbstractTable.getTableKey((PEDatabase)ondb, tableName));
		if (exists == null) {
			if (!ifExists)
				throw new SchemaException(Pass.SECOND, "No such view: " + viewName);
			return new PEDropViewStatement(viewName,ifExists);
		}
		return new PEDropViewStatement(exists.asView(),ifExists);
	}
	
	public Statement buildPreparePreparedStatement(Name pstmtName, String stmt) {
		return new PreparePStmtStatement(pstmtName.getUnqualified(), PEStringUtils.dequote(stmt));
	}
	
	public Statement buildExecutePreparedStatement(Name pstmtName, List<VariableInstance> vars) {
		return new ExecutePStmtStatement(pstmtName.getUnqualified(), vars);
	}
	
	public Statement buildDeallocatePreparedStatement(Name pstmtName) {
		return new DeallocatePStmtStatement(pstmtName.getUnqualified());
	}	
	
	public Statement buildShowPlanCache(boolean statsToo) {
		pc.getPolicyContext().checkRootPermission("show plan cache");
		return new ShowPlanCacheStatement(statsToo);
	}
	
	public Statement buildXACommit(UserXid xid, Object onePhase) {
		return new XACommitTransactionStatement(xid, onePhase != null);
	}
	
	public Statement buildXAEnd(UserXid xid, Object suspend, Object forMigrate) {
		if (forMigrate != null)
			throw new SchemaException(Pass.SECOND, "No support for xa end suspend for migrate");
		if (suspend != null)
			throw new SchemaException(Pass.SECOND, "No support for xa end suspend");
		return new XAEndTransactionStatement(xid);
	}
	
	public Statement buildXAPrepare(UserXid xid) {
		return new XAPrepareTransactionStatement(xid);
	}
	
	public Statement buildXARecover() {
		return new XARecoverTransactionStatement();
	}
	
	public Statement buildXARollback(UserXid xid) {
		return new XARollbackTransactionStatement(xid);
	}
	
	public Statement buildXAStart(UserXid xid, Object join, Object resume) {
		if (join != null)
			throw new SchemaException(Pass.SECOND, "No support for xa start join");
		if (resume != null)
			throw new SchemaException(Pass.SECOND, "No support for xa start resume");
		return new XABeginTransactionStatement(xid);
	}
	
	public UserXid buildXAXid(String gtrid, String bqual, String formatID) {
		// not quite right yet - we should convert based on the type of the literal
		return new UserXid(gtrid,bqual,formatID);
	}
	
	public void pushDML(String reason) {
		if (lockInfo == null && (opts != null && !opts.isIgnoreLocking())) {
			com.tesora.dve.lockmanager.LockType lt = StatementTraits.getLockType(reason);
			if (lt == null)
				throw new SchemaException(Pass.FIRST, "Missing lock type for dml reason " + reason);
			lockInfo = new LockInfo(lt,reason);			
		}
	}
	
	public NativeCharSetCatalog getNativeCharSetCatalog() { 
		if (supportedCharSets == null) {
			supportedCharSets = Singletons.require(HostService.class).getDBNative().getSupportedCharSets();
		}
		return supportedCharSets;
	}
	
	public NativeCollationCatalog getNativeCollationCatalog() { 
		if (supportedCollations == null) {
			supportedCollations = Singletons.require(HostService.class).getDBNative().getSupportedCollations();
		}
		return supportedCollations;
	}

	public Statement buildCompoundStatement(List<Statement> stmts) {
		return new CompoundStatementList(null,stmts);
	}
	
	public Statement buildCreateTrigger(Name triggerName, boolean isBefore, TriggerEvent triggerType,
			PETable targetTable, Statement body,Token triggerToken) {
		PETrigger already = pc.findTrigger(PETrigger.buildCacheKey(triggerName.getUnquotedName().get(), (TableCacheKey) targetTable.getCacheKey()));
		if (already != null)
			// todo: come back and do the right err msg
			throw new SchemaException(Pass.SECOND,"Trigger " + triggerName + " already exists");
		
    	String origStmt = getInputSQL();
    	int l = triggerToken.getCharPositionInLine();
    	String rawSQL = origStmt.substring(l);
		
		String charset =
				KnownVariables.CHARACTER_SET_CLIENT.getSessionValue(pc.getConnection().getVariableSource()).getName();
		String collation = 
				KnownVariables.COLLATION_CONNECTION.getSessionValue(pc.getConnection().getVariableSource());
		String collationDB =
				KnownVariables.COLLATION_DATABASE.getSessionValue(pc.getConnection().getVariableSource());
		SQLMode sqlMode =
				KnownVariables.SQL_MODE.getSessionValue(pc.getConnection().getVariableSource());
		SQLMode globalMode =
				KnownVariables.SQL_MODE.getGlobalValue(pc.getConnection().getVariableSource());
		if (globalMode.equals(sqlMode))
			sqlMode = null;
		
		PETrigger trig = new PETrigger(pc,triggerName,targetTable,body,triggerType,
				null /* PEUser user */,
				collation,
				charset,
				collationDB,
				isBefore,
				sqlMode,rawSQL);
		popScope();
		opts = opts.setResolve();

		return new PECreateTriggerStatement(trig);
	}
	
	public Statement addViewTriggerFields(Statement in, boolean createOrReplace, String algo, UserScope definer, String security) {
		PECreateStatement pect = (PECreateStatement) in;
		Persistable pt = pect.getCreated();
		
		PEUser user = null;
		if (definer == null || pc.getOptions().isIgnoreMissingUser())
			user = pc.getCurrentUser().get(pc);
		else {
			user = pc.findUser(definer.getUserName(), definer.getScope());
			if (user == null)
				// apparently it is legal to specify a user that doesn't exist.  in that case, just use the current user
				user = pc.getCurrentUser().get(pc);
				// throw new SchemaException(Pass.SECOND, "No such user: " + definer.getSQL());
		}

		
		if (pt instanceof PEView) {
			PECreateViewStatement cvs = (PECreateViewStatement) pect;
			PEView pev = (PEView) pt;
			pev.setUser(pc, user, false);
			
			PEDatabase theDB = cvs.getDatabase(pc);

			PEAbstractTable<?> existing = pc.findTable(PEAbstractTable.getTableKey(theDB, pev.getName()));
			if (existing != null && !createOrReplace) {
				throw new SchemaException(Pass.SECOND, "View " + pev.getName() + " already exists");
			}

			if (createOrReplace) {
				((PECreateViewStatement)pect).setCreateOrReplace();
			}
			
			String algorithm = (algo == null ? "UNDEFINED" : algo);
			String sec = (security == null ? "DEFINER" : security);

			pev.setAlgorithm(algorithm);
			pev.setSecurity(sec);
			
		} else {
			PETrigger trig = (PETrigger) pt;
			trig.setUser(pc,user);
			if (createOrReplace || algo != null || security != null) {
				// TODO:
				// come back and put in the right error message
				throw new SchemaException(Pass.FIRST, "Illegal syntax");
			}
		}
		return pect;
	}
	
	public PETable pushTriggerTable(Name n) {
		TableInstance targTab = basicResolver.lookupTable(pc, n, lockInfo);
		PETable theTable = targTab.getAbstractTable().asTable();
		TriggerTableInstance before = new TriggerTableInstance(theTable,true);
		TriggerTableInstance after = new TriggerTableInstance(theTable,false);
		pushScope();
		scope.insertTable(before);
		scope.insertTable(after);
		opts = opts.clearSetting(Option.RESOLVE);
		return theTable;
	}
	
	public PETable pushTriggerTable(PETable tab) {
		TriggerTableInstance before = new TriggerTableInstance(tab,true);
		TriggerTableInstance after = new TriggerTableInstance(tab,false);
		pushScope();
		scope.insertTable(before);
		scope.insertTable(after);
		return tab;
	}
	
	public Statement buildCaseStatement(ExpressionNode testExpr, List<StatementWhenClause> whenClauses, Statement elseStatement) {
		return new CaseStatement(null,testExpr, whenClauses, elseStatement);
	}
	
	public StatementWhenClause buildStatementWhenClause(ExpressionNode testExpr, Statement result) {
		return new StatementWhenClause(testExpr,result,null);
		
	}
}
