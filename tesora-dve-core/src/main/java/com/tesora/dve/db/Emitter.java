package com.tesora.dve.db;

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
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.TokenStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.InformationSchemaTable;
import com.tesora.dve.sql.infoschema.direct.DirectInfoSchemaStatement;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.CastFunctionCall;
import com.tesora.dve.sql.node.expression.CharFunctionCall;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConvertFunctionCall;
import com.tesora.dve.sql.node.expression.Default;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.ExpressionSet;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.GroupConcatCall;
import com.tesora.dve.sql.node.expression.IdentifierLiteralExpression;
import com.tesora.dve.sql.node.expression.IntervalExpression;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.node.expression.LateResolvingVariableExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameInstance;
import com.tesora.dve.sql.node.expression.RandFunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.expression.WildcardTable;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Comment;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.ForeignKeyAction;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEPolicy;
import com.tesora.dve.sql.schema.PEProject;
import com.tesora.dve.sql.schema.PERawPlan;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.PETrigger;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.PEView;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.schema.cache.ConstantType;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.schema.modifiers.TableModifiers;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.AddGlobalVariableStatement;
import com.tesora.dve.sql.statement.ddl.AddStorageSiteStatement;
import com.tesora.dve.sql.statement.ddl.AlterDatabaseStatement;
import com.tesora.dve.sql.statement.ddl.AlterDatabaseTemplateStatement;
import com.tesora.dve.sql.statement.ddl.AlterStatement;
import com.tesora.dve.sql.statement.ddl.AlterTableDistributionStatement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.ddl.GrantStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterExternalServiceStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterPersistentSite;
import com.tesora.dve.sql.statement.ddl.PEAlterPolicyStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterRawPlanStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterSiteInstanceStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTableStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTemplateStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTenantStatement;
import com.tesora.dve.sql.statement.ddl.PECreateStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTriggerStatement;
import com.tesora.dve.sql.statement.ddl.PECreateViewStatement;
import com.tesora.dve.sql.statement.ddl.PEDropStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTableStatement;
import com.tesora.dve.sql.statement.ddl.PEGroupProviderDDLStatement;
import com.tesora.dve.sql.statement.ddl.PEQueryVariablesStatement;
import com.tesora.dve.sql.statement.ddl.RenameTableStatement;
import com.tesora.dve.sql.statement.ddl.SetPasswordStatement;
import com.tesora.dve.sql.statement.ddl.alter.AddColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.AddIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeKeysStatusAction;
import com.tesora.dve.sql.statement.ddl.alter.ChangeTableModifierAction;
import com.tesora.dve.sql.statement.ddl.alter.ConvertToAction;
import com.tesora.dve.sql.statement.ddl.alter.DropColumnAction;
import com.tesora.dve.sql.statement.ddl.alter.DropIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.RenameTableAction;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.MysqlSelectOption;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.statement.dml.compound.CaseStatement;
import com.tesora.dve.sql.statement.dml.compound.CompoundStatement;
import com.tesora.dve.sql.statement.dml.compound.CompoundStatementList;
import com.tesora.dve.sql.statement.dml.compound.StatementWhenClause;
import com.tesora.dve.sql.statement.session.AnalyzeKeysStatement;
import com.tesora.dve.sql.statement.session.ExternalServiceControlStatement;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.sql.statement.session.LockStatement;
import com.tesora.dve.sql.statement.session.LockType;
import com.tesora.dve.sql.statement.session.PStmtStatement;
import com.tesora.dve.sql.statement.session.RollbackTransactionStatement;
import com.tesora.dve.sql.statement.session.SavepointStatement;
import com.tesora.dve.sql.statement.session.SessionSetVariableStatement;
import com.tesora.dve.sql.statement.session.SessionStatement;
import com.tesora.dve.sql.statement.session.SetExpression;
import com.tesora.dve.sql.statement.session.SetTransactionIsolationExpression;
import com.tesora.dve.sql.statement.session.SetVariableExpression;
import com.tesora.dve.sql.statement.session.ShowErrorsWarningsStatement;
import com.tesora.dve.sql.statement.session.ShowPassthroughStatement;
import com.tesora.dve.sql.statement.session.ShowProcesslistStatement;
import com.tesora.dve.sql.statement.session.ShowSitesStatusStatement;
import com.tesora.dve.sql.statement.session.TableMaintenanceStatement;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.statement.session.UseContainerStatement;
import com.tesora.dve.sql.statement.session.UseDatabaseStatement;
import com.tesora.dve.sql.statement.session.UseStatement;
import com.tesora.dve.sql.statement.session.UseTenantStatement;
import com.tesora.dve.sql.statement.session.XACommitTransactionStatement;
import com.tesora.dve.sql.statement.session.XATransactionStatement;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Options;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

// responsible for emitting the internal representation as sql
// presumably we will have different versions of this (via subclassing)
// for different databases or purposes
public abstract class Emitter {

	/**
	 * Convenience function object for in-place Emitter invocations.
	 */
	public static abstract class EmitterInvoker {

		private final Emitter emitter;

		public EmitterInvoker(Emitter emitter) {
			this.emitter = emitter;
		}
		
		public final String getValueAsString(final SchemaContext sc) {
			final StringBuilder buf = new StringBuilder();
			this.emitStatement(sc, buf);

			return buf.toString();
		}

		public final GenericSQLCommand buildGenericCommand(final SchemaContext sc) throws PEException {
			final StringBuilder buf = new StringBuilder();
			this.emitStatement(sc, buf);

			return this.emitter.buildGenericCommand(sc, buf.toString());
		}

		public final Emitter getEmitter() {
			return this.emitter;
		}

		protected abstract void emitStatement(final SchemaContext sc, final StringBuilder buf);
	}

	protected static Logger logger = Logger.getLogger(Emitter.class);
	
	protected EmitOptions options;
	
	protected EmitContext cntxt;
	
	protected GenericSQLCommand.Builder builder = new GenericSQLCommand.Builder();
	
	public Emitter() {
	}
	
	public GenericSQLCommand buildGenericCommand(final SchemaContext sc, String format) {
		return builder.build(sc, format);
	}
	
	public abstract Emitter buildNew();
	
	public void startGenericCommand() {
		builder = new GenericSQLCommand.Builder();
	}
	
	public String getLineTerminator() {
		return "\n";
	}
	
	public void setOptions(EmitOptions opts) {
		options = opts;
	}
	
	public EmitOptions getOptions() { 
		return options;
	}

	public boolean hasOptions() {
		return this.options != null;
	}

	public void pushContext(TokenStream in) {
		cntxt = new EmitContext(in);
	}

	public void popContext() {
		cntxt = null;
	}
	
	public EmitContext getEmitContext() {
		return cntxt;
	}
		
	public boolean emitExtensions() {
		if (options == null)
			return false;
		return options.emitPEMetadata();
	}
	
	protected boolean isResultSetMetadata() {
		return (this.hasOptions() && getOptions().isResultSetMetadata());
	}
	
	protected int bumpIndent(int in) {
		if (in == -1) return in;
		return in + 1;
	}
	
	public abstract String getPersistentName(SchemaContext sc, ConnectionValues cv, PEAbstractTable<?> t);
	public abstract String getPersistentName(PEColumn c);

	public abstract <T> Lookup<T> getLookup();
	
	protected void error(String message) {
		RuntimeException re = new RuntimeException(message);
		logger.warn(re);
		throw re;
	}
	
	public void emitTraversable(SchemaContext sc, ConnectionValues cv, LanguageNode t, StringBuilder buf) {
		if (t instanceof Statement)
			emitStatement(sc,cv,(Statement)t, buf);
		else if (t instanceof ExpressionNode)
			emitExpression(sc, cv,(ExpressionNode)t, buf, -1);
		else
			error("Unknown node kind: " + t.getClass().getName());
	}
	
	public void emitStatement(SchemaContext sc, ConnectionValues cv, Statement s, StringBuilder buf) {
		if (s.getParent() == null)
			builder.withType(s.getStatementType());
		if (s instanceof DMLStatement) {
			emitDMLStatement(sc,cv,(DMLStatement)s, buf);
		} else if (s instanceof DDLStatement) {
			emitDDLStatement(sc,cv,(DDLStatement)s, buf);
		} else if (s instanceof SessionStatement) {
			emitSessionStatement(sc,cv,(SessionStatement)s, buf);
		} else if (s instanceof CompoundStatement) {
			emitCompoundStatement(sc,cv,(CompoundStatement)s, buf);
		} else if (s instanceof EmptyStatement) {
			// does nothing
		} else {
			error("Unknown statement kind for emitter: " + s.getClass().getName());
		}
	}
	
	public void emitDMLStatement(SchemaContext sc, ConnectionValues cv, DMLStatement s, StringBuilder buf) {
		emitDMLStatement(sc,cv,s,buf,0);
	}
		
	public void emitDMLStatement(SchemaContext sc, ConnectionValues cv, DMLStatement s, StringBuilder buf, int indent) {
		if (s instanceof SelectStatement) {
			emitSelectStatement(sc,cv,(SelectStatement)s, buf, indent);
		} else if (s instanceof UpdateStatement) {
			emitUpdateStatement(sc,cv,(UpdateStatement)s, buf, indent);
		} else if (s instanceof InsertIntoValuesStatement) {
			emitInsertStatement(sc,cv, (InsertIntoValuesStatement)s, buf);
		} else if (s instanceof DeleteStatement) {
			emitDeleteStatement(sc,cv,(DeleteStatement)s, buf, indent);
		} else if (s instanceof TruncateStatement) {
			emitTruncateStatement(sc,cv,(TruncateStatement)s, buf);
		} else if (s instanceof InsertIntoSelectStatement) {
			emitInsertIntoSelectStatement(sc,cv,(InsertIntoSelectStatement)s, buf, indent);
		} else if (s instanceof UnionStatement) {
			emitUnionStatement(sc,cv,(UnionStatement)s, buf, indent);
		} else {
			error("Unknown DML statement kind for emitter: " + s.getClass().getName());
		}
	}

	@SuppressWarnings("rawtypes")
	public void emitDDLStatement(SchemaContext sc, ConnectionValues cv, DDLStatement s, StringBuilder buf) {
		if (s instanceof GrantStatement) {
			emitGrantStatement((GrantStatement)s, buf);
		} else if (s instanceof PECreateStatement) {
			emitCreateStatement(sc,cv, (PECreateStatement)s, buf);
		} else if (s instanceof PEDropStatement) {
			emitDropStatement(sc,cv,(PEDropStatement)s, buf);
		} else if (s instanceof AlterStatement) {
			emitAlterStatement(sc, cv,(AlterStatement)s, buf);
		} else if (s instanceof RenameTableStatement) {
			emitRenameStatement(sc, (RenameTableStatement) s, buf);
		} else if (s instanceof PEGroupProviderDDLStatement) {
			emitPEGroupProviderDDLStatement(sc, (PEGroupProviderDDLStatement)s,buf);
		} else if (s instanceof DirectInfoSchemaStatement) {
			emitDirectInfoSchemaStatement(sc, cv,(DirectInfoSchemaStatement)s, buf);
		} else if (s instanceof PEQueryVariablesStatement) {
			// nothing yet
		} else {
			error("Unknown DDL statement kind for emitter: " + s.getClass().getName());
		}
	}
	
	public void emitSessionStatement(SchemaContext sc, ConnectionValues cv, SessionStatement s, StringBuilder buf) {
		emitSessionStatement(sc, cv,s, buf, 0);
	}
	
	public void emitSessionStatement(SchemaContext sc, ConnectionValues cv, SessionStatement s, StringBuilder buf, int indent) {
		if (s instanceof UseStatement) {
			UseStatement us = (UseStatement)s;
			if (us instanceof UseDatabaseStatement) {
				emitUseDatabaseStatement(sc,(UseDatabaseStatement)us, buf, indent);				
			} else if (us instanceof UseTenantStatement) {
				emitUseTenantStatement((UseTenantStatement)us, buf, indent);
			} else {
				error("Unknown use statement target: " + us.getTarget().getClass().getName());
			}
		} else if (s instanceof UseDatabaseStatement) {
			emitUseDatabaseStatement(sc,(UseDatabaseStatement)s, buf, indent);
		} else if (s instanceof UseContainerStatement) {
			emitUseContainerStatement(sc, (UseContainerStatement)s, buf, indent);
		} else if (s instanceof SessionSetVariableStatement) {
			emitSessionSetVariableStatement(sc,cv,(SessionSetVariableStatement)s, buf, indent);
		} else if (s instanceof TransactionStatement) {
			emitTransactionStatement((TransactionStatement)s, buf, indent);
		} else if (s instanceof SavepointStatement) {
			emitSavepointStatement((SavepointStatement)s, buf, indent);
		} else if (s instanceof LockStatement) {
			emitLockStatement(sc,cv,(LockStatement)s, buf, indent);
		} else if (s instanceof ShowPassthroughStatement) {
			emitShowPassthroughStatement((ShowPassthroughStatement)s, buf, indent);
		} else if (s instanceof ExternalServiceControlStatement) {
			emitExternalServiceControlStatement((ExternalServiceControlStatement)s, buf, indent);
		} else if (s instanceof ShowProcesslistStatement) {
			emitShowProcesslistStatement((ShowProcesslistStatement)s, buf, indent);
		} else if (s instanceof ShowSitesStatusStatement) {
			emitShowSitesStatusStatement((ShowSitesStatusStatement)s, buf, indent);
		} else if (s instanceof TableMaintenanceStatement) {
			emitTableMaintenanceStatement(sc,cv,(TableMaintenanceStatement)s, buf, indent);
		} else if (s instanceof LoadDataInfileStatement) {
			emitLoadDataInfileStatement(sc,cv,(LoadDataInfileStatement)s, buf, indent);
		} else if (s instanceof AnalyzeKeysStatement) {
			emitAnalyzeKeysStatement(sc,cv,(AnalyzeKeysStatement)s,buf, indent);
		} else if (s.isAdhoc()) {
			emitAdhocSessionStatement(s,buf, indent);
		} else if (s instanceof ShowErrorsWarningsStatement) {
			emitShowErrorsWarningsStatement((ShowErrorsWarningsStatement)s,buf, indent);
		} else if (s instanceof PStmtStatement) {
			// ignore for right now
		} else {
			error("Unknown session statement kind for emitter: " + s.getClass().getName());
		}
	}
	
	@SuppressWarnings("rawtypes")
	public void emitDeclaration(SchemaContext sc, ConnectionValues cv, Persistable p, StringBuilder buf) {
		emitDeclaration(sc, cv, p, null, buf);
	}	
	
	@SuppressWarnings("rawtypes")
	public void emitDeclaration(SchemaContext sc, ConnectionValues cv, Persistable p, PECreateStatement cs, StringBuilder buf) {
		if (p instanceof PEColumn)
			emitColumnDeclaration(sc,cv,(PEColumn)p, buf);
		else if (p instanceof PETable) {
			PETable pet = (PETable) p;
			if (pet.isUserlandTemporaryTable())
				buf.append(" TEMPORARY");
			buf.append(" TABLE ");
			emitTable(sc,cv,pet,sc.getCurrentDatabase(false),buf);
			emitTableDeclaration(sc,cv,(PETable)p, buf);
		}
		else if (p instanceof PESiteInstance)
			emitSiteInstanceDeclaration((PESiteInstance)p, buf);
		else if (p instanceof PEStorageSite)
			emitStorageSiteDeclaration((PEStorageSite)p, buf);
		else if (p instanceof PEPersistentGroup)
			emitStorageGroupDeclaration(sc,(PEPersistentGroup)p, buf);
		else if (p instanceof PEProject)
			emitProjectDeclaration((PEProject)p, buf);
		else if (p instanceof PEDatabase)
			emitDatabaseDeclaration(sc,(PEDatabase)p, cs, buf);
		else if (p instanceof RangeDistribution)
			emitRangeDistributionDeclaration(sc,(RangeDistribution)p, buf);
		else if (p instanceof PEUser)
			emitUserDeclaration((PEUser)p, buf);
		else if (p instanceof PETenant)
			emitTenantDeclaration((PETenant)p, buf);
		else if (p instanceof PEContainer)
			emitContainerDeclaration(sc, (PEContainer)p, buf);
		else if (p instanceof PEPolicy) {
			// no decl for now
		}
		else if (p instanceof PEExternalService) {
			emitExternalServiceDeclaration((PEExternalService)p, buf);
		} else if (p instanceof PETemplate) {
			emitTemplateDeclaration((PETemplate)p, buf);
		} else if (p instanceof PERawPlan) {
			// ignore for now
		} else if (p instanceof PEViewTable) {
			emitViewDeclaration(sc,cv,((PEViewTable)p).getView(sc),(PECreateViewStatement)cs,buf);
		} else if (p instanceof PEView) {
			emitViewDeclaration(sc,cv,(PEView)p,(PECreateViewStatement)cs,buf);
		} else if (p instanceof PETrigger) {
			emitTriggerDeclaration(sc,cv,(PETrigger)p,(PECreateTriggerStatement)cs,buf);
		}
		else
			error("Unknown persistable kind: " + p.getClass().getName());
	}
	
	public void emitColumnDeclaration(final SchemaContext sc, final ConnectionValues cv, final PEColumn c, final StringBuilder buf) {
		final Type columnType = c.getType();
		buf.append(c.getName().getQuotedName().getSQL()).append(" ");
		emitDeclaration(columnType, c, buf, true);
		// fixed order for the attributes
		// here is the overall declaration order for a column
		// name (root-type) (binary) (length/precision) (character set charset_name) (collate collation_name) (unsigned) (zerofill)
		// (not null | null) (default 'default_value') (on update ...) auto_increment (comparison '') (comment '')

		/*
		// charset
		UnqualifiedName unq = c.getCharset();
		if (unq != null)
			buf.append(" CHARACTER SET ").append(unq.getSQL());
		// collation
		unq = c.getCollation();
		if (unq != null)
			buf.append(" COLLATE ").append(unq.getSQL());
			*/
		// unsigned
		// zerofill
		// not nullable
		if (c.isNotNullable()) {
			buf.append(" NOT NULL");
		} else if (columnType.isTimestampType()) {
			buf.append(" NULL");
		}

		// default value
		if (c.getDefaultValue() != null) {
			buf.append(" DEFAULT ");
			String defaultValue = new EmitterInvoker(buildNew()) {
				@Override
				protected void emitStatement(final SchemaContext sc, final StringBuilder buf) {
					emitExpression(sc, cv, c.getDefaultValue(), buf, -1);
				}
			}.getValueAsString(sc);
			
			/*
			 * The default value must be a constant. It cannot be a function or
			 * an expression. The exception is that you can specify
			 * CURRENT_TIMESTAMP as the default for TIMESTAMP and DATETIME
			 * columns.
			 * 
			 * TODO: In numeric contexts, hexadecimal values act like integers
			 * (64-bit precision). In string contexts, they act like binary
			 * strings.
			 * We currently push the hex value down and let the native do the
			 * job, in order to exactly match the output of "SHOW CREATE", we
			 * may need to resolve them here using the same charset as the one
			 * used by the underlying database.
			 */
			if (!defaultValue.equalsIgnoreCase("NULL")
					&& !(columnType.isTimestampType()
							&& defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")
							|| defaultValue.equalsIgnoreCase("0"))
					&& !PEStringUtils.isHexNumber(defaultValue)) {

				if (columnType.isBitType()) {
					if (defaultValue.length() == 1) { // Transform 0 and 1 literals to b'value' notation.
						defaultValue = 'b' + PEStringUtils.singleQuote(defaultValue);
					}
				} else {
					if (columnType.isFloatType()) { // Transform float literals that can be expressed as integers.
						defaultValue = PEStringUtils.trimToInt(PEStringUtils.dequote(defaultValue));
					}

					defaultValue = PEStringUtils.singleQuote(defaultValue);
				}

			}
			
			buf.append(defaultValue);
		}

		// on update
		if (c.isOnUpdated()) {
			buf.append(" ON UPDATE CURRENT_TIMESTAMP");
		}

		// auto increment
		if (c.isAutoIncrement() && this.hasOptions() && ((getOptions().isTableDefinition()) || getOptions().isExternalTableDeclaration())) {
			buf.append(" AUTO_INCREMENT");
		}

		// comparison
		// comment
		emitComment(c.getComment(), buf);		
	}

	public abstract void emitComment(Comment c, StringBuilder buf);
	
	public String emitExternalCreateTableStatement(SchemaContext sc, ConnectionValues cv,PETable t, boolean omitDists) {
		StringBuilder buf = new StringBuilder();
		setOptions(omitDists ? EmitOptions.TEST_TABLE_DECLARATION : EmitOptions.EXTERNAL_TABLE_DECLARATION);
		buf.append("CREATE");
		if (t.isUserlandTemporaryTable())
			buf.append(" TEMPORARY");
		buf.append(" TABLE ").append(t.getName().getQuotedName().getSQL());
		emitTableDeclaration(sc, cv,t,buf);
		return buf.toString();		
	}
	
	public String emitCreateTableStatement(SchemaContext sc, ConnectionValues cv, PEAbstractTable<?> t) {
		StringBuilder buf = new StringBuilder();
		buf.append("CREATE");
		if (t.isUserlandTemporaryTable())
			buf.append(" TEMPORARY");
		buf.append(" TABLE ");
		emitTable(sc,cv,t,sc.getCurrentDatabase(false),buf);
		emitTableDeclaration(sc, cv,t,buf);
		return buf.toString();
	}
	
	public String emitTableDefinition(SchemaContext sc, ConnectionValues cv, PETable t) {
		StringBuilder buf = new StringBuilder();
		setOptions(EmitOptions.TABLE_DEFINITION);
		emitTableDeclaration(sc, cv,t,buf);
		return buf.toString();
	}
	
	protected void emitColumnDeclarations(final SchemaContext sc, final ConnectionValues cv, List<PEColumn> columns, String newline, 
			final boolean externalTableDecl, StringBuilder buf) {
		Functional.join(columns, buf, "," + newline, new BinaryProcedure<PEColumn, StringBuilder>() {

			@Override
			public void execute(PEColumn aobj, StringBuilder bobj) {
				if (externalTableDecl)
					bobj.append("  ");
				emitDeclaration(sc, cv, aobj, bobj);
			}
			
		});
		
	}
	
	public void emitTableDeclaration(final SchemaContext sc, final ConnectionValues cv, PEAbstractTable<?> t, StringBuilder buf) {
		// if pretty printing, add a newline after each column
		String newline = null;
		final boolean isExternalTableDecl = this.hasOptions() && getOptions().isExternalTableDeclaration();
		if (isExternalTableDecl)
			newline = getLineTerminator();
		else 
			newline = "";
		buf.append(" (").append(newline);
		emitColumnDeclarations(sc,cv, t.getColumns(sc),newline, isExternalTableDecl,buf);
		if (!t.getKeys(sc).isEmpty()) {
			buf.append(",").append(newline);
			List<PEKey> filtered = null;
			if (this.hasOptions() && (getOptions().isExternalTableDeclaration() || getOptions().isTableDefinition()))
				filtered = t.getKeys(sc);
			else
				filtered = Functional.select(t.getKeys(sc), new UnaryPredicate<PEKey>() {

					@Override
					public boolean test(PEKey object) {
						if (!object.isForeign()) return true;
						PEForeignKey pefk = (PEForeignKey) object;
						return pefk.isPersisted();
					}
					
				});
			Functional.join(filtered, buf, "," + newline, new BinaryProcedure<PEKey, StringBuilder>() {

				@Override
				public void execute(PEKey aobj, StringBuilder bobj) {
					if (isExternalTableDecl)
						bobj.append("  ");
					emitDeclaration(sc, cv, aobj, bobj);
				}

			});
		}

		buf.append(newline).append(")");
		if (t.isTable()) {
			emitTableModifiers(sc, t.asTable(),t.asTable().getModifiers(), buf);
			emitComment(t.asTable().getComment(), buf);
		}

		boolean emitDistVect = (sc != null && t.getEnclosingDatabaseMTMode(sc) == MultitenantMode.OFF); 
		
		boolean omitDistVect = (this.hasOptions() && getOptions().isOmitDistVect());
		
		if (!omitDistVect &&
				((emitExtensions() || (isExternalTableDecl && emitDistVect)) && t.getDistributionVector(sc) != null)) {
			if (isExternalTableDecl) 
				buf.append(" /*#dve ");
			emitDeclaration(sc,t.getDistributionVector(sc), buf);
			if (isExternalTableDecl)
				buf.append(" */");
		}

	}
	
	public abstract void emitTableModifiers(SchemaContext sc,PETable tab, TableModifiers mods, StringBuilder buf);
	
	public void emitDeclaration(Type t, PEColumn c, StringBuilder buf, boolean emitSizing) {
		buf.append(t.getTypeName());
		if (emitSizing)
			emitTypeSize(t,buf);
		// the order of modifiers is important
//		if (t.isBinaryText()) 
//			buf.append(" BINARY");
		if (((c != null) && c.shouldEmitCharset()) || ((c == null) && (t.isBinaryText() && (t.getCharset() != null)))) {
			buf.append(" CHARACTER SET ").append(t.getCharset().getSQL());
		}
		if (((c != null) && c.shouldEmitCollation()) || ((c == null) && (t.getCollation() != null))) {
			buf.append(" COLLATE ").append(t.getCollation().getSQL());
		}
		if (t.isUnsigned())
			buf.append(" unsigned");
		if (t.isZeroFill())
			buf.append(" zerofill");
		// what to do about comparison?
		if (t.getComparison() != null && this.hasOptions() && getOptions().isExternalTableDeclaration())
			buf.append(" /*#dve comparator '").append(t.getComparison()).append("' */");
	}

	public void emitConvertTypeDeclaration(Type t, StringBuilder buf) {
		// if it's an integral type we swap the order of the int unsigned (don't ask me why)
		if (t.isIntegralType()) {
			if (t.isUnsigned())
				buf.append("UNSIGNED ");
			else
				buf.append("SIGNED ");
			buf.append("INTEGER");
		} else {
			buf.append(t.getTypeName());
			emitTypeSize(t,buf);
		}
	}

	protected void emitTypeSize(Type t, StringBuilder buf) {
		if (t.declUsesSizing() && t.hasSize()) {
			buf.append("(");
			if (t.hasPrecisionAndScale()) {
				buf.append(t.getPrecision()).append(",").append(t.getScale());
			} else {
				buf.append(t.getSize());
			}
			buf.append(")");
		}
	}
	
	public void emitDeclaration(SchemaContext sc, ConnectionValues cv, PEForeignKey key, StringBuilder buf) {
		// must be a foreign key constraint, go find it
		if (key.getSymbol() != null) {
			buf.append("CONSTRAINT ");
			if (this.hasOptions() && getOptions().isTableDefinition()) {
				buf.append(key.getSymbol().getQuoted());
			} else {
				buf.append(key.getPhysicalSymbol().getQuoted());
			}
			buf.append(" ");
		}
		buf.append("FOREIGN KEY ");
		if (key.getName() != null) {
			if (!this.hasOptions() || getOptions().isTableDefinition()) {
				buf.append(key.getName()).append(" ");
			}
		}
		buf.append("(");
		boolean first = true;
		for(PEKeyColumnBase pekc : key.getKeyColumns()) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
			if (first) first = false;
			else buf.append(", ");
			buf.append(pefkc.getColumn().getName().getQuotedName().getSQL());
		}
		buf.append(") REFERENCES ");
		if (key.isForward()) 
			buf.append(key.getTargetTableName(sc, getOptions() != null && getOptions().isQualifiedTables()));
		else
			emitTable(sc,cv,key.getTargetTable(sc),key.getTable(sc).getPEDatabase(sc),buf);
		buf.append(" (");
		first = true;
		for(PEKeyColumnBase pekc : key.getKeyColumns()) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
			if (first) first = false;
			else buf.append(", ");
			buf.append(pefkc.getTargetColumnName().getQuotedName().getSQL());
		}
		buf.append(")");

		final DBNative nativeDb = Singletons.require(HostService.class).getDBNative();
		final ForeignKeyAction onDeleteFkAction = key.getDeleteAction();
		final ForeignKeyAction onUpdateFkAction = key.getUpdateAction();
		if ((onDeleteFkAction != null) && (onDeleteFkAction != nativeDb.getDefaultOnDeleteAction())) {
			buf.append(" ON DELETE ").append(onDeleteFkAction.getSQL());
		}
		if ((onUpdateFkAction != null) && (onUpdateFkAction != nativeDb.getDefaultOnUpdateAction())) {
			buf.append(" ON UPDATE ").append(onUpdateFkAction.getSQL());
		}
	}
	
	public void emitDeclaration(SchemaContext sc, ConnectionValues cv, PEKey key, StringBuilder buf) {
		if (key.isForeign()) {
			emitDeclaration(sc, cv, (PEForeignKey)key, buf);
			return;
		}
		if (key.getConstraint() != null) {
			if (key.getSymbol() != null && key.getConstraint() != ConstraintType.PRIMARY && key.getConstraint() != ConstraintType.UNIQUE) 
				buf.append("CONSTRAINT ").append(key.getSymbol().getQuoted()).append(" ");
			buf.append(key.getConstraint().getSQL()).append(" ");
		}
		if (key.getType() == IndexType.FULLTEXT)
			buf.append("FULLTEXT ");
		buf.append("KEY ");

		if (key.getName() != null && !key.isPrimary())
			buf.append(key.getName().getQuoted()).append(" ");

		// if the index type is not btree or fulltext, emit it
		if (key.getType() != IndexType.FULLTEXT && key.getType() != IndexType.BTREE)
			buf.append("USING ").append(key.getType().getSQL()).append(" ");

		buf.append("(");
		Functional.join(key.getKeyColumns(), buf, ",", new BinaryProcedure<PEKeyColumnBase, StringBuilder>() {

			@Override
			public void execute(PEKeyColumnBase aobj, StringBuilder bobj) {
				bobj.append(aobj.getName().getQuotedName().getSQL());
				if (aobj.getLength() != null)
					bobj.append("(").append(aobj.getLength()).append(")");
			}
			
		});
		buf.append(")");

		if (key.getComment() != null) {
			emitComment(key.getComment(),buf);
		}
		
//		if (emitExtensions() && key.getCardinality() > -1) {
//			buf.append(" /*#dve CARDINALITY ").append(key.getCardinality()).append(" */");
//		}
	
	} 
		
	public void emitSiteInstanceDeclaration(PESiteInstance pesi, StringBuilder buf) {
		buf.append("PERSISTENT INSTANCE ").append(pesi.getName().getSQL());
		buf.append((pesi.getUrl()!=null) ? "," + PESiteInstance.OPTION_URL.toUpperCase() + "='" + pesi.getUrl() + "'": "");
		buf.append((pesi.getStatus()!=null) ? "," + PESiteInstance.OPTION_STATUS.toUpperCase() + "='" + pesi.getStatus() + "'" : "");
	}
	
	public void emitStorageSiteDeclaration(PEStorageSite pess, StringBuilder buf) {
		buf.append("PERSISTENT SITE ").append(pess.getDeclarationSQL());
	}

	public void emitStorageGroupDeclaration(SchemaContext sc, PEPersistentGroup pesg, StringBuilder buf) {
		buf.append("PERSISTENT GROUP ").append(pesg.getName().getSQL());
		List<PEStorageSite> sites = pesg.getSites(sc);
		if (!sites.isEmpty()) {
			buf.append(" ADD ");
			Functional.join(sites, buf, ",", new BinaryProcedure<PEStorageSite, StringBuilder>(){

				@Override
				public void execute(PEStorageSite aobj, StringBuilder bobj) {
					bobj.append(aobj.getName().getSQL());
				}
				
			});
		}
	}
	
	public void emitProjectDeclaration(PEProject pep, StringBuilder buf) {
		buf.append("PROJECT ").append(pep.getName().getSQL());
//		if (pep.getDefaultStorageGroup() != null)
//			buf.append(" DEFAULT PERSISTENT GROUP ").append(pep.getDefaultStorageGroup().getName().getSQL());
	}
	
	@SuppressWarnings("rawtypes")
	public void emitDatabaseDeclaration(SchemaContext sc, PEDatabase ped, PECreateStatement pecs, StringBuilder buf) {
		buf.append("DATABASE ");
		if (Boolean.TRUE.equals(pecs.isIfNotExists()))
			buf.append(" IF NOT EXISTS ");
		buf.append(ped.getName().getSQL());
		if (!StringUtils.isBlank(ped.getCharSet())) {
			buf.append(" CHARACTER SET ");
			buf.append(ped.getCharSet());
		}
		if (!StringUtils.isBlank(ped.getCollation())) {
			buf.append(" COLLATE ");
			buf.append(ped.getCollation());
		}
		if (emitExtensions()) {
			if (ped.getDefaultStorage(sc) != null) {
				buf.append(" DEFAULT PERSISTENT GROUP ").append(ped.getDefaultStorage(sc).getName().getSQL());
			}
			emitUsingTemplateClause(sc, ped, buf);
		}
	}

	private void emitUsingTemplateClause(SchemaContext sc, PEDatabase ped, StringBuilder buf) {
		if (ped.getTemplateName() != null) {
			buf.append(" USING TEMPLATE ").append(ped.getTemplateName());

			final TemplateMode templateMode = ped.getTemplateMode();
			if (!templateMode.isDefault()) {
				buf.append(" ").append(templateMode.toString());
			}
		}
	}
	
	public void emitRangeDistributionDeclaration(SchemaContext sc, RangeDistribution rd, StringBuilder buf) {
		buf.append("RANGE ").append(rd.getName().getSQL()).append(" (").append(rd.getSignature()).append(") ");
		buf.append("PERSISTENT GROUP ").append(rd.getStorageGroup(sc).getName().getSQL());
	}
	
	protected void emitIndent(StringBuilder buf, int indent, String what) {
		if (indent > -1) 
			builder.withPretty(buf.length(), indent);
		else
			buf.append(" ");
		buf.append(what);
	}
			
	public void emitSelectStatement(SchemaContext sc, ConnectionValues cv, SelectStatement s, StringBuilder buf, int indent) {
		emitIndent(buf,indent, "SELECT ");
		if (s.getSetQuantifier() != null)
			buf.append(s.getSetQuantifier().getSQL()).append(" ");
		if (s.getSelectOptions() != null) {
			for(MysqlSelectOption mso : s.getSelectOptions()) {
				buf.append(mso.toString()).append(" ");
			}
		}
		emitExpressions(sc,cv,s.getProjection(),buf, indent);
		if (!s.getTables().isEmpty()) {
			emitIndent(buf,indent, "FROM ");
			emitFromTableReferences(sc,cv,s.getTables(), buf, indent);
		}
		if (s.getWhereClause() != null) {
			emitIndent(buf,indent,"WHERE ");
			emitExpression(sc,cv,s.getWhereClause(), buf, indent);
		}
		if (!s.getGroupBys().isEmpty()) {
			emitIndent(buf, indent, "GROUP BY ");
			emitSortingSpecifications(sc, cv,s.getGroupBys(), buf, indent);
		}
		if (s.getHavingEdge().has()) {
			emitIndent(buf, indent, "HAVING ");
			emitExpression(sc, cv,s.getHavingExpression(),buf, indent);
		}
		if (!s.getOrderBys().isEmpty()) {
			emitIndent(buf, indent, "ORDER BY ");
			emitSortingSpecifications(sc, cv,s.getOrderBys(), buf, indent);
		}
		if (s.getLimit() != null) { 
			emitLimitSpecification(sc, cv,s.getLimit(), buf, indent);
		}
		if (s.isLocking()) {
			buf.append(" FOR UPDATE");
			builder.withForUpdate();
		}
	}
	
	public void emitUpdateStatement(SchemaContext sc, ConnectionValues cv, UpdateStatement update, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"UPDATE ");
		if (update.getIgnore()) {
			buf.append("IGNORE ");
		}
		if (!update.getTables().isEmpty()) 
			emitFromTableReferences(sc, cv, update.getTables(), buf, indent);
		emitIndent(buf,indent,"SET ");
		emitExpressions(sc, cv, update.getUpdateExpressions(), buf, indent);
		if (update.getWhereClause() != null) {
			emitIndent(buf,indent,"WHERE ");
			emitExpression(sc,cv, update.getWhereClause(), buf, indent);
		}
		if (!update.getOrderBys().isEmpty()) {
			emitIndent(buf,indent,"ORDER BY ");
			emitSortingSpecifications(sc, cv, update.getOrderBys(), buf, indent);
			buf.append(" ");
		}
		if (update.getLimit() != null) 
			emitLimitSpecification(sc, cv, update.getLimit(), buf, indent);
	}

	public void emitDeleteStatement(SchemaContext sc, ConnectionValues cv, DeleteStatement delete, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"DELETE ");
		if (delete.getTargetDeleteEdge().has() && delete.getOrderBysEdge().isEmpty() && delete.getLimit() == null)
			emitTableInstance(sc,cv, delete.getTargetDeleteEdge().get(),buf,TableInstanceContext.NAKED);
		emitIndent(buf,indent,"FROM ");
		emitFromTableReferences(sc,cv, delete.getTables(), buf, indent);
		if (delete.getWhereClause() != null) {
			emitIndent(buf,indent,"WHERE ");
			emitExpression(sc,cv, delete.getWhereClause(), buf, indent);
		}
		if (!delete.getOrderBys().isEmpty()) {
			emitIndent(buf,indent,"ORDER BY ");
			emitSortingSpecifications(sc, cv, delete.getOrderBys(), buf, indent);
			buf.append(" ");
		}
		if (delete.getLimit() != null) {
			emitLimitSpecification(sc, cv, delete.getLimit(), buf,indent);
		}
	}
	
	public void emitTruncateStatement(SchemaContext sc, ConnectionValues cv, TruncateStatement ts, StringBuilder buf) {
		buf.append("TRUNCATE ");
		emitExpression(sc,cv,ts.getTruncatedTable(), buf, -1);
	}
	
	public void emitInsertValues(SchemaContext sc, ConnectionValues cv, List<List<ExpressionNode>> values, StringBuilder buf) {
		buf.append(" VALUES ");
		for(Iterator<List<ExpressionNode>> rowiter = values.iterator(); rowiter.hasNext();) {
			List<ExpressionNode> row = rowiter.next();
			buf.append("(");
			emitExpressions(sc,cv,row, buf, -1);
			buf.append(")");
			if (rowiter.hasNext()) {
				buf.append(",");
			}
		}		
	}
	
	public void emitInsertPrefix(SchemaContext sc, ConnectionValues cv, InsertStatement s, StringBuilder buf) {
		buf.append((s.isReplace() ? "REPLACE " : "INSERT "));
		if (s.getModifier() != null) {
			buf.append(s.getModifier().getSQL() + " ");
		}
		if (s.getIgnore()) {
			buf.append("IGNORE ");
		}
		buf.append("INTO ");
		emitExpression(sc,cv,s.getTableInstance(),buf, -1);
		if (s.getColumnSpecification() != null) {
			buf.append(" (");
			emitExpressions(sc, cv,s.getColumnSpecification(), buf, -1);
			buf.append(")");
		}		
	}
	
	public void emitInsertSuffix(SchemaContext sc, ConnectionValues cv, InsertStatement s, StringBuilder buf) {
		List<ExpressionNode> onDupValues = s.getOnDuplicateKey();
		emitInsertSuffix(sc,cv, onDupValues,buf);
	}
	
	public void emitInsertSuffix(SchemaContext sc, ConnectionValues cv, List<ExpressionNode> onDupValues, StringBuilder buf) {
		if (onDupValues != null && onDupValues.size() > 0) {
			buf.append(" ON DUPLICATE KEY UPDATE ");
			for(Iterator<ExpressionNode> rowiter = onDupValues.iterator(); rowiter.hasNext();) {
				ExpressionNode row = rowiter.next();
				emitExpression(sc, cv, row, buf, -1);
				if (rowiter.hasNext())
					buf.append(",");
			}
		}		
		
	}
	
	public void emitInsertStatement(SchemaContext sc, ConnectionValues cv, InsertIntoValuesStatement s, StringBuilder buf) {
		emitInsertPrefix(sc,cv,s,buf);
		if (s.getValues() != null) 
			emitInsertValues(sc,cv,s.getValues(),buf);
		emitInsertSuffix(sc,cv,s,buf);
	}
	
	public void emitInsertIntoSelectStatement(SchemaContext sc, ConnectionValues cv, InsertIntoSelectStatement iiss, StringBuilder buf, int pretty) {
		emitInsertPrefix(sc,cv,iiss,buf);
		emitDMLStatement(sc,cv,iiss.getSource(), buf, bumpIndent(pretty));
		emitInsertSuffix(sc,cv,iiss,buf);
	}
	
	public void emitUnionStatement(SchemaContext sc, ConnectionValues cv, UnionStatement us, StringBuilder buf, int pretty) {
		boolean grouped = us.getFromEdge().get().isGrouped();
		if (grouped) buf.append(" (");
		emitDMLStatement(sc, cv, us.getFromEdge().get(), buf, pretty);
		if (grouped) buf.append(")");
		if (us.isUnionAll())
			emitIndent(buf,pretty+1, "UNION ALL ");
		else
			emitIndent(buf,pretty+1, "UNION");
		grouped = us.getToEdge().get().isGrouped();
		if (grouped) buf.append(" (");
		emitDMLStatement(sc, cv, us.getToEdge().get(), buf, pretty);
		if (grouped) buf.append(") ");
		if (us.getOrderBysEdge().has()) {
			emitIndent(buf,pretty,"ORDER BY ");
			emitSortingSpecifications(sc, cv, us.getOrderBys(), buf, pretty);
		}
		if (us.getLimit() != null) 
			emitLimitSpecification(sc, cv, us.getLimit(), buf, pretty);
	}
	
	public void emitFromTableReferences(SchemaContext sc, ConnectionValues cv, List<FromTableReference> tables, StringBuilder buf, int pretty) {
		for(Iterator<FromTableReference> iter = tables.iterator(); iter.hasNext();) {
			emitFromTableReference(sc, cv, iter.next(), buf, pretty);
			if (iter.hasNext())
				buf.append(", ");
		}
	}
	
	public void emitSortingSpecifications(final SchemaContext sc, final ConnectionValues cv, 
			Collection<SortingSpecification> specs, StringBuilder buf, final int prefix) {
		final Emitter me = this;
		Functional.join(specs, buf, ", ", new BinaryProcedure<SortingSpecification, StringBuilder>() {

			@Override
			public void execute(SortingSpecification aobj, StringBuilder bobj) {
				me.emitOrderBySpecification(sc, cv, aobj, bobj, prefix);
			}
			
		});
	}
		
	public void emitTableFactor(SchemaContext sc, ConnectionValues cv, ExpressionNode targ, StringBuilder buf, int pretty) {
		if (targ instanceof Subquery) {
			Subquery q = (Subquery) targ;
			buf.append(" ( ");
			emitDMLStatement(sc,cv,q.getStatement(), buf, bumpIndent(pretty));
			buf.append(" ) ").append(q.getAlias().getSQL());
		} else if (targ instanceof TableInstance) {
			emitTableInstance(sc, cv,(TableInstance)targ, buf, TableInstanceContext.TABLE_FACTOR);
		} else if (targ instanceof TableJoin) {
			emitTableJoin(sc,cv,(TableJoin)targ,buf, pretty);
		} else {
			throw new SchemaException(Pass.EMITTER, "Unknown table factor kind: " + targ.getClass().getSimpleName());
		}		
	}
	
	public void emitFromTableReference(SchemaContext sc, ConnectionValues cv, FromTableReference ftr, StringBuilder buf, int pretty) {
		emitTableFactor(sc, cv, ftr.getTarget(), buf, pretty);
	}

	public void emitTableJoin(SchemaContext sc, ConnectionValues cv, TableJoin targ, StringBuilder buf, int pretty) {
		if (targ.isGrouped()) buf.append("( ");
		emitTableFactor(sc,cv,targ.getFactor(), buf, pretty);
		for(JoinedTable jt : targ.getJoins()) {
			emitJoinedTable(sc,cv,jt,buf, pretty);
		}
		if (targ.isGrouped()) buf.append(" )");
	}
	
	public void emitJoinedTable(SchemaContext sc, ConnectionValues cv, JoinedTable jt, StringBuilder buf, int pretty) {
		emitIndent(buf,bumpIndent(pretty),jt.getJoinType().getSQL() + " JOIN ");
		if (jt.getJoinedToTable() != null)
			emitTableInstance(sc,cv,jt.getJoinedToTable(), buf, TableInstanceContext.TABLE_FACTOR);
		else if (jt.getJoinedToQuery() != null) {
			emitSubquery(sc,cv,jt.getJoinedToQuery(),buf, pretty);
		} else 
			throw new SchemaException(Pass.EMITTER, "What kind of table join is this?");
		if (jt.getJoinOnEdge().has()) {
			buf.append(" ON ");
			emitExpression(sc,cv,jt.getJoinOn(), buf, pretty);
		} else if (jt.getUsingColSpec() != null && !jt.getUsingColSpec().isEmpty()) {
			buf.append(" USING (");
			Functional.join(jt.getUsingColSpec(), buf, ", ", new BinaryProcedure<Name, StringBuilder>() {

				@Override
				public void execute(Name aobj, StringBuilder bobj) {
					bobj.append(aobj.getSQL());
				}
				
			});
			buf.append(") ");			
		}
	}
	
	public void emitExpressions(SchemaContext sc, ConnectionValues cv, Collection<? extends ExpressionNode> exprs, StringBuilder buf, int indent) {
		emitExpressions(sc,cv,exprs.iterator(), buf, indent);
	}	
	
	public void emitExpressions(SchemaContext sc, ConnectionValues cv, Iterator<? extends ExpressionNode> iter, StringBuilder buf, int indent) {
		while(iter.hasNext()) {
			ExpressionNode e = iter.next();
			emitExpression(sc,cv,e, buf, indent);
			if (iter.hasNext())
				buf.append(",");
		}
	}
	
	public void emitExpression(SchemaContext sc, ConnectionValues cv, ExpressionNode e, StringBuilder buf) {
		emitExpression(sc,cv,e,buf,-1);
	}
	
	public void emitExpression(SchemaContext sc, ConnectionValues cv, ExpressionNode e, StringBuilder buf, int indent) {
		if (isResultSetMetadata() && e.getSourceLocation() != null && getEmitContext() != null) {
			String maybe = getEmitContext().getOriginalText(e.getSourceLocation());
			if (maybe != null) {
				buf.append(maybe);
				return;
			}
		}
		if (e.isNegated())
			buf.append(" -");
		if (e.isGrouped())
			buf.append(" (");
		
		if (e instanceof ColumnInstance) {
			emitColumnInstance(sc,cv,(ColumnInstance)e, buf);
		} else if (e instanceof FunctionCall) {
			emitFunctionCall(sc, cv,(FunctionCall)e, buf, indent);
		} else if (e instanceof TableInstance) {
			emitTableInstance(sc,cv,(TableInstance)e, buf, TableInstanceContext.COLUMN);
		} else if (e instanceof IdentifierLiteralExpression) {
			emitIdentifierLiteral(sc,(IdentifierLiteralExpression)e, buf);
		} else if (e instanceof LiteralExpression) {
			emitLiteral(cv,(LiteralExpression)e, buf);
		} else if (e instanceof Wildcard) {
			emitWildcard((Wildcard)e, buf);
		} else if (e instanceof ExpressionAlias) {
			emitDerivedColumn(sc,cv,(ExpressionAlias)e, buf, indent);
		} else if (e instanceof VariableInstance) {
			emitVariable((VariableInstance)e, buf);
		} else if (e instanceof AliasInstance) {
			emitAliasInstance(sc,(AliasInstance)e, buf);
		} else if (e instanceof Default) {
			emitDefault((Default)e, buf);
		} else if (e instanceof IParameter) {
			emitParameter(sc,cv,(IParameter)e, buf);
		} else if (e instanceof ExpressionSet) {
			emitExpressionSet(sc, cv,(ExpressionSet) e, buf, indent);
		} else if (e instanceof Subquery) {
			emitSubquery(sc,cv,(Subquery)e, buf, indent);
		} else if (e instanceof CaseExpression) {
			emitCaseExpression(sc, cv,(CaseExpression)e, buf, indent);
		} else if (e instanceof NameInstance) {
			// shouldn't see these - (well except for some tests)
			buf.append(((NameInstance)e).getName().getSQL());
		} else if (e instanceof IntervalExpression) {
			emitIntervalExpression(sc, cv,(IntervalExpression)e, buf, indent);
		} else if (e instanceof LateResolvingVariableExpression) {
			emitLateResolvingVariableExpression(sc, (LateResolvingVariableExpression)e, buf);
		} else if (e instanceof LateBindingConstantExpression) {
			emitLateBindingConstantExpression(sc.getValues(), (LateBindingConstantExpression)e, buf);
		} else {
			error("Unsupported expression for emit: " + e.getClass().getName());
		}
		if (e.isGrouped())
			buf.append(") ");
	}
	
	public void emitColumnInstance(SchemaContext sc,ConnectionValues cv, ColumnInstance cr, StringBuilder buf) {
		// in order for the plan cache to work correctly, need to emit the table instance separately
		if (!this.hasOptions() || (!getOptions().isResultSetMetadata())) {
			if (cr.getColumn() == null)
				buf.append(cr.getSpecifiedAs().getSQL());
			else if (cr.getSpecifiedAs() == null || !cr.getSpecifiedAs().isQualified()) {
				emitTableInstance(sc,cv, cr.getTableInstance(),buf,TableInstanceContext.COLUMN);
				buf.append(".").append(cr.getColumn().getName().getUnqualified());
			} else if (cr.getTableInstance().isMT()) {
				// if the table is aliased, use that; otherwise use the mangled name
				if (cr.getTableInstance().getAlias() != null)
					buf.append(cr.getTableInstance().getAlias());
				else
					buf.append(cr.getTableInstance().getTable().getName().getSQL());
				buf.append(".");
				if (cr.getSpecifiedAs() == null)
					buf.append(cr.getColumn().getName().getSQL());
				else if (cr.getSpecifiedAs().isQualified())
					buf.append(cr.getSpecifiedAs().getUnqualified().getSQL());
				else
					buf.append(cr.getSpecifiedAs().getSQL());
			} else {
				if (this.hasOptions() && getOptions().isGenericSQL()) {
					emitTableInstance(sc,cv,cr.getTableInstance(),buf,TableInstanceContext.COLUMN);
					buf.append(".").append(cr.getColumn().getName().getUnqualified());
				} else {
					buf.append(cr.getSpecifiedAs());
				}
			}
		} else if (getOptions().isResultSetMetadata()) {
			Name specified = cr.getSpecifiedAs();
			if (specified != null)
				buf.append(specified.getSQL());
			else
				buf.append(cr.getColumn().getName().getSQL());
		} else {
			throw new SchemaException(Pass.REWRITER, "Unknown emit state for column");
		}
	}
	
	public void emitAliasInstance(SchemaContext sc, AliasInstance ai, StringBuilder buf) {
		buf.append(ai.getTarget().getAlias().getSQL());
	}
	
	/**
	 * @param def
	 * @param buf
	 */
	public void emitDefault(Default def, StringBuilder buf) {
		buf.append("default");
	}
	
	public void emitParameter(SchemaContext sc, ConnectionValues cv, IParameter p, StringBuilder buf) {
		EmitOptions opts = getOptions();
		if (opts != null && opts.isForceParamValues()) {
			buf.append(cv.getParameterValue(p.getPosition()));
		} else {
			boolean decorate = this.hasOptions() && getOptions().isGenericSQL();
			String tok = null;
			if (decorate) {
				tok = "_p" + p.getPosition();
			} else {
				tok = "?";
			}
			if (decorate)
				builder.withParameter(buf.length(), tok, p);
			buf.append(tok);
		}
	}
	
	public void emitExpressionSet(SchemaContext sc, ConnectionValues cv, ExpressionSet set, StringBuilder buf, int prefix) {
		buf.append("(");
		for (final ExpressionNode value : set.getSubExpressions()) {
			emitExpression(sc, cv, value, buf, prefix);
			buf.append(", ");
		}
		buf.delete(buf.length() - 2, buf.length() - 1).append(")");
	}

	public void emitLateResolvingVariableExpression(SchemaContext sc, LateResolvingVariableExpression lrve, StringBuilder buf) {
		boolean decorate = this.hasOptions() && getOptions().isGenericSQL();
		String tok = null;
		if (decorate) {
			tok = lrve.getAccessor().getSQL();
		} else {
			String v = null;
			try {
				v = sc.getConnection().getVariableValue(lrve.getAccessor());
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER, "Unable to obtain variable value",pe);
			}
			if (v == null)
				tok = "null";
			else
				tok = "'" + v + "'";
		}
		if (decorate)
			builder.withLateVariable(buf.length(), tok, lrve);
		buf.append(tok);
	}
	
	public void emitSubquery(SchemaContext sc, ConnectionValues cv, Subquery sq, StringBuilder buf, int indent) {
		DMLStatement ss = sq.getStatement();
		buf.append("(");
		emitDMLStatement(sc, cv, ss,buf, bumpIndent(indent));
		buf.append(")");
		if (sq.getAlias() != null)
			buf.append(" AS ").append(sq.getAlias().getSQL());
	}
	
	public void emitCaseExpression(SchemaContext sc, ConnectionValues cv, CaseExpression ce, StringBuilder buf, int prefix) {
		buf.append("CASE ");
		if (ce.getTestExpression() != null)
			emitExpression(sc, cv, ce.getTestExpression(), buf, prefix);
		for(WhenClause wc : ce.getWhenClauses()) {
			buf.append(" WHEN ");
			emitExpression(sc, cv, wc.getTestExpression(), buf, prefix);
			buf.append(" THEN ");
			emitExpression(sc, cv, wc.getResultExpression(), buf, prefix);
		}
		if (ce.getElseExpression() != null) {
			buf.append(" ELSE ");
			emitExpression(sc, cv, ce.getElseExpression(), buf, prefix);
		}
		buf.append(" END");
	}

	public void emitFunctionCall(SchemaContext sc, ConnectionValues cv, FunctionCall fc, StringBuilder buf, int prefix) {
		if (fc.isOperator())
			emitOperatorFunctionCall(sc, cv, fc, buf, prefix);
		else
			emitRegularFunctionCall(sc, cv,fc, buf, prefix);
	}
	
	public void emitRegularFunctionCall(SchemaContext sc, ConnectionValues cv, FunctionCall fc, StringBuilder buf, int prefix) {
		if (fc.getFunctionName().isCast()) {
			CastFunctionCall cfc = (CastFunctionCall) fc;
			buf.append(cfc.getFunctionName().get()).append("(");
			emitExpressions(sc, cv,fc.getParameters(), buf, prefix);
			buf.append(" ").append(cfc.getAsText()).append(" ");
			buf.append(cfc.getTypeName().getSQL());
			buf.append(")");
		} else if (fc.getFunctionName().isChar()) {
			buf.append("CHAR (");
			emitExpressions(sc, cv,fc.getParameters(), buf, prefix);
			CharFunctionCall cfc = (CharFunctionCall) fc;
			final Name outputEncoding = cfc.getOutputEncoding();
			if (outputEncoding != null) {
				buf.append(" USING ").append(outputEncoding.getSQL());
			}
			buf.append(")");
		} else if (fc.getFunctionName().isRand()) {
			final RandFunctionCall rfc = ((RandFunctionCall) fc);
			buf.append("RAND (");
			if (rfc.hasSeed()) {
				final ExpressionNode seed = rfc.getSeed();
				final int offset = buf.length();
				emitExpression(sc, cv, seed, buf);
				builder.withRandomSeed(offset, buf.substring(offset), seed);
			}
			buf.append(")");
		} else if (fc.getFunctionName().isConvert()) {
			buf.append("CONVERT( ");
			emitExpressions(sc,cv,fc.getParameters(), buf, prefix);
			ConvertFunctionCall cfc = (ConvertFunctionCall) fc;
			buf.append(cfc.isTranscoding() ? " USING " : ",");
			buf.append(cfc.getTypeName().getSQL()).append(" ) ");
		} else if (fc.getFunctionName().isGroupConcat()) {
			GroupConcatCall gcc = (GroupConcatCall) fc;
			buf.append(gcc.getFunctionName().get()).append("(");
			emitExpressions(sc,cv,fc.getParameters(), buf, prefix);
			if (gcc.getSeparator() != null)
				buf.append(" SEPARATOR ").append(gcc.getSeparator());
			buf.append(")");
		} else {
			buf.append(fc.getFunctionName().getSQL()).append("( ");
			if (fc.getSetQuantifier() != null)
				buf.append(fc.getSetQuantifier().getSQL()).append(" ");
			emitExpressions(sc, cv,fc.getParameters(), buf, prefix);
			buf.append(" ) ");
		}
	}
	
	public void emitOperatorFunctionCall(final SchemaContext sc, final ConnectionValues cv, FunctionCall fc, StringBuilder buf, final int prefix) {
		if (fc.getFunctionName().isIn() || fc.getFunctionName().isNotIn()) {
			emitInOperatorCall(sc, cv, fc, buf, prefix);
			return;
		} else if (fc.getFunctionName().isBetween() || fc.getFunctionName().isNotBetween()) {
			emitExpression(sc, cv, fc.getParametersEdge().get(0), buf, prefix);
			if (fc.getFunctionName().isNotBetween())
				buf.append(" NOT BETWEEN ");
			else
				buf.append(" BETWEEN ");
			emitExpression(sc, cv, fc.getParametersEdge().get(1), buf, prefix);
			buf.append(" AND ");
			emitExpression(sc, cv, fc.getParametersEdge().get(2), buf, prefix);
			return;
		}
		if (fc.getParameters().size() == 1) {
			buf.append(fc.getFunctionName().getSQL()).append(" ");
			emitExpression(sc, cv, fc.getParameters().get(0), buf, prefix);
		} else {
			String fn = fc.getFunctionName().getSQL();
			if (fc.getFunctionName().isNotLike())
				fn = "NOT LIKE";
			else if (fc.getFunctionName().isNotIs())
				fn = "IS NOT";
			String around = " ";
			if (fc.getFunctionName().isEquals() && this.hasOptions() && getOptions().isExternalTableDeclaration())
				around = "";
			Functional.join(fc.getParameters(), buf,around + fn + around,
					new BinaryProcedure<ExpressionNode, StringBuilder>() {

						@Override
						public void execute(ExpressionNode aobj,
								StringBuilder bobj) {
							emitExpression(sc, cv, aobj,bobj, prefix);
						}
				
			});
		}
	}
	
	public void emitInOperatorCall(SchemaContext sc, ConnectionValues cv, FunctionCall fc, StringBuilder buf, int indent) {
		emitExpression(sc, cv, fc.getParameters().get(0), buf, indent);
		String fn = (fc.getFunctionName().isIn() ? fc.getFunctionName().getSQL() : "NOT IN");
		buf.append(" ").append(fn).append(" ");
		buf.append("( ");
		Iterator<ExpressionNode> iter = fc.getParameters().iterator();
		iter.next();
		emitExpressions(sc, cv, iter, buf, indent);
		buf.append(" )");
	}
	
	@SuppressWarnings("null")
	public void emitLiteral(ConnectionValues cv, ILiteralExpression le, StringBuilder buf) {
		// null literals are invisible to late resolution
		if (le.isNullLiteral()) {
			buf.append("NULL");
			return;
		}
		
		if (this.hasOptions() && getOptions().isAnalyzerLiteralsAsParameters()) {
			buf.append("?");
			return;
		}
		
		DelegatingLiteralExpression dle = null;
		boolean autoinc = false;
		if (le instanceof DelegatingLiteralExpression) {
			dle = (DelegatingLiteralExpression) le;
			if (hasOptions() && getOptions().isTriggerBody() && dle.getConstantType() == ConstantType.AUTOINCREMENT_LITERAL)
				autoinc = true;
		}
		boolean deltoken = (dle != null) && this.hasOptions() && getOptions().isGenericSQL();
		
		
		int offset = -1;
		if (dle != null)
			offset = buf.length();
		String tok = null;
		if (deltoken) {
			tok = "_e" + dle.getPosition();
		} else {
			Object v = le.getValue(cv);
			if (le.getCharsetHint() != null)
				buf.append(le.getCharsetHint().getUnquotedName().get());
			if (v instanceof String) {
				tok = (String) v;
			} else if (v instanceof Date) {
					tok = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIMESTAMP_FORMAT).format((Date) v);
			} else {
				tok = String.valueOf(v);
			}
			if (v != null && le.isStringLiteral()) {
				tok = "'" + tok + "'";
			} 
		}
		
		buf.append(tok);
		if (dle != null) {
			if (autoinc)
				builder.withLateAutoinc(offset, tok, (IAutoIncrementLiteralExpression) dle);
			else
				builder.withLiteral(offset, tok, dle);
		}

	}
		
	public void emitIdentifierLiteral(SchemaContext sc, IdentifierLiteralExpression ile, StringBuilder buf) {
		buf.append(ile.getValue(sc.getValues()));
	}
	
	public void emitLateBindingConstantExpression(ConnectionValues cv, LateBindingConstantExpression expr, StringBuilder buf) {
		boolean gsql = this.hasOptions() && getOptions().isGenericSQL();
		
		int offset = -1;
		String tok;
		if (gsql)
			offset = buf.length();
		tok = "_lbc" + expr.getPosition();
		buf.append(tok);
		if (gsql)
			builder.withLateConstant(offset, tok, expr);
	}

	public String emitConstantExprValue(IConstantExpression expr, Object value) {
		boolean stringLit = false;
		String any = null;
		if (expr instanceof ILiteralExpression) {
			ILiteralExpression ile = (ILiteralExpression) expr;
			if (ile.getCharsetHint() != null)
				any = ile.getCharsetHint().getUnquotedName().get();
			stringLit = ile.isStringLiteral();
		} else if (expr instanceof LateBindingConstantExpression) {
			LateBindingConstantExpression lbce = (LateBindingConstantExpression) expr;
			if (lbce.getType().isStringType())
				stringLit = true;
		}
		String tok = null;
		if (value instanceof String) {
			tok = (String) value;
		} else if (value instanceof Date) {
			tok = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIMESTAMP_FORMAT).format((Date) value);
		} else {
			tok = String.valueOf(value);
		}
		if (value != null && stringLit) {
			tok = "'" + tok + "'";
		}
		if (any != null) return any + tok;
		return tok;
	}
	
	public void emitVariable(VariableInstance vi, StringBuilder buf) {
		VariableScope vs = vi.getScope();
		if (vs.getKind() == VariableScopeKind.USER) {
			buf.append("@");
		} else {
			String raw = vi.getVariableName().get().toUpperCase();
			if ("NAMES".equals(raw)) {
				buf.append(raw);
				return;
			}
			// if the rhs form, we're going to do @@<decorator><name>
			// if the lhs form, then we can do global <name>
			if (vi.getRHSForm()) {
				buf.append("@@").append(vi.getScope().getKind().name()).append(".");
			} else {
				buf.append(vi.getScope().getKind().name()).append(" ");
			}
		}
		buf.append(vi.getVariableName().getSQL());		
	}
	
	public void emitSessionSetVariableStatement(final SchemaContext sc,
			final ConnectionValues cv,
			SessionSetVariableStatement ssvs, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"SET ");
		Functional.join(ssvs.getSetExpressions(), buf, ", ", new BinaryProcedure<SetExpression, StringBuilder>() {

			@Override
			public void execute(SetExpression aobj, StringBuilder bobj) {
				emitSetExpression(sc, cv,aobj, bobj, -1);
			}
			
		});
	}

	public void emitSetExpression(SchemaContext sc, ConnectionValues cv, SetExpression se, StringBuilder buf, int pretty) {
		if (se.getKind() == SetExpression.Kind.TRANSACTION_ISOLATION) {
			emitSetTransactionIsolation((SetTransactionIsolationExpression)se, buf);
		} else if (se.getKind() == SetExpression.Kind.VARIABLE) {
			SetVariableExpression sve = (SetVariableExpression) se;
			emitVariable(sve.getVariable(),buf);
			buf.append(" ");
			if (sve.getVariable().getVariableName().get().equalsIgnoreCase("names")) {
				emitExpression(sc,cv, sve.getValue().get(0),buf);
				if (sve.getValue().size() > 1) {
					buf.append(" COLLATE ");
					emitExpression(sc,cv,sve.getValue().get(1),buf);
				}
			} else {
				emitExpressions(sc,cv,sve.getValue(), buf, pretty);
			}
		}
	}

	public void emitSetTransactionIsolation(SetTransactionIsolationExpression stie, StringBuilder buf) {
		if (stie.getScope() != null)
			buf.append(stie.getScope().getKind().name()).append(" ");
		buf.append("TRANSACTION ISOLATION ").append(stie.getLevel().getSQL());
	}

	
	// for ddl
	public void emitTable(SchemaContext sc, ConnectionValues cv, PEAbstractTable<?> pet, Database<?> defaultDB, StringBuilder buf) {
		Database<?> tblDb = pet.getDatabase(sc);
		if ((getOptions() != null && getOptions().isQualifiedTables())
				|| ((defaultDB == null && tblDb != null) || 
				((defaultDB != null && tblDb != null) && (defaultDB.getId() != tblDb.getId())))) {
			buf.append("`");
			int offset = buf.length();
			String toAdd = pet.getDatabase(sc).getName().getUnqualified().getUnquotedName().getSQL();
			buf.append(toAdd).append("`.");
			builder.withDBName(offset, toAdd);
		}
		buf.append(pet.getName(sc,cv).getSQL());
	}
	
	public void emitTable(SchemaContext sc, Name n, StringBuilder buf) {
		// would it matter if we always put in the dbname in the builder?  all that would happen is we would reswap the actual name
		if (n.isQualified()) {
			QualifiedName qn = (QualifiedName) n;
			UnqualifiedName dbname = qn.getNamespace();
			int offset = buf.length();
			String toAdd = dbname.getSQL();
			buf.append(toAdd).append(".");
			builder.withDBName(offset,toAdd);
		}
		buf.append(n.getUnqualified().getSQL());
	}
	
	// contexts under which table instances are emitted
	public enum TableInstanceContext {
		
		COLUMN,  // part of a column
		TABLE_FACTOR, // i.e. join on Table
		NAKED  // i.e. delete A from ....
		
	}
	
	public void emitTableInstance(SchemaContext sc, ConnectionValues cv, TableInstance tr, StringBuilder buf, TableInstanceContext context) {
		Table<?> tab = tr.getTable();
		if (tab == null) {
			buf.append(tr.getSpecifiedAs(sc).getSQL());			
			if (context == TableInstanceContext.COLUMN || context == TableInstanceContext.NAKED) {
				// no as foo clause
			} else if (tr.getAlias() != null) {
				buf.append(" AS ").append(tr.getAlias().getSQL());
			}
		} else if (tab instanceof PEAbstractTable) {
			// for temp tables: we never include the alias
			// if the current db is different than the owning db, use a qualified name if not as a colum
			// for table factors, add the alias.
			PEAbstractTable<?> pet = (PEAbstractTable<?>) tab;
			Database<?> curDb = sc.getCurrentDatabase(false);
			Database<?> tblDb = pet.getDatabase(sc);
			if (context == TableInstanceContext.TABLE_FACTOR) {
				if (((curDb == null) && (tblDb != null)) ||
						(((curDb != null) && (tblDb != null)) && (curDb.getId() != tblDb.getId()))) {
					if (tblDb.hasNameManglingEnabled()) {
						int offset = buf.length();
						String toAdd = pet.getDatabase(sc).getName().getUnqualified().getSQL();
						buf.append(toAdd).append(".");
						builder.withDBName(offset, toAdd);
					} else {
						// catalog - just emit the database name
						buf.append(pet.getDatabase(sc).getName().getUnqualified().getSQL()).append(".");
					}
				}
			}
			boolean prohibitAlias = false;
			if (pet.isTempTable()) {
				int offset= buf.length();
				String toAdd = pet.getName(sc,cv).getSQL();
				buf.append(toAdd);
				builder.withTempTable(offset, toAdd, (TempTable)pet);
				// we never emit aliases with temp tables
				prohibitAlias = true;
			} else {
				if ((context == TableInstanceContext.COLUMN || context == TableInstanceContext.NAKED) && tr.getAlias() != null) {
					buf.append(tr.getAlias().getSQL());
				} else {
					// table name
					if (pet.getPEDatabase(sc).getMTMode() == MultitenantMode.ADAPTIVE) {
						buf.append(tr.getTable().getName(sc,cv).getSQL());
					} else {
						buf.append(tr.getSpecifiedAs(sc).getQuotedName().getSQL());
					}
				}
			}
			if (context == TableInstanceContext.COLUMN || prohibitAlias || context == TableInstanceContext.NAKED) {
				// no as foo clause
			} else if (tr.getAlias() != null) {
				buf.append(" AS ").append(tr.getAlias().getSQL());
			}
		} else if (tab instanceof InformationSchemaTable) {
			// we'd get this from a debug statement
			if (context == TableInstanceContext.COLUMN && tr.getAlias() != null) {
				buf.append(tr.getAlias().getSQL());
			} else {
				buf.append(tr.getTable().getName().getSQL());
			}
			if (context == TableInstanceContext.TABLE_FACTOR && tr.getAlias() != null)
				buf.append(" AS ").append(tr.getAlias().getSQL());
		} else {
			// info schema
			if (context == TableInstanceContext.COLUMN && tr.getAlias() != null) {
				buf.append(tr.getAlias().getSQL());
			} else {
				buf.append(tr.getTable().getName().getSQL());
			}
			if (context == TableInstanceContext.TABLE_FACTOR && tr.getAlias() != null)
				buf.append(" AS ").append(tr.getAlias().getSQL());
		}
	}
	
	public void emitLimitSpecification(SchemaContext sc, ConnectionValues cv, LimitSpecification ls, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"LIMIT ");
		if (ls.getOffset() != null) {
			emitExpression(sc, cv, ls.getOffset(), buf, indent);
			buf.append(", ");
		}
		emitExpression(sc, cv, ls.getRowcount(), buf, indent);
		builder.withLimit();
	}
	
	public void emitOrderBySpecification(SchemaContext sc, ConnectionValues cv, SortingSpecification obs, StringBuilder buf, int indent) {
		emitExpression(sc, cv, obs.getTarget(), buf, indent);
		buf.append(" ").append(obs.isAscending() ? "ASC" : "DESC");
	}
	
	public void emitWildcard(Wildcard wd, StringBuilder buf) {
		if (wd instanceof WildcardTable) {
			emitWildcardTable((WildcardTable)wd, buf);
			return;
		}
		buf.append("*");
	}
	
	public void emitWildcardTable(WildcardTable wct, StringBuilder buf) {
		buf.append(wct.getTableName().getSQL()).append(".*");
	}
	
	public void emitDerivedColumn(SchemaContext sc, ConnectionValues cv, ExpressionAlias dc, StringBuilder buf, int indent) {
		emitExpression(sc, cv, dc.getTarget(), buf, indent);
		buf.append(" AS ");
		buf.append(dc.getAlias().getSQL());
	}

	public void emitGrantStatement(GrantStatement pecs, StringBuilder buf) {
		buf.append("GRANT ").append(pecs.getPrivileges()).append(" ON ")
			.append(pecs.getGrantScope().getSQL())
			.append(" TO ");
		emitUserDeclaration(pecs.getUser(),buf);
	}
	
	@SuppressWarnings("rawtypes")
	public void emitCreateStatement(SchemaContext sc, ConnectionValues cv, PECreateStatement pecs, StringBuilder buf) {
		if (!emitExtensions() && pecs.isDVEOnly())
			return;
		buf.append("CREATE ");
		emitDeclaration(sc, cv, pecs.getCreated(), pecs, buf);
	}
			
	public void emitDeclaration(SchemaContext sc, DistributionVector dv, StringBuilder buf) {
		if (dv.isContainer()) {
			if (dv.getTable().isContainerBaseTable(sc)) {
				buf.append(" DISCRIMINATE ON (");
				Functional.join(dv.getContainer(sc).getDiscriminantColumns(sc), buf, ", ", new BinaryProcedure<PEColumn,StringBuilder>() {

					@Override
					public void execute(PEColumn aobj, StringBuilder bobj) {
						bobj.append(aobj.getName().getQuotedName().getSQL());
					}
					
				});
				// add the original columns here
				buf.append(") USING CONTAINER ").append(dv.getContainer(sc).getName().getQuotedName().getSQL());
			} else {
				buf.append(" CONTAINER DISTRIBUTE ").append(dv.getContainer(sc).getName().getQuotedName().getSQL());
			}
		} else {
			buf.append(" ").append(dv.getModel().getSQL()).append(" DISTRIBUTE");
			if (!dv.getColumns(sc).isEmpty()) {
				buf.append(" ON (");
				Functional.join(dv.getColumns(sc), buf, ", ", new BinaryProcedure<PEColumn, StringBuilder>() {

					@Override
					public void execute(PEColumn aobj, StringBuilder bobj) {
						bobj.append(aobj.getName().getQuotedName().getSQL());
					}

				});
				buf.append(") ");
			}
			if (dv.getRangeDistribution() != null) {
				buf.append("USING ").append(dv.getRangeDistribution().getDistribution(sc).getName().getQuotedName().getSQL());
			} 
		}
		
	}
	
	public void emitUseDatabaseStatement(SchemaContext sc, UseDatabaseStatement uds, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"USE ");
		buf.append(uds.getDatabase(sc).getName().getSQL());
	}
	
	public void emitUseTenantStatement(UseTenantStatement pet, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"USE ");
		if (pet.getTenant() == null)
			buf.append(PEConstants.LANDLORD_TENANT);
		else
			buf.append(pet.getTenant().getExternalID());
	}
	
	public void emitUseContainerStatement(final SchemaContext sc, UseContainerStatement ucs, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"USE CONTAINER ");
		if (ucs.isGlobal()) {
			buf.append("GLOBAL");
		} else if (ucs.getContainer() != null) {
			buf.append(ucs.getContainer().getName().getSQL()).append(" (");
			Functional.join(ucs.getDiscriminant(), buf, ", ", new BinaryProcedure<Pair<PEColumn,LiteralExpression>, StringBuilder>() {

				@Override
				public void execute(Pair<PEColumn, LiteralExpression> aobj,
						StringBuilder bobj) {
					bobj.append(aobj.getFirst().getName().getSQL());
					bobj.append("=");
					emitLiteral(sc.getValues(),aobj.getSecond(),bobj);
				}
				
			});
			buf.append(" )");
		} else {
			buf.append("null");
		}
			
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void emitDropStatement(SchemaContext sc, ConnectionValues cv, PEDropStatement dts, StringBuilder buf) {
		if (PEDatabase.class.equals(dts.getTargetClass())) {
			emitDropDatabaseStatement(dts,buf);
		} else if (PETable.class.equals(dts.getTargetClass())) {
			emitDropTableStatement(sc, cv,(PEDropTableStatement) dts,buf);
		} else if (PEViewTable.class.equals(dts.getTargetClass())) {
			emitDropViewStatement(dts,buf);
		} else if (PETrigger.class.equals(dts.getTargetClass())) {
			emitDropTriggerStatement(dts, buf);
		} else if (PEUser.class.equals(dts.getTargetClass())) {
			emitDropUserStatement(dts,buf);
		} else if (PETenant.class.equals(dts.getTargetClass())) {
			emitDropTenantStatement(dts, buf);
		} else if (PEPolicy.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (RangeDistribution.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PEPersistentGroup.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PEStorageSite.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PESiteInstance.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PEExternalService.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PEContainer.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PETemplate.class.equals(dts.getTargetClass())) {
			// no ddl
		} else if (PERawPlan.class.equals(dts.getTargetClass())) {
			// no ddl
		} else {
			error("Unknown drop target type: " + dts.getTarget().getClass().getName());
		}
	}
	
	public void emitDropTableStatement(final SchemaContext sc, ConnectionValues cv, PEDropTableStatement peds, StringBuilder buf) {
		buf.append("DROP ");
		if (peds.isTemporary())
			buf.append("TEMPORARY ");
		buf.append("TABLE ");
		if (peds.isIfExists())
			buf.append("IF EXISTS ");
		boolean first = true;
		for(TableKey tk : peds.getDroppedTableKeys()) {
			if (first) first = false;
			else buf.append(",");
			Name tabName = null;
			if (tk.isUserlandTemporaryTable()) {
				tabName = new QualifiedName(tk.getAbstractTable().getDatabaseName(sc).getUnqualified(),tk.getAbstractTable().getName().getUnqualified());
			} else {
				tabName = tk.getAbstractTable().getName(sc,cv);
			}
			emitTable(sc,tabName,buf);
		}		
	}
	
	public void emitDropViewStatement(PEDropStatement<?,?> peds, StringBuilder buf) {
		emitDropStatement(peds, buf, "VIEW");
	}
	
	public void emitDropTriggerStatement(PEDropStatement<?, ?> peds, StringBuilder buf) {
		emitDropStatement(peds, buf, "TRIGGER");
	}

	public void emitDropDatabaseStatement(PEDropStatement<?,?> peds, StringBuilder buf) {
		emitDropStatement(peds, buf, "DATABASE");
	}

	public void emitDropTenantStatement(PEDropStatement<?,?> peds, StringBuilder buf) {
		if (emitExtensions()) {
			PETenant pet = (PETenant) peds.getTarget();
			buf.append("DROP TENANT ").append(pet.getExternalID());
		}
	}
	
	public abstract void emitDropUserStatement(PEDropStatement<PEUser, User> peds, StringBuilder buf);
		
	private void emitDropStatement(PEDropStatement<?,?> ds, StringBuilder buf, String what) {
		buf.append("DROP ").append(what).append(" ");
		if (ds.isIfExists())
			buf.append("IF EXISTS ");
		if (ds.getTarget() == null)
			buf.append(ds.getTargetName().getSQL());
		else
			buf.append(ds.getTarget().getName().getSQL());
	}
	
	public void emitAlterStatement(SchemaContext sc, ConnectionValues cv, AlterStatement as, StringBuilder buf) {
		if (as instanceof AddStorageSiteStatement) {
			emitAddStoragesiteStatement((AddStorageSiteStatement)as, buf);
		} else if (as instanceof PEAlterPersistentSite) {
			emitAlterPersistentSiteStatement((PEAlterPersistentSite)as, buf);
		} else if (as instanceof PEAlterSiteInstanceStatement) {
			emitAlterSiteInstanceStatement((PEAlterSiteInstanceStatement)as, buf);
		} else if (as instanceof SetPasswordStatement) {
			emitSetPasswordStatement((SetPasswordStatement)as, buf);
		} else if (as instanceof PEAlterTenantStatement) {
			emitAlterTenantStatement((PEAlterTenantStatement)as, buf);
		} else if (as instanceof PEAlterTableStatement) {
			emitAlterTableStatement(sc, cv,(PEAlterTableStatement)as, buf);
		} else if (as instanceof PEAlterPolicyStatement) {
			// nothing to do yet
		} else if (as instanceof PEAlterExternalServiceStatement) {
			// emit nothing so that it doesn't get sent to workers
		} else if (as instanceof PEAlterTemplateStatement) {
			emitAlterTemplateStatement((PEAlterTemplateStatement)as,buf);
		} else if (as instanceof AlterTableDistributionStatement) {
			emitAlterTableDistributionStatement(sc, (AlterTableDistributionStatement)as,buf);
		} else if (as instanceof PEAlterRawPlanStatement) {
			// don't care right now
		} else if (as instanceof AlterDatabaseStatement) {
			emitAlterDatabaseStatement(sc, (AlterDatabaseStatement) as, buf);
		} else if (as instanceof AlterDatabaseTemplateStatement) {
			emitAlterDatabaseTemplateStatement(sc, (AlterDatabaseTemplateStatement) as, buf);
		} else if (as instanceof AddGlobalVariableStatement) {
			// don't care right now
		} else {
			error("Unknown alter statement kind: " + as.getClass().getName());
		}
	}
	
	public void emitRenameStatement(SchemaContext sc, RenameTableStatement rs, StringBuilder buf) {
		buf.append("RENAME TABLE ");
		final List<Pair<Name, Name>> sourceTargetNamePairs = rs.getNamePairs();
		for (final Pair<Name, Name> namePair : sourceTargetNamePairs) {
			emitTable(sc,namePair.getFirst(),buf);
			buf.append(" TO ");
			emitTable(sc,namePair.getSecond(),buf);
			buf.append(",");
		}
		buf.deleteCharAt(buf.length() - 1);
	}

	public void emitPEGroupProviderDDLStatement(SchemaContext sc, PEGroupProviderDDLStatement in, StringBuilder buf) {
		if (!emitExtensions())
			return;
		// these are set up to have a fairly regular syntax:
		// action DYNAMIC SITE PROVIDER <name> <operation> <options>
		String action = in.getAction().toString();
		String operation = null;
		ArrayList<String> opts = new ArrayList<String>();
		if (in.getAction() == Action.CREATE) {
			operation = "USING";
			opts.add("PLUGIN='" + in.getProvider().getPlugin() + "'");
			opts.add("ACTIVE=" + (in.getProvider().isActive() ? "TRUE" : "FALSE"));
		}
		buf.append(action).append(" DYNAMIC SITE PROVIDER ").append(in.getProvider().getName().getSQL())
			.append(" ").append(operation).append(" ");
		// and now emit the options
		for(Pair<Name,LiteralExpression> p : in.getOptions()) {
			opts.add(p.getFirst().getSQL() + "=" + p.getSecond().getValue(sc.getValues()));
		}
		buf.append(Functional.join(opts, ", "));
	}
	
	public void emitDirectInfoSchemaStatement(SchemaContext sc, ConnectionValues cv, DirectInfoSchemaStatement diss, StringBuilder buf) {
		emitSelectStatement(sc,cv, diss.getCatalogQuery(), buf, 1);
	}
	
	public void emitAlterTableDistributionStatement(SchemaContext sc, AlterTableDistributionStatement atds, StringBuilder buf) {
		if (emitExtensions()) {
			buf.append("ALTER TABLE ").append(atds.getTarget().getName()).append(" ");
			emitDeclaration(sc, atds.getNewVector(), buf);
		}
	}
	
	public void emitAlterDatabaseStatement(SchemaContext sc, AlterDatabaseStatement ads, StringBuilder buf) {
		final Name targetName = ads.getTarget().getName();
		final String charSet = ads.getCharSet();
		final String collation = ads.getCollation();

		buf.append("ALTER DATABASE ").append(targetName.getSQL());
		if (!StringUtils.isBlank(charSet)) {
			buf.append(" CHARACTER SET ");
			buf.append(charSet);
		}
		if (!StringUtils.isBlank(collation)) {
			buf.append(" COLLATE ");
			buf.append(collation);
		}
	}

	public void emitAlterDatabaseTemplateStatement(SchemaContext sc, AlterDatabaseTemplateStatement adts, StringBuilder buf) {
		if (emitExtensions()) {
			final PEDatabase db = adts.getTarget();
			buf.append("ALTER DATABASE ").append(db.getName()).append(" ");
			emitUsingTemplateClause(sc, db, buf);
		}
	}

	public void emitTransactionStatement(TransactionStatement ts, StringBuilder buf, int indent) {
		if (ts.getXAXid() != null) {
			XATransactionStatement xast = (XATransactionStatement) ts;
			String prefix = null, postfix = null;
			switch(ts.getKind()) {
			case START:
				prefix = "START";
				break;
			case END:
				prefix = "END";
				break;
			case PREPARE:
				prefix = "PREPARE";
				break;
			case COMMIT:
				prefix = "COMMIT";
				XACommitTransactionStatement comst = (XACommitTransactionStatement) xast;
				if (comst.isOnePhase())
					postfix = "ONE PHASE";
				break;
			case ROLLBACK:
				prefix = "ROLLBACK";
				break;
			default:
				error("unknown xa transaction kind: " + ts.getKind());
			}
			emitIndent(buf,indent,"XA ");
			buf.append(prefix).append(" ").append(ts.getXAXid().getSQL());
			if (postfix != null)
				buf.append(" ").append(postfix);				
		} else {
			if (ts.getKind() == TransactionStatement.Kind.START) {
				emitIndent(buf,indent,"START TRANSACTION");
				if (ts.isConsistent())
					buf.append(" WITH CONSISTENT SNAPSHOT");
			}
			else if (ts.getKind() == TransactionStatement.Kind.COMMIT)
				buf.append("COMMIT");
			else if (ts.getKind() == TransactionStatement.Kind.ROLLBACK) {
				emitRollbackStatement((RollbackTransactionStatement)ts, buf,indent);
			} else
				error("Unknown transaction statement kind: " + ts.getKind());
		}
	}
	
	public void emitRollbackStatement(RollbackTransactionStatement rts, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"ROLLBACK");
		if (rts.getSavepointName() != null)
			buf.append(" TO ").append(rts.getSavepointName().getSQL());
	}
	
	public void emitSavepointStatement(SavepointStatement ss, StringBuilder buf, int indent) {
		emitIndent(buf,indent,(ss.isRelease() ? "RELEASE SAVEPOINT " : "SAVEPOINT "));
		buf.append(ss.getSavepointName().getSQL());
	}
	
	public void emitLockStatement(SchemaContext sc, ConnectionValues cv, LockStatement ls, StringBuilder buf, int indent) {
		if (ls.isUnlock()) {
			emitIndent(buf,indent,"UNLOCK TABLES");
			buf.append("UNLOCK TABLES");
		} else {
			emitIndent(buf,indent,"LOCK TABLES ");
			ArrayList<String> locks = new ArrayList<String>();
			for(Pair<TableInstance, LockType> p : ls.getLocks()) {
				StringBuilder temp = new StringBuilder();
				emitExpression(sc,cv,p.getFirst(), temp, -1);
				temp.append(" ").append(p.getSecond().getSQL());
				locks.add(temp.toString());
			}
			buf.append(Functional.join(locks, ","));
		}
	}
	
	public void emitShowPassthroughStatement(ShowPassthroughStatement sps, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"");
		buf.append(sps.emitShowSql());
	}
	
	public void emitAdhocSessionStatement(SessionStatement ss, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"");
		buf.append(ss.getAdhocSQL());
	}
	
	public void emitAddStoragesiteStatement(AddStorageSiteStatement as, StringBuilder buf) {
		if (emitExtensions()) {
			buf.append("ALTER PERSISTENT GROUP ").append(as.getTarget().getName().getSQL()).append(" ADD GENERATION ");
			buf.append(Functional.join(as.getStorageSites(), ", ", new UnaryFunction<String, PEStorageSite>() {

				@Override
				public String evaluate(PEStorageSite object) {
					return object.getName().get();
				}
			}));
		}
	}

	private void emitAlterPersistentSiteStatement(PEAlterPersistentSite as,
			StringBuilder buf) {
		if (emitExtensions()) {
			buf.append("ALTER PERSISTENT SITE ");
			buf.append(as.getTarget().getDeclarationSQL());
		}
	}
	
	private void emitAlterSiteInstanceStatement(PEAlterSiteInstanceStatement as,
			StringBuilder buf) {
		if (emitExtensions()) {
			buf.append("ALTER PERSISTENT INSTANCE ");
			buf.append((as.getTarget().getUrl()!=null) ? "," + PESiteInstance.OPTION_URL.toUpperCase() + "='" + as.getTarget().getUrl() + "'": "");
			buf.append((as.getTarget().getMaster()!=null) ? "," + PESiteInstance.OPTION_MASTER.toUpperCase() + "='" + Boolean.toString(as.getTarget().getMaster()) + "'": "");
			buf.append((as.getTarget().getStatus()!=null) ? "," + PESiteInstance.OPTION_STATUS.toUpperCase() + "='" + as.getTarget().getStatus() + "'" : "");
		}
	}

	private void emitAlterTemplateStatement(PEAlterTemplateStatement in, StringBuilder buf) {
		if (emitExtensions()) {
			buf.append("ALTER TEMPLATE ").append(in.getTarget().getName().getSQL());
			if (in.getNewDefinition() != null)
				buf.append(" XML='").append(in.getNewDefinition()).append("'");
			if (in.getNewMatch() != null)
				buf.append(" MATCH='").append(in.getNewMatch()).append("'");
			if (in.getNewComment() != null)
				buf.append(" COMMENT='").append(in.getNewComment()).append("'");
		}
	}
	
	public void emitTenantDeclaration(PETenant p, StringBuilder buf) {
		buf.append("TENANT ").append(p.getExternalID()).append(" '").append(p.getDescription()).append("'");
	}

	public void emitAlterTenantStatement(PEAlterTenantStatement peats, StringBuilder buf) {
		if (emitExtensions()) {
			buf.append(peats.isSuspend() ? "SUSPEND" : "RESUME").append(" TENANT ").append(peats.getTarget().getExternalID());
		}
	}
	
	public void emitAlterTableStatement(SchemaContext sc, ConnectionValues cv, PEAlterTableStatement peats, StringBuilder buf) {
		if (peats.hasSQL(sc)) {
			buf.append("ALTER TABLE ");
			emitTable(sc,cv,peats.getTarget(),sc.getCurrentDatabase(false),buf);
			buf.append(" ");
			boolean first = true;
			for(Iterator<AlterTableAction> iter = peats.getActions().iterator(); iter.hasNext();) {
				AlterTableAction ata = iter.next();
				if (ata.hasSQL(sc, peats.getTarget())) {
					if (!first) {
						buf.append(", ");
					}
					emitAlterAction(sc, cv, ata, buf);
					first = false;
				}
			}
		}
	}

	public void emitAlterAction(SchemaContext sc, ConnectionValues cv, AlterTableAction aa, StringBuilder buf) {
		if (aa instanceof AddColumnAction) {
			emitAddColumnAction(sc,cv,(AddColumnAction)aa,buf);
		} else if (aa instanceof AddIndexAction) {
			emitAddIndexAction(sc,cv,(AddIndexAction)aa,buf);
		} else if (aa instanceof AlterColumnAction) {
			emitAlterColumnAction(sc,cv,(AlterColumnAction)aa, buf);
		} else if (aa instanceof ChangeColumnAction) {
			emitChangeColumnAction(sc,cv,(ChangeColumnAction)aa, buf);
		} else if (aa instanceof DropColumnAction) {
			emitDropColumnAction((DropColumnAction)aa, buf);
		} else if (aa instanceof DropIndexAction) {
			emitDropIndexAction((DropIndexAction)aa, buf);
		} else if (aa instanceof RenameTableAction) {
			emitRenameTableAction((RenameTableAction)aa, buf);
		} else if (aa instanceof ConvertToAction) {
			emitConvertToAction((ConvertToAction) aa, buf);
		} else if (aa instanceof ChangeKeysStatusAction) {
			emitChangeKeysStatusAction((ChangeKeysStatusAction)aa,buf);
		} else if (aa instanceof ChangeTableModifierAction) {
			emitChangeTableModifierAction(sc,(ChangeTableModifierAction)aa,buf);
		} else {
			error("Unknown alter table action: " + aa.getClass().getName());
		}
	}
	
	public void emitRenameTableAction(RenameTableAction rta, StringBuilder buf) {
		buf.append("RENAME TO ").append(rta.getNewName().getSQL());
	}
	
	public void emitConvertToAction(ConvertToAction cta, StringBuilder buf) {
		buf.append("CONVERT TO CHARACTER SET ").append(cta.getCharSetName().getSQL());
		buf.append(" COLLATE ").append(cta.getCollationName().getSQL());
	}

	public void emitChangeKeysStatusAction(ChangeKeysStatusAction act, StringBuilder buf) {
		buf.append(act.isEnable() ? "ENABLE" : "DISABLE").append(" KEYS");
	}
		
	public void emitChangeTableModifierAction(SchemaContext sc, ChangeTableModifierAction act, StringBuilder buf) {
		TableModifiers mods = new TableModifiers();
		mods.setModifier(act.getModifier());
		emitTableModifiers(sc, null, mods, buf);
	}
	
	public void emitAddIndexAction(SchemaContext sc, ConnectionValues cv, AddIndexAction aia, StringBuilder buf) {
		buf.append("ADD ");
		emitDeclaration(sc, cv, aia.getNewIndex(), buf);
	}
	
	public void emitDropIndexAction(DropIndexAction dia, StringBuilder buf) {
		buf.append("DROP ");
		ConstraintType ct = dia.getConstraintType();
		if (ct != null && ct != ConstraintType.UNIQUE)
			buf.append(ct.getSQL());
		buf.append(" KEY ");
		if (ConstraintType.PRIMARY != ct)
			buf.append(dia.getIndexName());
	}

	public void emitAddColumnAction(SchemaContext sc, ConnectionValues cv, AddColumnAction aca, StringBuilder buf) {
		buf.append("ADD ");
		if (aca.getNewColumns().size() > 1)
			buf.append("(");
		emitColumnDeclarations(sc,cv,aca.getNewColumns(),"", false,buf);
		if (aca.getNewColumns().size() > 1)
			buf.append(")");
		if (aca.getFirstOrAfterSpec() != null) {
			buf.append(" ").append(aca.getFirstOrAfterSpec().getFirst()).append(" ");
			if (aca.getFirstOrAfterSpec().getSecond() != null) {
				buf.append(aca.getFirstOrAfterSpec().getSecond().getQuotedName().getSQL()).append(" ");
			}
		}
	}
	
	public void emitDropColumnAction(DropColumnAction dca, StringBuilder buf) {
		buf.append("DROP ").append(dca.getDroppedColumn().getName().getSQL());
	}
		
	public void emitChangeColumnAction(SchemaContext sc, ConnectionValues cv, ChangeColumnAction stmt, StringBuilder buf) {
		buf.append(" CHANGE ").append(stmt.getOldDefinition().getName().getSQL()).append(" ");
		emitDeclaration(sc, cv, stmt.getNewDefinition(), buf);
		final Pair<String, Name> firstOrAfterSpec = stmt.getFirstOrAfterSpec();
		if (firstOrAfterSpec != null) {
			final Name afterColumn = firstOrAfterSpec.getSecond();
			buf.append(" ").append(firstOrAfterSpec.getFirst());
			if (afterColumn != null) {
				buf.append(" ").append(afterColumn.getQuotedName().getSQL());
			}
		}
	}
	
	public void emitAlterColumnAction(SchemaContext sc, ConnectionValues cv, AlterColumnAction stmt, StringBuilder buf) {
		buf.append(" ALTER COLUMN ").append(stmt.getAlteredColumn().getName().getSQL());
		if (stmt.isDropDefault())
			buf.append(" DROP DEFAULT");
		else {
			buf.append(" SET DEFAULT ");
			emitExpression(sc, cv, stmt.getNewDefault(), buf, -1);
		}
	}
	
	/**
	 * @param stmt
	 * @param buf
	 */
	public void emitShowProcesslistStatement(ShowProcesslistStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"SHOW PROCESSLIST ");
	}

	/**
	 * @param stmt
	 * @param buf
	 */
	public void emitShowSitesStatusStatement(ShowSitesStatusStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"SHOW SITES STATUS ");
	}
	
	public void emitShowErrorsWarningsStatement(ShowErrorsWarningsStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"SHOW " + stmt.getLevel().getSQLName() + " ");
	}
	
	public void emitTableMaintenanceStatement(SchemaContext sc, ConnectionValues cv, TableMaintenanceStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,stmt.getCommand().getSqlCommand() + " ");
		buf.append(stmt.getOption().getSql());
		buf.append(" TABLE ");

		boolean first = true;
		for (TableInstance table : stmt.getTableInstanceList()) {
			if (!first) {
				buf.append(",");
			}
			first = false;
			emitTableInstance(sc, cv, table, buf, TableInstanceContext.NAKED);
		}
	}
	

	public void emitExternalServiceControlStatement(ExternalServiceControlStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,stmt.getAction().name() + " EXTERNAL SERVICE");
	}
	
	public void emitExternalServiceDeclaration(PEExternalService p, StringBuilder buf) {
		buf.append("EXTERNAL SERVICE ").append(p.getExternalServiceName()).append(" USING ").append(p.getOptions());
	}
	
	public void emitIntervalExpression(SchemaContext sc, ConnectionValues cv, IntervalExpression e, StringBuilder buf, int indent) {
		buf.append(" INTERVAL ");
		emitExpression(sc, cv, e.getExpr_unit(), buf, indent);
		buf.append(" " + e.getUnit());
	}

	private void emitContainerDeclaration(SchemaContext sc, PEContainer p, StringBuilder buf) {
		buf.append("CONTAINER ").append(p.getName()).append(" PERSISTENT GROUP ").append(p.getDefaultStorage(sc).getName().getSQL())
			.append(" ").append(p.getContainerDistributionModel().getSQL()).append(" DISTRIBUTE");
		if (p.getContainerDistributionModel() == DistributionVector.Model.RANGE)
			buf.append(" USING ").append(p.getRange(sc).getName().getSQL());
		
	}

	public void emitTemplateDeclaration(PETemplate pet, StringBuilder buf) {
		buf.append("TEMPLATE ").append(pet.getName().getSQL()).append(" XML='");
		buf.append(pet.getDefinition()).append("'");
	}
	
	public void emitViewDeclaration(SchemaContext sc, ConnectionValues cv, PEView view, PECreateViewStatement pecs, StringBuilder buf) {
		if (pecs != null && pecs.isCreateOrReplace()) 
			buf.append(" OR REPLACE");
		buf.append(" ALGORITHM = ").append(view.getAlgorithm());
		if (!view.getDefiner(sc).isRoot()) {
			// TODO:
			// having some issues getting this right for root
			buf.append(" DEFINER = ");
			emitUserSpec(view.getDefiner(sc),buf);
		}
		buf.append(" SQL SECURITY ").append(view.getSecurity());
		buf.append(" VIEW ").append(view.getName().getSQL()).append(" AS ");
		emitDMLStatement(sc,cv, view.getViewDefinition(sc, (pecs == null ? null : pecs.getViewTable()),false),buf, -1);
		if (!"NONE".equals(view.getCheckOption())) {
			buf.append(" WITH ").append(view.getCheckOption()).append(" CHECK OPTION");
		}
	}


	// this is for errors
	public void emitDebugViewDeclaration(SchemaContext sc, ConnectionValues cv, PEViewTable viewTable, StringBuilder buf) {
		emitViewDeclaration(sc, cv, viewTable.getView(sc), null, buf);
		buf.append(" TABLE ");
		// turn omit dist vect now
		EmitOptions was = options;
		try {
			if (options == null)
				options = EmitOptions.TEST_TABLE_DECLARATION;
			else
				options = options.addOmitDistVect();
			emitTableDeclaration(sc, cv, viewTable,buf);
		} finally {
			options = was;
		}
	}
	
	public void emitTriggerDeclaration(SchemaContext sc, ConnectionValues cv, PETrigger trigger, PECreateTriggerStatement pecs, StringBuilder buf) {
		if (!trigger.getDefiner(sc).isRoot()) {
			// TODO:
			// having some issues getting this right for root
			buf.append(" DEFINER = ");
			emitUserSpec(trigger.getDefiner(sc),buf);
		}
		buf.append(" TRIGGER ").append(trigger.getName()).append(" ").append(trigger.getTime().name());
		buf.append(" ").append(trigger.getEvent().name()).append(" ON ");
		emitTable(sc,cv,trigger.getTargetTable(),sc.getCurrentDatabase(false),buf);
		buf.append(" FOR EACH ROW ");
		buf.append(trigger.getBodySource());
	}

	
	public void emitAnalyzeKeysStatement(final SchemaContext sc, final ConnectionValues cv, AnalyzeKeysStatement aks, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"ANALYZE KEYS ");
		Functional.join(aks.getTables(), buf, ",", new BinaryProcedure<TableInstance,StringBuilder>() {

			@Override
			public void execute(TableInstance aobj, StringBuilder bobj) {
				emitTableInstance(sc,cv,aobj,bobj,TableInstanceContext.NAKED);
			}
			
		});
	}
	
	protected void emitUserSpec(PEUser peu, StringBuilder buf) {
		buf.append("'").append(peu.getUserScope().getUserName()).append("'@'").append(peu.getUserScope().getScope()).append("' ");  
	}
			
	public abstract void emitUserDeclaration(PEUser peu, StringBuilder buf);
	public abstract void emitSetPasswordStatement(SetPasswordStatement sps, StringBuilder buf);
	
	public abstract void emitLoadDataInfileStatement(SchemaContext sc, ConnectionValues cv, LoadDataInfileStatement s, StringBuilder buf, int indent);
	

	public void emitCompoundStatement(SchemaContext sc,ConnectionValues cv, CompoundStatement s, StringBuilder buf) {
		emitCompoundStatement(sc,cv,s,buf,0);
	}
		
	public void emitCompoundStatement(SchemaContext sc, ConnectionValues cv, CompoundStatement s, StringBuilder buf, int indent) {
		if (s instanceof CompoundStatementList) {
			emitCompoundStatementList(sc, cv,(CompoundStatementList)s, buf, indent);
		} else if (s instanceof CaseStatement) {
			emitCaseStatement(sc, cv,(CaseStatement)s, buf, indent);
		} else {
			error("Unknown Compound statement kind for emitter: " + s.getClass().getName());
		}
	}

	public void emitStatementForCompoundStatement(SchemaContext sc, ConnectionValues cv, Statement stmt, StringBuilder buf, int indent) {
		if (stmt instanceof CompoundStatement) {
			emitCompoundStatement(sc,cv,(CompoundStatement)stmt,buf,indent);
		} else {
			emitDMLStatement(sc,cv,(DMLStatement)stmt,buf,indent);
		}	
		buf.append("; ");
	}
	
	public void emitCompoundStatementList(SchemaContext sc, ConnectionValues cv, CompoundStatementList s, StringBuilder buf, int indent) {
		emitIndent(buf,indent, "BEGIN ");
		for(Statement stmt : s.getStatementsEdge()) {
			// let's try to preserve the indentation junk
			emitStatementForCompoundStatement(sc,cv,stmt,buf,indent);
		}
		emitIndent(buf,indent," END");	
	}

	public void emitCaseStatement(SchemaContext sc, ConnectionValues cv, CaseStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"CASE ");
		emitExpression(sc,cv,stmt.getTestExpression(),buf,indent);
		for(StatementWhenClause swc : stmt.getWhenClausesEdge()) {
			emitIndent(buf,indent," WHEN ");
			emitExpression(sc,cv,swc.getTestExpression(),buf,indent);
			emitIndent(buf,indent," THEN ");
			emitStatementForCompoundStatement(sc,cv,swc.getResultStatement(),buf,indent);
		}
		if (stmt.getElseResult() != null) {
			emitIndent(buf,indent," ELSE ");
			emitStatementForCompoundStatement(sc,cv,stmt.getElseResult(),buf,indent);
		}
		emitIndent(buf,indent," END CASE");
	}
	
	// options
	public static class EmitOptions extends Options<EmitOption> {

		private EmitOptions() {
			super();
		}
		
		private EmitOptions(EmitOptions o) {
			super(o);
		}
		
		@Override
		protected Options<EmitOption> copy() {
			return new EmitOptions(this);
		}
		
		public static final EmitOptions NONE = new EmitOptions();
		public static final EmitOptions PEMETADATA = NONE.add(EmitOption.PEMETADATA, Boolean.TRUE);
		// setting for result set metadata
		public static final EmitOptions RESULTSETMETADATA = NONE.add(EmitOption.UNQUALIFIEDCOLUMNS, Boolean.TRUE);
		public static final EmitOptions INFOSCHEMA_VIEW = NONE.add(EmitOption.INFOSCHEMA_VIEW, Boolean.TRUE);
		// setting for table definitions - skips comments
		public static final EmitOptions TABLE_DEFINITION = NONE.add(EmitOption.TABLE_DEFINITION, Boolean.TRUE);
		// setting for external table declarations - comments and autoincs, also puts dist info in pe comments
		public static final EmitOptions EXTERNAL_TABLE_DECLARATION = NONE.add(EmitOption.TABLE_DECLARATION, Boolean.TRUE);
		// setting for emitting generic sql
		public static final EmitOptions GENERIC_SQL = NONE.addGenericSQL();
		// if this is set - don't add dist vect decl
		public static final EmitOptions TEST_TABLE_DECLARATION = EXTERNAL_TABLE_DECLARATION.addOmitDistVect();
		
		private EmitOptions add(EmitOption opt, Object v) {
			return (EmitOptions)addSetting(opt, v);
		}
		
		public String getMultilinePretty() {
			return (String) getSetting(EmitOption.MULTILINE_PRETTYPRINT);
		}
		
		public EmitOptions addMultilinePretty(String indent) {
			return this.add(EmitOption.MULTILINE_PRETTYPRINT, indent);
		}
		
		public boolean emitPEMetadata() {
			return hasSetting(EmitOption.PEMETADATA);
		}
		
		public boolean isResultSetMetadata() {
			return hasSetting(EmitOption.UNQUALIFIEDCOLUMNS);
		}
		
		public boolean isTableDefinition() {
			return hasSetting(EmitOption.TABLE_DEFINITION);
		}
		
		public boolean isExternalTableDeclaration() {
			return hasSetting(EmitOption.TABLE_DECLARATION);
		}
		
		public boolean isGenericSQL() {
			return hasSetting(EmitOption.GENERIC_SQL);
		}
		
		public EmitOptions addGenericSQL() {
			return this.add(EmitOption.GENERIC_SQL, Boolean.TRUE);
		}
		
		public EmitOptions addForceParamValues() {
			return this.add(EmitOption.FORCE_PARAMETER_VALUES, Boolean.TRUE);
		}
		
		public boolean isForceParamValues() {
			return hasSetting(EmitOption.FORCE_PARAMETER_VALUES);
		}

		public EmitOptions addQualifiedTables() {
			return this.add(EmitOption.QUALIFIED_TABLES, Boolean.TRUE);
		}
		
		public boolean isQualifiedTables() {
			return hasSetting(EmitOption.QUALIFIED_TABLES);
		}
		
		public EmitOptions addOmitDistVect() {
			return this.add(EmitOption.OMIT_DIST_VECT, Boolean.TRUE);
		}
		
		public boolean isOmitDistVect() {
			return hasSetting(EmitOption.OMIT_DIST_VECT);
		}
		
		public EmitOptions analyzerLiteralsAsParameters() {
			return this.add(EmitOption.ANALYZER_EMIT_LITERALS_AS_PARAMETERS,Boolean.TRUE);
		}
		
		public boolean isAnalyzerLiteralsAsParameters() {
			return hasSetting(EmitOption.ANALYZER_EMIT_LITERALS_AS_PARAMETERS);
		}
		
		public EmitOptions addViewTableDecls() {
			return this.add(EmitOption.VIEW_DECL_EMIT_TABLE_DECL,Boolean.TRUE);
		}
		
		public boolean isAddViewTableDecls() {
			return hasSetting(EmitOption.VIEW_DECL_EMIT_TABLE_DECL);
		}
		
		public EmitOptions addCatalog() {
			return this.add(EmitOption.CATALOG, Boolean.TRUE);
		}
		
		public boolean isCatalog() {
			return hasSetting(EmitOption.CATALOG);
		}
		
		public EmitOptions addTriggerBody() {
			return this.add(EmitOption.TRIGGER_BODY,Boolean.TRUE);
		}
		
		public boolean isTriggerBody() {
			return hasSetting(EmitOption.TRIGGER_BODY);
		}
		
	}
	
	public enum EmitOption {

		// multiline prettyprint format - used in debugging
		MULTILINE_PRETTYPRINT,
		// if pemetadata is set, we'll emit our metadata extensions
		PEMETADATA,
		UNQUALIFIEDCOLUMNS,
		// if set, we're build one of our info schema view queries
		INFOSCHEMA_VIEW,
		// we're emitting a table definition - no comments, but autoincs and suppressed fks are included
		TABLE_DEFINITION,
		// we're emitting a table declaration - everything including autoincs and suppressed fks
		TABLE_DECLARATION,
		// we're emitting generic sql - emit tokens for literals (but not for temp tables)
		GENERIC_SQL,
		// used in late sorting inserts - don't bother emitting builder literals (go straight to the literals)
		FORCE_PARAMETER_VALUES,
		// for testing - when we do table decls - don't emit the pe extension comments
		OMIT_DIST_VECT,
		// used in the analyzer - emit all literals as parameters
		ANALYZER_EMIT_LITERALS_AS_PARAMETERS,
		// for planner error messages - emit the table declarations for views as well (pe extension)
		VIEW_DECL_EMIT_TABLE_DECL,
		// we are querying the catalog directly, do not emit db entries
		CATALOG,
		// if set, then all table refs should be fully qualified - used in storage gen add
		QUALIFIED_TABLES,
		// if set, we are generating for a trigger body
		TRIGGER_BODY
	}
	
	public static class EmitContext {
		
		protected TokenStream tns;
		
		public EmitContext(TokenStream t) {
			tns = t;
		}
		
		public String getOriginalText(SourceLocation sloc) {
			return sloc.getText(tns);
		}
	}
}

