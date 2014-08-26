package com.tesora.dve.db.mysql;

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

import com.tesora.dve.common.catalog.User;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.sql.node.expression.CaseExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.IndexHint;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.WhenClause;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Comment;
import com.tesora.dve.sql.schema.HasName;
import com.tesora.dve.sql.schema.Lookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaLookup;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheAwareLookup;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.schema.modifiers.TableModifierTag;
import com.tesora.dve.sql.schema.modifiers.TableModifiers;
import com.tesora.dve.sql.statement.ddl.PEDropStatement;
import com.tesora.dve.sql.statement.ddl.SetPasswordStatement;
import com.tesora.dve.sql.statement.session.LoadDataInfileStatement;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;

public class MysqlEmitter extends Emitter {

	// implement mysql's fun case insensitivity
	
	@Override
	public String getPersistentName(SchemaContext sc, PEAbstractTable<?> t) {
		return getPersistentName(t.getName(sc));
	}

	@Override
	public String getPersistentName(PEColumn c) {
		return getPersistentName(c.getName());
	}

	private String getPersistentName(Name n) {
		return n.get();
	}
	
	@Override
	public Emitter buildNew() {
		return new MysqlEmitter();
	}
	
	@Override
	public void emitOperatorFunctionCall(SchemaContext sc, FunctionCall fc, StringBuilder buf, int pretty) {
		if (fc.getFunctionName().getCapitalized().get().equals("LIKE")) {
			// mysql has that weird escape syntax - do it here if there are three params
			if (fc.getParameters().size() == 3) {
				emitExpression(sc,fc.getParameters().get(0), buf);
				if (fc.getFunctionName().isNotLike()) {
					buf.append(" NOT");
				}
				buf.append(" LIKE ");
				emitExpression(sc,fc.getParameters().get(1), buf);
				buf.append(" ESCAPE ");
				emitExpression(sc,fc.getParameters().get(2), buf);
				return;
			}
		}
		super.emitOperatorFunctionCall(sc,fc, buf, pretty);
	}

	
	
	@Override
	public void emitComment(Comment c, StringBuilder buf) {
		if (c == null || this.hasOptions() && getOptions().isTableDefinition())
			return;
		buf.append(" COMMENT '").append(c.getComment()).append("'");
	}

	@Override
	public void emitTableModifiers(SchemaContext sc, PETable tab, TableModifiers mods, StringBuilder buf) {
		for(TableModifierTag tmt : TableModifierTag.values()) {
			TableModifier tm = mods.getModifier(tmt);
			if (tm == null) continue;
			if (tmt == TableModifierTag.AUTOINCREMENT && (!this.hasOptions() || !getOptions().isExternalTableDeclaration()))
				continue;
			if (tmt == TableModifierTag.DEFAULT_COLLATION && tab != null && !tab.shouldEmitCollation(sc)) continue;
			buf.append(" ");
			tm.emit(sc,this,buf);
		}
	}

	@Override
	public void emitColumnInstance(SchemaContext sc, ColumnInstance cr, StringBuilder buf) {
		if (this.hasOptions() && getOptions().isResultSetMetadata()) {
			boolean useSpecified = (cr.getParent() instanceof FunctionCall || cr.getParent() instanceof WhenClause || cr.getParent() instanceof CaseExpression);
			Name specified = cr.getSpecifiedAs();
			if (useSpecified && specified != null)
				buf.append(specified.getSQL());
			else
				buf.append(cr.getColumn().getName().get());
		} else {
			super.emitColumnInstance(sc,cr, buf);
		}
	}

	
	@Override
	public void emitTableInstance(SchemaContext sc, TableInstance tr, StringBuilder buf, boolean includeAlias) {
		super.emitTableInstance(sc, tr, buf, includeAlias);
		if (includeAlias) {
			if (tr.getHints() != null) {
				for(IndexHint ih : tr.getHints()) {
					emitHint(ih,buf);
				}
			}
		}
	}
	
	private void emitHint(IndexHint ih, StringBuilder buf) {
		buf.append(" ").append(ih.getHintType().getSQL());
		if (ih.isKey())
			buf.append(" KEY");
		else
			buf.append(" INDEX");
		if (ih.getHintTarget() != null) {
			buf.append(" FOR ").append(ih.getHintTarget().getSQL());
		}
		if (!ih.getIndexNames().isEmpty()) {
			buf.append(" (");
			Functional.join(ih.getIndexNames(), buf, ", ", new BinaryProcedure<UnqualifiedName,StringBuilder>() {

				@Override
				public void execute(UnqualifiedName aobj, StringBuilder bobj) {
					bobj.append(aobj.getSQL());
				}
				
			});
			buf.append(")");
		}
		buf.append(" ");
	}
	
	@Override
	public <T> Lookup<T> getLookup() {
		return new Lookup<T>(false,false,null);
	}

	@Override
	public void emitUserDeclaration(PEUser peu, StringBuilder buf) {
		emitUserSpec(peu,buf);
		if (peu.getPassword() != null)
			buf.append(" IDENTIFIED BY '").append(peu.getPassword()).append("'");
	}

	@Override
	public void emitSetPasswordStatement(SetPasswordStatement sps,
			StringBuilder buf) {
		buf.append("SET PASSWORD FOR ");
		emitUserSpec(sps.getTarget(),buf);
		buf.append(" = PASSWORD('").append(sps.getNewPassword()).append("')");
	}

	@Override
	public void emitDropUserStatement(PEDropStatement<PEUser, User> peds,
			StringBuilder buf) {
		buf.append("DROP USER ");
		emitUserSpec(peds.getTarget().get(), buf);
	}

	@Override
	public void emitLoadDataInfileStatement(SchemaContext sc, LoadDataInfileStatement stmt, StringBuilder buf, int indent) {
		emitIndent(buf,indent,"LOAD DATA ");
		if (stmt.getModifier() != null) {
			buf.append(stmt.getModifier().getSQL() + " ");
		}
		if (stmt.isLocal()) {
			buf.append("LOCAL ");
		}
		buf.append("INFILE ");
		buf.append(stmt.getFileName() + " ");
		if (stmt.isReplace()) {
			buf.append("REPLACE ");
		} else if (stmt.isIgnore()) {
			buf.append("IGNORE ");
		}
		buf.append("INTO ");
		buf.append(stmt.getTableName().getQuoted() + " ");
		if (stmt.getCharacterSet() != null) {
			buf.append("CHARACTER SET ");
			buf.append(stmt.getCharacterSet().get() + " ");
		}
		if (stmt.getColOption() != null) {
			buf.append("COLUMNS ");
			if (stmt.getColOption().getTerminated() != null) {
				buf.append("TERMINATED BY " + stmt.getColOption().getTerminated() + " ");
			}
			if (stmt.getColOption().getEnclosed() != null) {
				buf.append("ENCLOSED BY " + stmt.getColOption().getEnclosed() + " ");
			}
			if (stmt.getColOption().getEscaped() != null) {
				buf.append("ESCAPED BY " + stmt.getColOption().getEscaped() + " ");
			}
		}
		if (stmt.getLineOption() != null) {
			buf.append("LINES ");
			if (stmt.getLineOption().getStarting() != null) {
				buf.append("STARTING BY " + stmt.getLineOption().getStarting() + " ");
			}
			if (stmt.getLineOption().getTerminated() != null) {
				buf.append("TERMINATED BY " + stmt.getLineOption().getTerminated() + " ");
			}
		}
		if (stmt.getIgnoredLines() != null) {
			buf.append("IGNORE ");
			buf.append(stmt.getIgnoredLines() + " ");
			buf.append("LINES ");
		}
		if ((stmt.getColOrVarList() != null) && (stmt.getColOrVarList().size() > 0)) {
			buf.append("(");
			boolean first = true;
			for(Name colOrVar : stmt.getColOrVarList()) {
				if (!first) {
					buf.append(",");
				}
				buf.append(colOrVar.getQuoted());
				first = false;
			}
			buf.append(") ");
		}
		if ((stmt.getUpdateExprs() != null) && (stmt.getUpdateExprs().size() > 0)) {
			buf.append("SET ");
			emitExpressions(sc, stmt.getUpdateExprs(), buf, -1);
		}
	}

}
