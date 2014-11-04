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


import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.Parameter;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.*;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public abstract class AddStorageGenRangeTableInfo extends RebalanceInfo {
	protected final PETable srcTab;
    protected final Database srcDB;
    protected final KeyValue emptyDistKey;
	protected final List<PEColumn> distColumns;
	protected final int[] distKeyOffsets;
	
	public AddStorageGenRangeTableInfo(SchemaContext sc, PETable srcTab) {
		super(sc);
		this.srcTab = srcTab;
        this.srcDB = srcTab.getDatabase(sc);
        DistributionVector distributionVector = srcTab.getDistributionVector(sc);
        this.distColumns = distributionVector.getColumns(sc);
		this.distKeyOffsets = new int[distColumns.size()];
		for(int i = 0; i < distKeyOffsets.length; i++) {
			distKeyOffsets[i] = i;
		}

        this.emptyDistKey = distributionVector.buildEmptyKeyValue(sc);
	}

    public PETable getSourceTable(){
        return srcTab;
    }

    public Name getName(){
        return srcTab.getName();
    }

    public Database getDatabase(){
        return srcDB;
    }

    public KeyValue getEmptyDistKey(){
        return new KeyValue(emptyDistKey);
    }

	public abstract boolean hasSharedTable();
	
	public abstract TempTable getSideTable();

	public abstract SQLCommand getSideDelete();
	
	public int[] getSideInsertOffsets() {
		return getDistKeyOffsets();
	}
	
	public int[] getDistKeyOffsets() {
		return distKeyOffsets;
	}
	
	public SQLCommand getTruncateSideTableStmt() {
		TableInstance ti = buildSideTableInstance(); 
		TruncateStatement trunc = new TruncateStatement(ti,null);
		return getCommand(trunc);
	}
	
	public SQLCommand getCreateSideTableStmt() {
		PECreateTableStatement stmt = new PECreateTableStatement(getSideTable(),true,false);
		return getCommand(stmt);
	}
	
	public SQLCommand getDropSideTableStmt() {
		PEDropTableStatement stmt = new PEDropTableStatement(cntxt,Collections.singletonList(buildSideTableInstance().getTableKey()),
				Collections.<Name> emptyList(),true,false);
		return getCommand(stmt);
	}

	public SQLCommand getChunkQuery() {
		SelectStatement q = buildSkeleton(false);
		TableInstance ti = q.getTables().get(0).getBaseTable();
		q.setProjection(buildProjection(ti,distColumns));
		return getCommand(q);
	}

	public SQLCommand getDataChunkQuery() {
		SelectStatement q = buildSkeleton(false);
		TableInstance ti = q.getTables().get(0).getBaseTable();
		q.setProjection(buildProjection(ti,getFullColumns()));
		return getCommand(q);
	}

	// the insert column order is whatever getFullColumns returns,
	// which conveniently is also whatever comes back from the data chunk query 
	public SQLCommand getSrcInsertPrefix() {
		final TableInstance ti = new TableInstance(srcTab,srcTab.getName().getUnqualified(), null, false);
		final List<ExpressionNode> columns = Functional.apply(getFullColumns(), new UnaryFunction<ExpressionNode,PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				return new ColumnInstance(object,ti);
			}
			
		});
		final InsertIntoValuesStatement iivs = new InsertIntoValuesStatement(ti,columns,Collections.<List<ExpressionNode>> emptyList(),Collections.<ExpressionNode> emptyList(),new AliasInformation(),null);
		return getInsertPrefix(iivs);
	}

	public SQLCommand getSideInsertPrefix() {
		final TableInstance ti = buildSideTableInstance(); 
		final List<ExpressionNode> columns = Functional.apply(getSideTable().getColumns(cntxt), new UnaryFunction<ExpressionNode,PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				return new ColumnInstance(object,ti);
			}
			
		});
		final InsertIntoValuesStatement iivs = new InsertIntoValuesStatement(ti,columns,Collections.<List<ExpressionNode>> emptyList(),Collections.<ExpressionNode> emptyList(),new AliasInformation(),null);
        iivs.setIgnore(true);
		return getInsertPrefix(iivs);
	}
	
	public void display(String prefix, String tab, PrintStream ps) {
        String prefixWithIndent = prefix+tab;
		ps.println();
		ps.println(prefix + srcTab.getPEDatabase(cntxt).getName().get() + "." + srcTab.getName().get() + ": ");
		if (!hasSharedTable()) {
			ps.println(prefix + "Side table:");
			ps.println(prefixWithIndent+ getCreateSideTableStmt().getRawSQL());
			ps.println(prefixWithIndent+ getDropSideTableStmt().getRawSQL());
		}
		ps.println(prefix + "Chunk query");
		ps.println(prefixWithIndent + getChunkQuery().getRawSQL());
		ps.println(prefix + "Data chunk query");
		ps.println(prefixWithIndent + getDataChunkQuery().getRawSQL());
		ps.println(prefix + "Side insert offsets");
		StringBuffer buf = new StringBuffer();
		for(int i = 0; i < getSideInsertOffsets().length; i++) {
			if (i > 0) buf.append(", ");
			buf.append(getSideInsertOffsets()[i]);
		}
		ps.println(prefixWithIndent + buf.toString());
		ps.println(prefix + "Side insert prefix");
		ps.println(prefixWithIndent + getSideInsertPrefix().getRawSQL());
		ps.println(prefix + "Src insert prefix");
		ps.println(prefixWithIndent + getSrcInsertPrefix().getRawSQL());
		ps.println(prefix + "Delete");
		ps.println(prefixWithIndent + getSideDelete().getRawSQL());
	}
	

	protected List<PEColumn> getFullColumns() {
		List<PEColumn> proj = new ListSet<PEColumn>(distColumns);
		for(PEColumn pec : srcTab.getColumns(cntxt)) 
			proj.add(pec);
		return proj;
	}
	
	protected List<ExpressionNode> buildProjection(final TableInstance ti, List<PEColumn> columns) {
		List<ExpressionNode> proj = Functional.apply(columns, new UnaryFunction<ExpressionNode,PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				ColumnInstance ci = new ColumnInstance(object,ti);
				return new ExpressionAlias(ci,new NameAlias(object.getName().getUnqualified()),false);
			}
			
		});
		return proj;
	}
	
	protected SelectStatement buildSkeleton(boolean withLimitAndOffset) {
		final TableInstance ti = buildSrcTableInstance();
		List<SortingSpecification> sorts = Functional.apply(distColumns, new UnaryFunction<SortingSpecification,PEColumn>() {

			@Override
			public SortingSpecification evaluate(PEColumn object) {
				SortingSpecification ss = new SortingSpecification(new ColumnInstance(object,ti),true);
				ss.setOrdering(true);
				return ss;
			}
			
		});
		// do I need to set positions?  hmmm
        //TODO: leave out the limits for now, since using them requires iteration inside the rebalance. -sgossard

		SelectStatement ss = new SelectStatement(new AliasInformation());
		ss.setTables(ti);
		ss.setOrderBy(sorts);
        if (withLimitAndOffset) {
            LimitSpecification limit = new LimitSpecification(new Parameter(null),new Parameter(null));
            ss.setLimit(limit);
        }
		return ss;
	}

	protected TableInstance buildSideTableInstance() {
		return new TempTableInstance(cntxt,getSideTable());
	}

	protected TableInstance buildSrcTableInstance() {
		return new TableInstance(srcTab,srcTab.getName().getUnqualified(), new UnqualifiedName("q"), false);
	}
}
