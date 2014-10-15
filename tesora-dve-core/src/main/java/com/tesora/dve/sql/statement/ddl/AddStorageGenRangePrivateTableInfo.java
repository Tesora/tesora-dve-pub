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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.util.ListSet;

public class AddStorageGenRangePrivateTableInfo extends
		AddStorageGenRangeTableInfo {

	private final TempTable sideTable;
	private final PEKey joinKey;
	
	public AddStorageGenRangePrivateTableInfo(SchemaContext sc, PETable srcTab) {
		super(sc, srcTab);
		List<PEKey> keys = srcTab.getUniqueKeys(sc);
		if (keys.isEmpty()) 
			throw new SchemaException(Pass.PLANNER, "No unique key available for table " + srcTab.getName());
		// we prefer primary to unique
		PEKey primary = null;
		PEKey unique = null;
		for(PEKey pek : keys) {
			if (pek.isPrimary()) primary = pek;
			else if (unique == null) unique = pek;
			else if (unique.getKeyColumns().size() > pek.getKeyColumns().size()) unique = pek;
		}
		this.joinKey = (primary != null ? primary : unique);
		PEDatabase pdb = srcTab.getPEDatabase(cntxt);
		PEStorageGroup sg = srcTab.getStorageGroup(cntxt);
		List<PEColumn> ttc = new ArrayList<PEColumn>();
		for(PEColumn pec : joinKey.getColumns(cntxt)) {
			PEColumn nc = PEColumn.buildColumn(cntxt,pec.getName().getUnquotedName(),
					pec.getType(), Collections.<ColumnModifier> emptyList(), null, Collections.<ColumnKeyModifier> emptyList());
			ttc.add(nc);
		}
		try {
			sideTable = TempTable.buildAdHoc(cntxt, pdb, ttc, Model.BROADCAST, Collections.<PEColumn> emptyList(), sg, true);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER,"Unable to create shared side table",pe);
		}
		sideTable.addConstraint(cntxt, joinKey.getConstraint(), ttc);

	}

	@Override
	public boolean hasSharedTable() {
		return false;
	}

	@Override
	public TempTable getSideTable() {
		return sideTable;
	}

	@Override
	protected List<PEColumn> getFullColumns() {
		// we arrange for the key columns to be just after the dist key columns and before anything else
		ListSet<PEColumn> out = new ListSet<PEColumn>();
		out.addAll(distColumns);
		for(PEColumn pec : joinKey.getColumns(cntxt)) 
			out.add(pec);
		for(PEColumn pec : srcTab.getColumns(cntxt)) 
			out.add(pec);
		return out;
	}
	
	// could be different
	public int[] getSideInsertOffsets() {
		List<PEColumn> fullCols = getFullColumns();
		int[] out = new int[joinKey.getKeyColumns().size()];
		for(int i = 0; i < joinKey.getKeyColumns().size(); i++) {
			out[i] = fullCols.indexOf(joinKey.getKeyColumns().get(i).getColumn());
		}
		return out;
	}
	
	@Override
	public SQLCommand getSideDelete() {
		// delete a from A a inner join side s on a.uk1 = s.uk1 and a.uk2 = s.uk2 and a.uk3 = s.uk3
		final TableInstance sideti = buildSideTableInstance();
		final TableInstance srcti = buildSrcTableInstance();
		List<ExpressionNode> equijoins = new ArrayList<ExpressionNode>();
		for(int i = 0; i < joinKey.getKeyColumns().size(); i++) {
			equijoins.add(new FunctionCall(FunctionName.makeEquals(),
					new ColumnInstance(sideTable.getColumns(cntxt).get(i),sideti),
					new ColumnInstance(joinKey.getKeyColumns().get(i).getColumn(),srcti)));
		}
		FromTableReference ftr = new FromTableReference(srcti);
		JoinedTable join = new JoinedTable(sideti,ExpressionUtils.safeBuildAnd(equijoins),JoinSpecification.INNER_JOIN);
		ftr.addJoinedTable(join);
		DeleteStatement ds = new DeleteStatement(Collections.singletonList(srcti),
				Collections.singletonList(ftr), null, Collections.<SortingSpecification> emptyList(), null, false,
				new AliasInformation(), null);
		return getCommand(ds);
	}

}
