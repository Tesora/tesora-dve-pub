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

import com.tesora.dve.server.messaging.SQLCommand;
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
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DeleteStatement;

public class AddStorageGenRangeDistKeyTableInfo extends
		AddStorageGenRangeTableInfo {

	private final TempTable sideTable;
	
	// the dist key offsets are also the side insert offsets, compute them ahead of time
	private final int[] offsets;
	
	public AddStorageGenRangeDistKeyTableInfo(SchemaContext sc,
			PETable srcTab, TempTable sideTable) {
		super(sc,srcTab);
		this.sideTable = sideTable;
		this.offsets = new int[distColumns.size()];
		for(int i = 0; i < offsets.length; i++)
			offsets[i] = i;
	}
	
	@Override
	public boolean hasSharedTable() {
		return true;
	}

	@Override
	public TempTable getSideTable() {
		return sideTable;
	}


	@Override
	public SQLCommand getSideDelete() {
		// delete a from A a inner join side s on a.dk1 = s.c1 and a.dk2 = s.c2 and a.dk3 = s.c3
		final TableInstance sideti = buildSideTableInstance();
		final TableInstance srcti = buildSrcTableInstance();
		List<ExpressionNode> equijoins = new ArrayList<ExpressionNode>();
		for(int i = 0; i < distColumns.size(); i++) {
			equijoins.add(new FunctionCall(FunctionName.makeEquals(),
					new ColumnInstance(sideTable.getColumns(cntxt).get(i),sideti),
					new ColumnInstance(distColumns.get(i),srcti)));
		}
		FromTableReference ftr = new FromTableReference(srcti);
		JoinedTable join = new JoinedTable(sideti,ExpressionUtils.safeBuildAnd(equijoins),JoinSpecification.INNER_JOIN);
		ftr.addJoinedTable(join);
		DeleteStatement ds = new DeleteStatement(Collections.singletonList(srcti),
				Collections.singletonList(ftr), null, Collections.<SortingSpecification> emptyList(), null, false,
				new AliasInformation(), null);
		return getCommand(ds);
	}

	@Override
	public int[] getSideInsertOffsets() {
		
		return offsets;
	}

}
