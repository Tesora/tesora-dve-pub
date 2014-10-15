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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.RangeDistribution;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.schema.types.Type;

public class AddStorageGenRangeInfo extends RebalanceInfo {

	private final RangeDistribution range;
	private final TempTable shared; 
	
	private final List<AddStorageGenRangeTableInfo> tables;
	
	public AddStorageGenRangeInfo(SchemaContext sc, RangeDistribution dr, List<PETable> tables) {
		super(sc);
		this.range = dr;
		this.tables = new ArrayList<AddStorageGenRangeTableInfo>();
		boolean nonintegral = false;
		for(NativeType t : range.getNativeTypes()) {
			if (!t.isIntegralType()) {
				nonintegral = true;
				break;
			}
		}
		if (nonintegral) {
			shared = null;
		} else {
			PEDatabase pdb = tables.get(0).getPEDatabase(cntxt);
			PEStorageGroup sg = tables.get(0).getStorageGroup(cntxt);
			List<PEColumn> ttc = new ArrayList<PEColumn>();
			int counter = 0;
			for(Type t : range.getTypes()) {
				PEColumn pec = PEColumn.buildColumn(cntxt, new UnqualifiedName("c" + (++counter)),
						t, Collections.<ColumnModifier> emptyList(), null, Collections.<ColumnKeyModifier> emptyList());
				ttc.add(pec);
			}
			try {
				this.shared = TempTable.buildAdHoc(cntxt, pdb, ttc, Model.BROADCAST, Collections.<PEColumn> emptyList(), sg, true);
			} catch (PEException pe) {
				throw new SchemaException(Pass.PLANNER,"Unable to create shared side table",pe);
			}
			this.shared.addConstraint(cntxt, ConstraintType.UNIQUE, ttc);
		}
		for(PETable tab : tables) {
			if (shared != null)
				this.tables.add(new AddStorageGenRangeDistKeyTableInfo(cntxt, tab, shared));
			else
				this.tables.add(new AddStorageGenRangePrivateTableInfo(cntxt, tab));
		}
	}

	public RangeDistribution getRange() {
		return range;
	}
	
	public boolean hasSharedTable() {
		return shared != null;
	}
	
	public TempTable getSharedTable() {
		return shared;
	}

	public SQLCommand getCreateSharedTable() {
		if (shared == null) return null;
		return getCreateSideTableStmt(shared);
	}
	
	public SQLCommand getTruncateSideTableStmt() {
		if (shared == null) return null;
		return getTruncateSideTableStmt(shared);
	}
	
	public SQLCommand getDropSharedTable() {
		if (shared == null) return null;
		return getDropSideTableStmt(shared);
	}
	
	public List<AddStorageGenRangeTableInfo> getTableInfos() {
		return tables;
	}
	
	public void display(PrintStream ps) {
		ps.println("Rebalance info for range " + getRange().getName().get());
		if (shared != null)
			ps.println("  " + getCreateSharedTable().getRawSQL());
		for(AddStorageGenRangeTableInfo info : tables) {
			info.display(ps);
		}
	}
	
}
