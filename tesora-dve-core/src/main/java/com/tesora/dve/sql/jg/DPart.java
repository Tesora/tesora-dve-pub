// OS_STATUS: public
package com.tesora.dve.sql.jg;

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

import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.BinaryProcedure;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

public abstract class DPart extends DGVertex<JoinEdge> {

	protected DPart(int id) {
		super(id);
	}
	
	public abstract boolean isUnary();
	
	public abstract ListSet<TableKey> getTables();
	
	public boolean isBroadcastDistributed(SchemaContext sc) {
		for(TableKey ti : getTables()) {
			PEAbstractTable<?> tab = ti.getAbstractTable();
			if (tab.getDistributionVector(sc) == null)
				return false;
			if (!DistributionVector.Model.BROADCAST.equals(tab.getDistributionVector(sc).getModel()))
				return false;
		}
		return true;
	}
	
	public PEStorageGroup getStorageGroup(SchemaContext sc) {
		HashSet<PEStorageGroup> groups = new HashSet<PEStorageGroup>();
		for(TableKey ti : getTables())
			groups.add(ti.getAbstractTable().getStorageGroup(sc));
		if (groups.size() > 1)
			throw new SchemaException(Pass.PLANNER, "Partition across multiple persistent groups");
		return groups.iterator().next();
	}

	@Override
	protected void describeInternal(final SchemaContext sc, String indent, StringBuilder buf) {
		describeTables(sc,buf);
		buf.append(PEConstants.LINE_SEPARATOR);
		for(JoinEdge e : edges) {
			e.describe(sc, indent + "  ",buf);
			buf.append(PEConstants.LINE_SEPARATOR);
		}
	}
	
	protected void describeTables(final SchemaContext sc, StringBuilder buf) {
		buf.append("{");
		Functional.join(getTables(), buf, ", ", new BinaryProcedure<TableKey,StringBuilder>() {

			@Override
			public void execute(TableKey aobj, StringBuilder bobj) {
				bobj.append(aobj.describe(sc));
			}
			
		});
		buf.append("}");
	}
	
	protected String terseDescribe(final SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		describeSelf(sc,"",buf);
		describeTables(sc,buf);
		return buf.toString();
	}
	
	public abstract List<JoinEdge> getEmbeddedJoins();
	
	public void update(DPart replacement) {
		for(JoinEdge je : getEdges()) {
			if (je.getFrom() == this) {
				je.setFrom(replacement);
			} else if (je.getTo() == this) {
				je.setTo(replacement);
			}
		}
	}
	
	public abstract List<DunPart> getUnaryParts();

	public abstract DistributionVector getGoverningVector(SchemaContext sc, TableKey forTable);	
}
