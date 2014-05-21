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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

// a multipart is a collection of colocated single table partitions
public class DMPart extends DPart {

	// flatten out any added partitions to their component unary partitions
	ListSet<DunPart> parts = new ListSet<DunPart>();
	// keep track of the table keys just for efficiency
	ListSet<TableKey> tables = new ListSet<TableKey>();
	// embedded joins - these are not part of the graph
	ListSet<JoinEdge> embedded = new ListSet<JoinEdge>();
	
	public DMPart(int id) {
		super(id);
	}

	// population of a dmpart is solely by adding in joins - if the join is colocated
	// then the DMPart can hold both parts
	public void take(JoinEdge je) {
		embedded.add(je);
		take(je.getFrom());
		take(je.getTo());
	}
	
	public void take(DPart dp) {
		parts.addAll(dp.getUnaryParts());
		tables.addAll(dp.getTables());
	}
	
	@Override
	public boolean isUnary() {
		return false;
	}
	
	@Override
	public ListSet<TableKey> getTables() {
		return tables;
	}

	@Override
	public String getGraphRole() {
		return "MultiPartition";
	}

	@Override
	public List<JoinEdge> getEmbeddedJoins() {
		return embedded;
	}
	
	@Override
	public List<DunPart> getUnaryParts() {
		return parts;
	}

	@Override
	protected void describeInternal(final SchemaContext sc, String indent, StringBuilder buf) {
		super.describeInternal(sc,indent,buf);
		for(JoinEdge je : embedded) {
			buf.append("E ");
			je.describe(sc, "", buf);
			buf.append(PEConstants.LINE_SEPARATOR);
		}
	}
	
	@Override
	public DistributionVector getGoverningVector(final SchemaContext sc, TableKey forTable) {
		DistributionVector dvect = forTable.getAbstractTable().getDistributionVector(sc);
		if (dvect.isBroadcast()) {
			// find all the vectors, find the most restrictive (pick the first one)
			List<DistributionVector> vects = Functional.apply(tables, new UnaryFunction<DistributionVector,TableKey>() {
		
				@Override
				public DistributionVector evaluate(TableKey object) {
					return object.getAbstractTable().getDistributionVector(sc);
				}
				
			});
			Collections.sort(vects, DistributionVector.orderByModel);
			return vects.get(vects.size() - 1);
		}
		return dvect;
	}

}
