// OS_STATUS: public
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

import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;

class Partitions extends DerivedAttribute<CollapsedJoinGraph> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return EngineConstant.FROMCLAUSE.has(ln) || EngineConstant.WHERECLAUSE.has(ln);
	}

	@Override
	public CollapsedJoinGraph computeValue(SchemaContext sc, LanguageNode ln) {
		MultiTableDMLStatement mdml = null;
		if (ln instanceof MultiTableDMLStatement)
			mdml = (MultiTableDMLStatement) ln;
		else if (ln instanceof InsertIntoSelectStatement) {
			InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) ln;
			mdml = iiss.getSource();
		}
			
		UncollapsedJoinGraph ujg = new UncollapsedJoinGraph(sc,mdml);
		CollapsedJoinGraph cjg = new CollapsedJoinGraph(sc,ujg);
		
		return cjg;
	}

}
