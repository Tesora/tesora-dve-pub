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

import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.SiteInstanceStatus;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Pair;

public class PECreateSiteInstanceStatement extends
		PECreateStatement<PESiteInstance, SiteInstance> {

	public PECreateSiteInstanceStatement(PESiteInstance newInstance) {
		super(newInstance,true,TranslatorUtils.PERSISTENT_INSTANCE, false);
	}
	
	public static PECreateSiteInstanceStatement build(SchemaContext pc, Name instanceName, List<Pair<Name,LiteralExpression>> opts) {
		PESiteInstance newInstance = new PESiteInstance(pc,instanceName);
		parseOptions(pc,newInstance,opts,true);
		return new PECreateSiteInstanceStatement(newInstance);
	}
	

	public static void parseOptions(SchemaContext pc, PESiteInstance instance, List<Pair<Name,LiteralExpression>> options, boolean reqd) {
		String[] commonOpts = PECreateStorageSiteStatement.consumeCommonOptions(pc, options, reqd);
		if (commonOpts[0] != null)
			instance.setUrl(commonOpts[0]);
		if (commonOpts[1] != null)
			instance.setUser(commonOpts[1]);
		if (commonOpts[2] != null)
			instance.setPassword(commonOpts[2]);
			
		// parse the remaining options
		String status = null;
		for(Iterator<Pair<Name,LiteralExpression>> iter = options.iterator(); iter.hasNext();) {
			Pair<Name,LiteralExpression> p = iter.next();
			String n = p.getFirst().getUnquotedName().get();
			if (PESiteInstance.OPTION_STATUS.equalsIgnoreCase(n)) {
				if (p.getSecond().isStringLiteral() && SiteInstanceStatus.isValidUserOption(p.getSecond().asString(pc.getValues()))) {
					status = p.getSecond().asString(pc.getValues());
				} else {
					throw new SchemaException(Pass.SECOND, "Invalid value specified for option '" + p.getFirst().get() + "'.  Value = " + p.getSecond().toString());
				}				
			}
		}
		if (status != null)
			instance.setStatus(status);
	}
	
}
