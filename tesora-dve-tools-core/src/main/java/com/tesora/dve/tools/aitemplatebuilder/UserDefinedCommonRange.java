package com.tesora.dve.tools.aitemplatebuilder;

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

import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.tools.CLIBuilder.ColorStringBuilder;
import com.tesora.dve.tools.aitemplatebuilder.AiTemplateBuilder.MessageSeverity;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;

public class UserDefinedCommonRange extends CommonRange {

	private final String name;

	public UserDefinedCommonRange(final String name, final boolean isSafeMode) {
		super(isSafeMode);
		this.name = name;
	}

	@Override
	public void addUserDefinedDistribution(final TableStats table, final Set<String> dv) throws PEException {
		super.addUserDefinedDistribution(table, dv);
	}

	@Override
	public String getTemplateItemName() {
		return this.name;
	}

	@Override
	public String toString() {
		final ColorStringBuilder value = new ColorStringBuilder();
		value.append(super.toString()).append(" (").append("user defined", MessageSeverity.ALERT.getColor()).append(")");
		return value.toString();
	}

}
