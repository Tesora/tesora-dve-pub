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

import java.util.SortedSet;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;

public final class Range extends FuzzyTableDistributionModel {

	private static final String FCL_BLOCK_NAME = "RangeDistribution";
	private static final String DISTRIBUTION_TEMPLATE_BLOCK_NAME = "Range";

	public static final TemplateItem SINGLETON_TEMPLATE_ITEM;
	static {
		try {
			SINGLETON_TEMPLATE_ITEM = new Range();
		} catch (final PEException e) {
			throw new Error(e);
		}
	}

	private Range() throws PEException {
		super(FCL_BLOCK_NAME);
	}

	public Range(final TableStats match, final SortedSet<Long> sortedCardinalities) throws PEException {
		super(FCL_BLOCK_NAME, match, sortedCardinalities);
	}

	protected Range(final double pcOrderBy, final double pcCardinality)
			throws PEException {
		super(FCL_BLOCK_NAME, pcOrderBy, pcCardinality);
	}

	@Override
	public String getFclName() {
		return FCL_BLOCK_NAME;
	}

	@Override
	public String getTemplateItemName() {
		return DISTRIBUTION_TEMPLATE_BLOCK_NAME;
	}

	@Override
	protected String getFlvName() {
		return getTemplateItemName();
	}
}