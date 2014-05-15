// OS_STATUS: public
package com.tesora.dve.tools.aitemplatebuilder;

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