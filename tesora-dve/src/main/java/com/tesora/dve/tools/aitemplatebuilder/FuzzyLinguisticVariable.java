// OS_STATUS: public
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sourceforge.jFuzzyLogic.FIS;
import net.sourceforge.jFuzzyLogic.FunctionBlock;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.exceptions.PEException;

public abstract class FuzzyLinguisticVariable implements TemplateItem {

	private static final Logger logger = Logger.getLogger(FuzzyLinguisticVariable.class);

	private static final Comparator<FuzzyLinguisticVariable> MODEL_SCORE_COMPARATOR = new Comparator<FuzzyLinguisticVariable>() {
		@Override
		public int compare(FuzzyLinguisticVariable a, FuzzyLinguisticVariable b) {
			if ((a == null) || (b == null)) {
				throw new NullPointerException();
			}

			if (a.getScore() > b.getScore()) {
				return 1;
			} else if (a.getScore() < b.getScore()) {
				return -1;
			} else {
				return 0;
			}
		}
	};

	private static final String FCL_SCHEMA_FILE_NAME = "DistributionModels.fcl";
	private static final String SCORE_FLV_NAME = "desirability";
	private final FunctionBlock ai;

	public static double toPercent(final double value, final double total) {
		return 100.0 * (value / total);
	}

	public static Comparator<FuzzyLinguisticVariable> getScoreComparator() {
		return MODEL_SCORE_COMPARATOR;
	}

	public static List<FuzzyLinguisticVariable> evaluateDistributionModels(final FuzzyLinguisticVariable... distributionModels) {

		final List<FuzzyLinguisticVariable> sortedDistributionModels = Arrays.asList(distributionModels);
		for (final FuzzyLinguisticVariable distributionModel : sortedDistributionModels) {
			distributionModel.evaluate();
		}

		Collections.sort(sortedDistributionModels, getScoreComparator());

		return sortedDistributionModels;
	}

	protected FuzzyLinguisticVariable(final String fclBlockName) throws PEException {
		final InputStream fclSchema = PEFileUtils.getResourceStream(FuzzyLinguisticVariable.class, FCL_SCHEMA_FILE_NAME);
		try {
			this.ai = FIS.load(fclSchema, false).getFunctionBlock(fclBlockName);
			if (this.ai == null) {
				throw new PEException("Could not load the Fuzzy Control Language (FCL) specification from '" + FCL_SCHEMA_FILE_NAME + "'");
			}
		} finally {
			try {
				fclSchema.close();
			} catch (final IOException e) {
				logger.error(e.getMessage(), e); // Could not close the file.
			}
		}
	}

	protected void setVariable(final String name, final double value) {
		ai.setVariable(name, value);
	}

	public void evaluate() {
		ai.evaluate();
	}

	public double getScore() {
		return ai.getVariable(SCORE_FLV_NAME).getLatestDefuzzifiedValue();
	}

	public void plotScoreFuzzyManifold() {
		ai.getVariable(SCORE_FLV_NAME).chartDefuzzifier(true);
	}

	@Override
	public String toString() {
		final StringBuilder value = new StringBuilder();
		return value.append(this.getFlvName()).append(" (").append(this.getScore()).append(")").toString();
	}

	/**
	 * Name of the associated Function Block in the FCL file.
	 */
	protected abstract String getFclName();

	/**
	 * Name of this FLV object.
	 */
	protected abstract String getFlvName();
}
