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
import java.util.Map;

import net.sourceforge.jFuzzyLogic.FIS;
import net.sourceforge.jFuzzyLogic.FunctionBlock;
import net.sourceforge.jFuzzyLogic.rule.Rule;
import net.sourceforge.jFuzzyLogic.rule.Variable;

import org.apache.commons.lang.math.DoubleRange;
import org.apache.log4j.Logger;

import com.tesora.dve.common.MathUtils;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.exceptions.PECodingException;
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

	protected interface FlvName {
		public String get();
	}

	private enum Variables implements FlvName {
		SCORE_FLV_NAME {
			@Override
			public String get() {
				return "desirability";
			}
		};
	}

	private static final String RULE_NAME_PREFIX_SEPARATOR = "_";
	private static final String FCL_SCHEMA_FILE_NAME = "DistributionModels.fcl";
	private static final DoubleRange VALID_RULE_WEIGHT_RANGE = new DoubleRange(0, 1);
	private static final double DEFAULT_RULE_WEIGHT = 1.0;

	private final FunctionBlock ai;

	public static double toPercent(final double value, final double total) {
		return 100.0 * (value / total);
	}

	public static Comparator<FuzzyLinguisticVariable> getScoreComparator() {
		return MODEL_SCORE_COMPARATOR;
	}

	public static List<FuzzyTableDistributionModel> evaluateDistributionModels(final FuzzyTableDistributionModel... distributionModels) {
		return evaluateDistributionModels(Collections.EMPTY_MAP, distributionModels);
	}

	public static List<FuzzyTableDistributionModel> evaluateDistributionModels(final Map<FlvName, Double> ruleWeights,
			final FuzzyTableDistributionModel... distributionModels) {
		final List<FuzzyTableDistributionModel> sortedDistributionModels = Arrays.asList(distributionModels);
		for (final FuzzyLinguisticVariable distributionModel : sortedDistributionModels) {
			distributionModel.setRuleWeights(ruleWeights);
			distributionModel.evaluate();
		}

		Collections.sort(sortedDistributionModels, getScoreComparator());

		return sortedDistributionModels;
	}

	protected FuzzyLinguisticVariable(final String fclBlockName) throws PECodingException {
		try (final InputStream fclSchema = PEFileUtils.getResourceStream(FuzzyLinguisticVariable.class, FCL_SCHEMA_FILE_NAME)) {
			this.ai = FIS.load(fclSchema, false).getFunctionBlock(fclBlockName);
			if (this.ai == null) {
				throw new PECodingException("Could not load the Fuzzy Control Language (FCL) specification from '" + FCL_SCHEMA_FILE_NAME + "'");
			}
		} catch (final IOException | PEException e) {
			logger.error(e.getMessage(), e);
			throw new PECodingException(e);
		}
	}

	protected void setVariable(final FlvName name, final double value) {
		ai.setVariable(name.get(), value);
	}

	protected void setVariables(final Map<FlvName, Double> variables) {
		if (variables != null) {
			for (final Map.Entry<FlvName, Double> var : variables.entrySet()) {
				this.setVariable(var.getKey(), var.getValue());
			}
		}
	}

	protected void setRuleWeights(final Map<FlvName, Double> weights) {
		if (weights != null) {
			for (final Map.Entry<FlvName, Double> weight : weights.entrySet()) {
				this.setWeightOnRule(weight.getKey(), weight.getValue());
			}
		}
	}

	/**
	 * Set a given weight on all rules named 'prefix_*'.
	 * 
	 * @param ruleWeight
	 *            A number from interval [0.0, 1.0] or 1.0 if null.
	 */
	protected void setWeightOnRule(final FlvName ruleNamePrefix, final Double ruleWeight) {
		final double weight = (ruleWeight != null) ? ruleWeight : DEFAULT_RULE_WEIGHT;
		if (!VALID_RULE_WEIGHT_RANGE.containsDouble(weight)) {
			throw new PECodingException("Weight (" + weight + ") for rule(s) '" + ruleNamePrefix + "*' is out of range.");
		}

		for (final Rule rule : this.ai.getFuzzyRuleBlock(null).getRules()) {
			if (rule.getName().startsWith(ruleNamePrefix.get().concat(RULE_NAME_PREFIX_SEPARATOR))) {
				rule.setWeight(weight);
			}
		}
	}

	public void evaluate() {
		ai.evaluate();
	}

	public double getScore() {
		return this.getVariable(Variables.SCORE_FLV_NAME).getLatestDefuzzifiedValue();
	}

	public double getVariableValue(final FlvName name) {
		return this.getVariable(name).getValue();
	}

	private Variable getVariable(final FlvName name) {
		return this.ai.getVariable(name.get());
	}

	@Override
	public String toString() {
		final StringBuilder value = new StringBuilder();
		return value.append(this.getFlvName()).append(" (").append(MathUtils.round(this.getScore(), AiTemplateBuilder.NUMBER_DISPLAY_PRECISION)).append(")")
				.toString();
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
