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

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.ImmutableMap;
import com.tesora.dve.common.MathUtils;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.StatementType;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;

public abstract class FuzzyTableDistributionModel extends FuzzyLinguisticVariable implements TemplateModelItem {
	
	public enum Variables implements FlvName {
		OPERATIONS_FLV_NAME {
			@Override
			public String get() {
				return "operations";
			}
		},
		SORTS_FLV_NAME {
			@Override
			public String get() {
				return "sorts";
			}
		},
		WRITES_FLV_NAME {
			@Override
			public String get() {
				return "writes";
			}
		},
		CARDINALITY_FLV_NAME {
			@Override
			public String get() {
				return "cardinality";
			}
		};
	}

	protected FuzzyTableDistributionModel(final String fclBlockName) {
		super(fclBlockName);
	}

	protected FuzzyTableDistributionModel(final String fclBlockName,
			final TableStats match,
			final Set<Long> uniqueOperationFrequencies,
			final SortedSet<Long> sortedCardinalities,
			final boolean isRowWidthWeightingEnabled) {
		super(fclBlockName);

		final long totalOrderBy = match.getStatementCounts(StatementType.ORDERBY);
		final long totalOperations = MathUtils.getOneAtLeast(match.getTotalStatementCount());
		final double pcOrderBy = FuzzyLinguisticVariable.toPercent(totalOrderBy, totalOperations);

		final double averageNumberOfOperations = MathUtils.getOneAtLeast(MathUtils.mean(uniqueOperationFrequencies));
		final double pcOperations = FuzzyLinguisticVariable.toPercent(match.getTotalStatementCount(), averageNumberOfOperations);

		final long cardinality = match.getPredictedFutureSize(isRowWidthWeightingEnabled);
		final double pcCardinality = FuzzyLinguisticVariable.toPercent(
				CommonRange.findPositionFor(cardinality, sortedCardinalities), sortedCardinalities.size());

		initializeVariables(pcOperations, pcOrderBy, match.getWritePercentage(), pcCardinality);
	}

	protected FuzzyTableDistributionModel(final String fclBlockName,
			final double pcOperations,
			final double pcOrderBy, final double pcWrites, final double pcCardinality) {
		super(fclBlockName);
		initializeVariables(pcOperations, pcOrderBy, pcWrites, pcCardinality);
	}

	protected FuzzyTableDistributionModel(final String fclBlockName, final Map<FlvName, Double> variables) {
		super(fclBlockName);
		this.setVariables(variables);
	}

	private void initializeVariables(final double pcOperations, final double pcOrderBy, final double pcWrites, final double pcCardinality) {
		setVariables(this.buildVariableMap(pcOperations, pcOrderBy, pcWrites, pcCardinality));
	}

	public Map<FlvName, Double> getVariables() {
		return this.buildVariableMap(
				this.getVariableValue(Variables.OPERATIONS_FLV_NAME),
				this.getVariableValue(Variables.SORTS_FLV_NAME),
				this.getVariableValue(Variables.WRITES_FLV_NAME),
				this.getVariableValue(Variables.CARDINALITY_FLV_NAME));
	}

	private Map<FlvName, Double> buildVariableMap(final double pcOperations, final double pcOrderBy, final double pcWrites, final double pcCardinality) {
		return ImmutableMap.<FlvName, Double> of(
				Variables.OPERATIONS_FLV_NAME, pcOperations,
				Variables.SORTS_FLV_NAME, pcOrderBy,
				Variables.WRITES_FLV_NAME, pcWrites,
				Variables.CARDINALITY_FLV_NAME, pcCardinality
				);
	}

	@Override
	public abstract String getTemplateItemName();

	@Override
	protected abstract String getFclName();

}
