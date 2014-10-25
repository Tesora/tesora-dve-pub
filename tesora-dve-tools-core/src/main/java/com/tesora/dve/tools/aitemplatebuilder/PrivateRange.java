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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.SingletonMap;

import com.tesora.dve.common.HashSetFactory;
import com.tesora.dve.common.LinkedHashMapFactory;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.tools.CLIBuilder.ColorStringBuilder;
import com.tesora.dve.tools.aitemplatebuilder.AiTemplateBuilder.MessageSeverity;
import com.tesora.dve.tools.aitemplatebuilder.AiTemplateBuilder.ScoreBonus;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.JoinStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public class PrivateRange implements TemplateRangeItem {

	public interface ColumnRanker<T> extends Comparator<Entry<T, Long>> {
		public boolean isSafeMode();

		public Set<TableColumn> findRangeColumns(final Map<T, Long> columnStats);
	}

	private static final class SingleColumnRanker implements ColumnRanker<TableColumn> {

		private final Set<TemplateRangeItem> otherAvailableRanges;
		private final boolean isSafeMode;

		public SingleColumnRanker(final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
			this.otherAvailableRanges = otherAvailableRanges;
			this.isSafeMode = isSafeMode;
		}

		@Override
		public int compare(Entry<TableColumn, Long> a, Entry<TableColumn, Long> b) {
			final Double aFreq = getBonusFrequency(a, isSafeMode);
			final Double bFreq = getBonusFrequency(b, isSafeMode);
			final int frequencyDifference = aFreq.compareTo(bFreq); // Compare on frequencies.

			/* Break the tie. */
			if (frequencyDifference == 0) {
				final TableColumn aColumn = a.getKey();
				final TableColumn bColumn = b.getKey();

				/*
				 * Search the existing ranges for columns of the same name
				 * and type and return the most frequent one.
				 */
				int aCount = 0;
				int bCount = 0;
				for (final TemplateRangeItem range : this.otherAvailableRanges) {
					if (range.hasCommonColumn(toSingletonSet(aColumn))) {
						++aCount;
					}
					if (range.hasCommonColumn(toSingletonSet(bColumn))) {
						++bCount;
					}
				}

				final int occurancyDifference = aCount - bCount;
				if (occurancyDifference != 0) {
					return occurancyDifference;
				}

				/*
				 * Use size for compatible types. Return the column with larger
				 * type.
				 */
				final Type aType = aColumn.getType();
				final Type bType = bColumn.getType();
				if ((aType.isStringType() && bType.isStringType())
						|| (aType.isNumericType() && bType.isNumericType())) {

					final int sizeDifference = aType.getSize() - bType.getSize();
					if (sizeDifference != 0) {
						return sizeDifference;
					}
				}

				/* Enough! Get the first in the alphabet. */
				final String aName = aColumn.getName().getUnquotedName().get();
				final String bName = bColumn.getName().getUnquotedName().get();
				return bName.compareTo(aName);
			}

			return frequencyDifference;
		}

		@Override
		public boolean isSafeMode() {
			return this.isSafeMode;
		}

		@Override
		public Set<TableColumn> findRangeColumns(Map<TableColumn, Long> columnStats) {
			return (!columnStats.isEmpty()) ? toSingletonSet(Collections.max(columnStats.entrySet(), this).getKey()) : Collections.EMPTY_SET;
		}
	}

	private static final class ColumnVectorRanker implements ColumnRanker<Set<TableColumn>> {

		private final Set<TemplateRangeItem> otherAvailableRanges;
		private final boolean isSafeMode;

		public ColumnVectorRanker(final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
			this.otherAvailableRanges = otherAvailableRanges;
			this.isSafeMode = isSafeMode;
		}

		@Override
		public int compare(Entry<Set<TableColumn>, Long> a, Entry<Set<TableColumn>, Long> b) {
			final Double aFreq = getBonusFrequencyForColumnVector(a, isSafeMode);
			final Double bFreq = getBonusFrequencyForColumnVector(b, isSafeMode);
			final int frequencyDifference = aFreq.compareTo(bFreq);

			if (frequencyDifference == 0) {
				final Set<TableColumn> aColumns = a.getKey();
				final Set<TableColumn> bColumns = b.getKey();

				int aCount = 0;
				int bCount = 0;
				for (final TemplateRangeItem range : this.otherAvailableRanges) {
					if (range.hasCommonColumn(aColumns)) {
						++aCount;
					}
					if (range.hasCommonColumn(bColumns)) {
						++bCount;
					}
				}

				final int occurancyDifference = aCount - bCount;
				if (occurancyDifference != 0) {
					return occurancyDifference;
				}

				/*
				 * Use size for compatible types. Return the column vector with
				 * larger
				 * average type size.
				 */
				final double aAvgTypeSize = computeAverageTypeSizeForColumnVector(aColumns);
				final double bAvgTypeSize = computeAverageTypeSizeForColumnVector(bColumns);
				if (aAvgTypeSize > bAvgTypeSize) {
					return 1;
				} else if (aAvgTypeSize < bAvgTypeSize) {
					return -1;
				}

				/*
				 * Use the wider of the two column vectors.
				 */
				final int aSize = aColumns.size();
				final int bSize = bColumns.size();
				final int widthDifference = aSize - bSize;
				if (widthDifference != 0) {
					return widthDifference;
				}

				/* Enough! Compare them on something distinct and stable. */
				final Integer aHash = aColumns.hashCode();
				final Integer bHash = bColumns.hashCode();
				return aHash.compareTo(bHash);
			}

			return frequencyDifference;
		}

		@Override
		public boolean isSafeMode() {
			return this.isSafeMode;
		}

		@Override
		public Set<TableColumn> findRangeColumns(Map<Set<TableColumn>, Long> columnStats) {
			return (!columnStats.isEmpty()) ? Collections.max(columnStats.entrySet(), this).getKey() : Collections.EMPTY_SET;
		}
	}

	private static Set<TableColumn> toSingletonSet(final TableColumn column) {
		return Collections.<TableColumn> singleton(column);
	}

	private final ColumnRanker<?> columnRanker;
	private SingletonMap table;

	public static PrivateRange fromAllColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
		final Map<TableColumn, Long> tableColumns = new HashMap<TableColumn, Long>();
		for (final TableColumn column : table.getTableColumns()) {
			tableColumns.put(column, 1l);
		}
		return getValidRange(buildRangeFor(table, tableColumns, new SingleColumnRanker(otherAvailableRanges, isSafeMode)));
	}

	public static PrivateRange fromOuterJoinColumns(final TableStats table, final Set<JoinStats> joins, final Set<TemplateRangeItem> otherAvailableRanges,
			final boolean isSafeMode) {
		final Map<Set<TableColumn>, Long> outerJoinColumns = new HashMap<Set<TableColumn>, Long>();
		for (final JoinStats join : joins) {
			if (join.involves(table) && AiTemplateBuilder.isJoinToBroadcastAndRequiresRedist(join)) {
				final Set<TableColumn> joinColumns = (join.getType().isLeftOuterJoin()) ? join.getRightColumns() : join.getLeftColumns();

				Long count = outerJoinColumns.get(joinColumns);
				if (count == null) {
					count = new Long(join.getFrequency());
				} else {
					count += join.getFrequency();
				}
				outerJoinColumns.put(joinColumns, count);
			}
		}

		return getValidRange(buildRangeFor(table, outerJoinColumns, new ColumnVectorRanker(otherAvailableRanges, isSafeMode)));
	}

	public static PrivateRange fromWhereColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode,
			final boolean useIdentTuples) {
		return getValidRange(buildRangeFor(table, getIdentTuplesFromTable(table, useIdentTuples), new ColumnVectorRanker(otherAvailableRanges, isSafeMode)));
	}

	private static Map<Set<TableColumn>, Long> getIdentTuplesFromTable(final TableStats table, final boolean useIdentTuples) {
		if (useIdentTuples) {
			return table.getIdentColumns();
		}

		final Map<Set<TableColumn>, Long> singletons = new HashMap<Set<TableColumn>, Long>();
		for (final Map.Entry<Set<TableColumn>, Long> tupleEntry : table.getIdentColumns().entrySet()) {
			final Long tupleFrequency = tupleEntry.getValue();
			for (final TableColumn tupleColumn : tupleEntry.getKey()) {
				singletons.put(Collections.singleton(tupleColumn), tupleFrequency);
			}
		}

		return singletons;
	}

	public static PrivateRange fromGroupByColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
		return getValidRange(buildRangeFor(table, table.getGroupByColumns(), new ColumnVectorRanker(otherAvailableRanges, isSafeMode)));
	}

	private static PrivateRange buildRangeFor(final TableStats table, final Map<TableColumn, Long> columnStats, final SingleColumnRanker ranker) {
		final PrivateRange range = new PrivateRange(ranker);
		range.addTable(table, ranker.findRangeColumns(columnStats));
		return range;
	}

	private static PrivateRange buildRangeFor(final TableStats table, final Map<Set<TableColumn>, Long> columnStats, final ColumnVectorRanker ranker) {
		final PrivateRange range = new PrivateRange(ranker);
		range.addTable(table, ranker.findRangeColumns(columnStats));
		return range;
	}

	private static PrivateRange getValidRange(final PrivateRange range) {
		final Set<TableColumn> rangeColumns = range.getRangeColumnsFor(range.getTable());

		if (!rangeColumns.isEmpty()) {
			if ((!range.isSafeMode() || (range.isSafeMode() && AiTemplateBuilder.hasAutoIncrement(rangeColumns)))
					&& AiTemplateBuilder.hasRangeCompatible(rangeColumns)) {
				return range;
			}
		}

		return null;
	}

	/**
	 * Get bonus frequency for a given column.
	 */
	private static Double getBonusFrequency(final Entry<TableColumn, Long> columnStats, final boolean isSafeMode) {
		final float bonus = computeBonusFactor(Collections.singleton(columnStats.getKey()), isSafeMode);
		final long frequency = columnStats.getValue();
		return new Double(bonus * frequency);
	}

	private static Double getBonusFrequencyForColumnVector(final Entry<Set<TableColumn>, Long> columnStats, final boolean isSafeMode) {
		final float bonus = computeBonusFactor(columnStats.getKey(), isSafeMode);
		final long frequency = columnStats.getValue();
		return new Double(bonus * frequency);
	}

	/**
	 * The net bonus factor is the sum of all bonuses over all range columns.
	 */
	private static float computeBonusFactor(final Set<TableColumn> columns, final boolean isSafeMode) {
		float bonusFactor = 1.0f;
		for (final TableColumn column : columns) {
			final Set<ScoreBonus> columnBonuses = AiTemplateBuilder.getBonusesForColumn(column, isSafeMode);
			for (final ScoreBonus bonus : columnBonuses) {
				bonusFactor += bonus.getBonusFactor();
			}
		}

		return bonusFactor;
	}

	private static double computeAverageTypeSizeForColumnVector(final Set<TableColumn> columns) {
		double sum = 0.0;
		for (final TableColumn column : columns) {
			final Type type = column.getType();
			if (type.isStringType() || type.isNumericType()) {
				sum += type.getSize();
			}
		}

		return (sum / columns.size());
	}

	private PrivateRange(final ColumnRanker<?> ranker) {
		this.columnRanker = ranker;
	}

	public boolean isSafeMode() {
		return this.columnRanker.isSafeMode();
	}

	public TableStats getTable() {
		return (TableStats) this.table.firstKey();
	}

	public Set<TableColumn> getColumns() {
		return ((MultiMap<TableColumn, ScoreBonus>) this.table.getValue()).keySet();
	}

	@Override
	public boolean contains(final TableStats table) {
		return this.getTable().equals(table);
	}

	/*
	 * Follow this filter order:
	 * 1. WHERE
	 * 2. GROUP BY
	 */
	@Override
	public Set<TableColumn> getRangeColumnsFor(final TableStats table) {
		if (this.contains(table)) {
			return getColumns();
		}
		return null;
	}

	@Override
	public Set<Type> getUniqueColumnTypes() {
		return CommonRange.getUniqueColumnTypes(getRangeColumnsFor(this.getTable()));
	}

	@Override
	public boolean hasCommonColumn(final Set<TableColumn> columns) {
		return CommonRange.haveCommonColumn(columns, this.getColumns());
	}

	@Override
	public String getTemplateItemName() {
		return CommonRange.buildUniqueRangeNameForTable(this.getTable());
	}

	@Override
	public String toString() {
		final ColorStringBuilder value = new ColorStringBuilder();
		value.append(getTemplateItemName()).append(": {");

		/* Print the range table. */
		value.append(AiTemplateBuilder.LINE_SEPARATOR).append(AiTemplateBuilder.LINE_INDENT).append(this.getTable());

		value.append(AiTemplateBuilder.LINE_SEPARATOR);

		/* Print collected range bonuses. */
		final MultiMap<TableColumn, ScoreBonus> tableColumnBonuses = (MultiMap<TableColumn, ScoreBonus>) this.table.get(this.getTable());
		for (final TableColumn column : tableColumnBonuses.keySet()) {
			for (final ScoreBonus bonus : tableColumnBonuses.get(column)) {
				value.append(AiTemplateBuilder.LINE_SEPARATOR).append(AiTemplateBuilder.LINE_INDENT)
						.append(bonus.print(column), (bonus.isNegative()) ? MessageSeverity.WARNING.getColor() : MessageSeverity.INFO.getColor());
			}
		}

		return value.append(AiTemplateBuilder.LINE_SEPARATOR).append("}").toString();
	}

	private void addTable(final TableStats table, final Set<TableColumn> dv) {
		final MultiMap<TableColumn, ScoreBonus> columnMap = new MultiMap<TableColumn, ScoreBonus>(
				new LinkedHashMapFactory<TableColumn, Collection<ScoreBonus>>(),
				new HashSetFactory<ScoreBonus>()
				);

		for (final TableColumn column : dv) {
			columnMap.putAll(column, AiTemplateBuilder.getBonusesForColumn(column, this.isSafeMode()));
		}
		this.table = new SingletonMap(table, columnMap);
	}
}
