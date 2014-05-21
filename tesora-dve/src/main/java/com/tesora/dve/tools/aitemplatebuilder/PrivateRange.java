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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.hazelcast.util.AbstractMap.SimpleImmutableEntry;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.JoinStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public class PrivateRange implements TemplateRangeItem {

	private static final class ColumnRanker implements Comparator<Map.Entry<TableColumn, Long>> {

		private final Set<TemplateRangeItem> otherAvailableRanges;
		private final boolean isSafeMode;

		public ColumnRanker(final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
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
	}

	private static final class ColumnVectorRanker implements Comparator<Map.Entry<Set<TableColumn>, Long>> {

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
	}

	private static Set<TableColumn> toSingletonSet(final TableColumn column) {
		return Collections.<TableColumn> singleton(column);
	}

	private final Comparator<?> columnRanker;
	private final Map.Entry<TableStats, Set<TableColumn>> table;
	private final boolean isSafeMode;

	public static PrivateRange fromAllColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
		final Map<TableColumn, Long> tableColumns = new HashMap<TableColumn, Long>();
		for (final TableColumn column : table.getTableColumns()) {
			tableColumns.put(column, 1l);
		}
		return getValidRange(new PrivateRange(table, tableColumns, otherAvailableRanges, isSafeMode));
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

		return getValidRange(new PrivateRange(table, otherAvailableRanges, outerJoinColumns, isSafeMode));
	}

	public static PrivateRange fromWhereColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
		return getValidRange(new PrivateRange(table, table.getIdentColumns(), otherAvailableRanges, isSafeMode));
	}

	public static PrivateRange fromGroupByColumns(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final boolean isSafeMode) {
		return getValidRange(new PrivateRange(table, table.getGroupByColumns(), otherAvailableRanges, isSafeMode));
	}

	private static PrivateRange getValidRange(final PrivateRange range) {
		final Set<TableColumn> rangeColumns = range.getRangeColumnsFor(range.getTable());

		if (rangeColumns != null) {
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
		final float bonus = AiTemplateBuilder.getBonusForColumn(columnStats.getKey(), isSafeMode);
		final long frequency = columnStats.getValue();
		return new Double(bonus * frequency);
	}

	private static Double getBonusFrequencyForColumnVector(final Entry<Set<TableColumn>, Long> columnStats, final boolean isSafeMode) {
		final float bonus = AiTemplateBuilder.getBonusForColumns(columnStats.getKey(), isSafeMode);
		final long frequency = columnStats.getValue();
		return new Double(bonus * frequency);
	}

	private PrivateRange(final TableStats table, final Map<TableColumn, Long> columnStats, final Set<TemplateRangeItem> otherAvailableRanges,
			final boolean isSafeMode) {
		this.columnRanker = new ColumnRanker(otherAvailableRanges, isSafeMode);
		this.table = new SimpleImmutableEntry<TableStats, Set<TableColumn>>(table, findRangeColumns(columnStats));
		this.isSafeMode = isSafeMode;
	}

	private PrivateRange(final TableStats table, final Set<TemplateRangeItem> otherAvailableRanges, final Map<Set<TableColumn>, Long> outerJoinColumns,
			final boolean isSafeMode) {
		this.columnRanker = new ColumnVectorRanker(otherAvailableRanges, isSafeMode);
		this.table = new SimpleImmutableEntry<TableStats, Set<TableColumn>>(table, findRangeColumnVector(outerJoinColumns));
		this.isSafeMode = isSafeMode;
	}

	public boolean isSafeMode() {
		return this.isSafeMode;
	}

	public TableStats getTable() {
		return this.table.getKey();
	}

	public Set<TableColumn> getColumns() {
		return this.table.getValue();
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
		return "PrivateRange";
	}

	private Set<TableColumn> findRangeColumns(final Map<TableColumn, Long> columnStats) {
		return (!columnStats.isEmpty()) ? toSingletonSet(Collections.max(columnStats.entrySet(), (ColumnRanker) this.columnRanker).getKey()) : null;
	}

	private Set<TableColumn> findRangeColumnVector(final Map<Set<TableColumn>, Long> columnStats) {
		return (!columnStats.isEmpty()) ? Collections.max(columnStats.entrySet(), (ColumnVectorRanker) this.columnRanker).getKey() : null;
	}
}
