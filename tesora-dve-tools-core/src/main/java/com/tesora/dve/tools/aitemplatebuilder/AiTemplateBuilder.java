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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.tesora.dve.common.MathUtils;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.template.TemplateBuilder;
import com.tesora.dve.sql.template.jaxb.ModelType;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.tools.CLIBuilder;
import com.tesora.dve.tools.CLIBuilder.ColorStringBuilder;
import com.tesora.dve.tools.CLIBuilder.ConsoleColor;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.JoinStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.Relationship;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.Relationship.RelationshipSpecification;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.StatementType;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableSizeComparator;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.ForeignRelationship;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats.TableColumn;

public final class AiTemplateBuilder {

	private static final Set<TemplateModelItem> AVAILABLE_MODELS = ImmutableSet.<TemplateModelItem> of(
			Broadcast.SINGLETON_TEMPLATE_ITEM,
			Random.SINGLETON_TEMPLATE_ITEM,
			Range.SINGLETON_TEMPLATE_ITEM);

	public static TemplateModelItem getModelForName(final String name) throws PEException {
		for (final TemplateModelItem model : AVAILABLE_MODELS) {
			if (model.getTemplateItemName().equalsIgnoreCase(name)) {
				return model;
			}
		}

		throw new PEException("Invalid distribution model '" + String.valueOf(name) + "' specified");
	}

	public static enum MessageSeverity {

		INFO(ConsoleColor.DEFAULT, Level.INFO),
		ALERT(ConsoleColor.BLUE, Level.INFO),
		WARNING(ConsoleColor.YELLOW, Level.WARN),
		SEVERE(ConsoleColor.RED, Level.ERROR);

		private final ConsoleColor textColor;
		private final Level log4jLevel;

		private MessageSeverity(final ConsoleColor textColor, final Level log4jLevel) {
			this.textColor = textColor;
			this.log4jLevel = log4jLevel;
		}

		public ConsoleColor getColor() {
			return this.textColor;
		}

		public Level getLogLevel() {
			return this.log4jLevel;
		}
	}

	public static enum ScoreBonus {

		AI_RANGE_COLUMN("Range backed by an AI column", 0.15f, true),
		INTEGRAL_RANGE_COLUMN("Range backed by an integral column", 0.05f, true),
		UPDATED_RANGE_COLUMN("Update on the distribution vector", -0.10f, false),
		UNSAFE_RANGE_COLUMN("Column not safe for ranging on", -1.0f, true);

		private final String description;
		private final float bonus;
		private final boolean perRangeOnly;

		private ScoreBonus(final String description, final float bonus, final boolean perRangeOnly) {
			this.description = description;
			this.bonus = bonus;
			this.perRangeOnly = perRangeOnly;
		}

		public String print(final TableColumn column) {
			return this.description.concat(": ").concat(column.getQualifiedName());
		}

		public float getBonusFactor() {
			return this.bonus;
		}

		/**
		 * Some bonuses are per whole range only.
		 * 
		 * The per-range bonuses generally get applied on the range score only
		 * once (e.g. there can be several AI columns involved in a single
		 * range, but the bonus gets applied only once).
		 * 
		 * Others apply to individual relationships/joins within a range.
		 */
		public boolean isPerRangeOnly() {
			return this.perRangeOnly;
		}

		/**
		 * A negative bonus is a penalty.
		 */
		public boolean isNegative() {
			return this.bonus < 0.0;
		}

		@Override
		public String toString() {
			return this.description;
		}
	}

	private static enum RedistCause {

		ORDER_BY("Sort on a non-broadcast table") {
			@Override
			protected String causingObjectToString(final Object object) {
				return "[F:" + String.valueOf(object) + "x]";
			}
		},
		UNIQUE_UPDATE("Update on a unique key") {
			@SuppressWarnings("unchecked")
			@Override
			protected String causingObjectToString(final Object object) {
				final Map.Entry<TableColumn, Long> entry = (Map.Entry<TableColumn, Long>) object;
				return "[" + String.valueOf(entry.getKey()) + ": U:" + String.valueOf(entry.getValue()) + "x]";
			}
		},
		DV_UPDATE("Update on the distribution vector") {
			@SuppressWarnings("unchecked")
			@Override
			protected String causingObjectToString(final Object object) {
				final Map.Entry<TableColumn, Long> entry = (Map.Entry<TableColumn, Long>) object;
				return "[" + String.valueOf(entry.getKey()) + ": U:" + String.valueOf(entry.getValue()) + "x]";
			}
		},
		NON_COLLOCATED_JOIN("Non-collocated join") {
			@Override
			protected String causingObjectToString(final Object object) {
				return String.valueOf(object);
			}
		};

		protected final String description;

		private RedistCause(final String description) {
			this.description = description;
		}

		public String print(final Object object) {
			return this.description.concat(": ").concat(causingObjectToString(object));
		}

		protected abstract String causingObjectToString(final Object cause);

		@Override
		public String toString() {
			return this.description;
		}
	}

	public static final String LINE_SEPARATOR = System.getProperty("line.separator");
	public static final String LINE_INDENT = "\t";
	public static final int NUMBER_DISPLAY_PRECISION = 2;

	private static final Logger logger = Logger.getLogger(AiTemplateBuilder.class);
	private static final double LOWER_CORPUS_COVERAGE_THRESHOLD_PC = 40.0;
	private static final double UPPER_CORPUS_COVERAGE_THRESHOLD_PC = 60.0;
	private static final double MOSTLY_WRITTEN_THRESHOLD_PC = 80.0;
	private static int TABLE_NAME_MIN_PREFIX_LENGTH = 4;
	private static String TABLE_NAME_WILDCARD = ".*";

	private static final class Ranges {

		private static final class RangeSizeSorter implements Comparator<CommonRange> {
			@Override
			public int compare(CommonRange a, CommonRange b) {
				return a.getSize() - b.getSize();
			}
		}

		private static final RangeSizeSorter RANGE_SIZE_SORTER = new RangeSizeSorter();

		private final List<CommonRange> ranges = new ArrayList<CommonRange>();
		private final boolean isSafeMode;
		private final boolean isRowWidthWeightingEnabled;

		final List<Long> frequencies = new ArrayList<Long>();
		final List<Long> cardinalities = new ArrayList<Long>();

		public Ranges(final boolean isSafeMode, final boolean isRowWidthWeightingEnabled) {
			this.isSafeMode = isSafeMode;
			this.isRowWidthWeightingEnabled = isRowWidthWeightingEnabled;
		}

		public void add(final CommonRange range) {
			this.ranges.add(range);
		}

		public boolean isEmpty() {
			return this.frequencies.isEmpty();
		}

		public List<Long> getPredictedFutureJoinCardinalities() {
			return Collections.unmodifiableList(this.cardinalities);
		}

		public List<Long> getJoinFrequencies() {
			return Collections.unmodifiableList(this.frequencies);
		}

		public void generateAllPossibleCombinations() {
			flattenJoins();
			diversify();
		}

		/**
		 * OUTER JOINs can be merged with compatible INNER JOINs provided that
		 * they are not joins to Broadcast that trigger redistribution - those
		 * have to be handled separately.
		 */
		private void flattenJoins() {
			for (final CommonRange range : this.ranges) {
				range.flattenJoins();
			}
		}

		/**
		 * For all overlapping ranges add their versions without the overlaps.
		 */
		private void diversify() {
			final List<CommonRange> diversifiedRanges = new ArrayList<CommonRange>();
			for (final CommonRange range : this.ranges) {
				for (final CommonRange overlappingRange : getOverlapping(this.ranges, range)) {
					if (overlappingRange != range) {
						if (range.getSize() > 1) {
							final CommonRange copy = new CommonRange(range);
							for (final TableStats table : overlappingRange.getTables()) {
								copy.remove(table);
							}
							if (copy.getSize() > 0) {
								diversifiedRanges.add(copy);
							}
						}
					}
				}
			}

			this.ranges.addAll(diversifiedRanges);
		}

		private void addJoin(final Relationship join) throws PEException {
			if (join.isRangeCompatible()) {
				this.frequencies.add(join.getFrequency());
				this.cardinalities.add(join.getPredictedFutureSize(this.isRowWidthWeightingEnabled));

				final List<CommonRange> affectedRanges = addJoinToSuitableRanges(join);
				if (affectedRanges.isEmpty()) {
					final CommonRange range = new CommonRange(join, this.isSafeMode);

					/*
					 * Add suitable joins from other ranges.
					 */
					for (final CommonRange other : this.ranges) {
						other.mergeInto(range);
					}

					this.ranges.add(range);

				} else {

					/*
					 * Two-way merge all affected ranges with their overlaps.
					 */
					final Set<CommonRange> uniqueMergedRanges = new HashSet<CommonRange>();
					for (final CommonRange range : affectedRanges) {
						uniqueMergedRanges.addAll(doTwoWayMergeOnOverlappingRanges(range));
					}

					/*
					 * Check for and remove completely overlapping ranges.
					 */
					this.ranges.removeAll(findAllSubranges(new ArrayList<CommonRange>(uniqueMergedRanges)));
				}
			}
		}

		private List<CommonRange> doTwoWayMergeOnOverlappingRanges(final CommonRange range) throws PEException {
			final List<CommonRange> overlappingRanges = getOverlapping(this.ranges, range);
			for (final CommonRange overlappingRange : overlappingRanges) {
				if (overlappingRange != range) {
					range.mergeInto(overlappingRange);
					overlappingRange.mergeInto(range);
				}
			}

			return overlappingRanges;
		}

		private Set<CommonRange> findAllSubranges(final List<CommonRange> ranges) {
			final int numRanges = ranges.size();
			final Set<CommonRange> subranges = new HashSet<CommonRange>(numRanges);
			for (int i = 0; i < (numRanges - 1); ++i) {
				/* Keep the merged ranges sorted by size. */
				Collections.sort(ranges, RANGE_SIZE_SORTER);
				final CommonRange range = ranges.get(i);
				for (int j = i + 1; j < numRanges; ++j) {
					final CommonRange next = ranges.get(j);
					if (range.isSubrangeOf(next)) {
						subranges.add(range);
						break;
					}
				}
			}

			return subranges;
		}

		private List<CommonRange> addJoinToSuitableRanges(final Relationship join) throws PEException {
			final List<CommonRange> affectedRanges = new ArrayList<CommonRange>();
			for (final CommonRange range : this.ranges) {
				if (range.isSuitableFor(join)) {
					range.addJoin(join);
					affectedRanges.add(range);
				}
			}
			return affectedRanges;
		}

		private void evaluate(final Set<Long> uniqueJoinFrequency, final SortedSet<Long> sortedJoinCardinalities) {
			for (final CommonRange range : this.ranges) {
				range.evaluate(uniqueJoinFrequency, sortedJoinCardinalities, this.isRowWidthWeightingEnabled);
			}
		}

		private List<CommonRange> getSortedCommonRanges() {
			final List<CommonRange> sortedRanges = new ArrayList<CommonRange>(this.ranges);
			Collections.sort(sortedRanges, FuzzyLinguisticVariable.getScoreComparator());
			return sortedRanges;
		}

		private static List<CommonRange> getOverlapping(final List<CommonRange> ranges, final CommonRange range) {
			final List<CommonRange> overlappingRanges = new ArrayList<CommonRange>();
			for (final CommonRange item : ranges) {
				if (item.hasIntersectionWith(range)) {
					overlappingRanges.add(item);
				}
			}

			return overlappingRanges;
		}

	}

	public static List<Template> buildAllBroadcastTemplates(final List<String> databases) {
		return buildSingleModelTemplates(databases, Broadcast.SINGLETON_TEMPLATE_ITEM);
	}

	public static List<Template> buildAllRandomTemplates(final List<String> databases) {
		return buildSingleModelTemplates(databases, Random.SINGLETON_TEMPLATE_ITEM);
	}

	private static List<Template> buildSingleModelTemplates(final List<String> databases, final TemplateItem distributionModel) {
		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(buildSingleModelTemplate(database, distributionModel));
		}

		return templates;
	}

	private static Template buildSingleModelTemplate(final String databaseName, final TemplateItem distributionModel) {
		final TemplateBuilder builder = new TemplateBuilder(databaseName);
		builder.withTable(TABLE_NAME_WILDCARD, distributionModel.getTemplateItemName());

		return builder.toTemplate();
	}

	private static String getWelcomeMessage(final List<String> databases, final Long broadcastCardinalityCutoff, final boolean followForeignKeys,
			final boolean isSafeMode) {
		final StringBuilder welcomeMessage = new StringBuilder();
		welcomeMessage.append("Generating templates for '" + StringUtils.join(databases, "', '") + "'.");
		welcomeMessage.append(" Broadcast cardinality cutoff: ").append((broadcastCardinalityCutoff != null) ? broadcastCardinalityCutoff : "automatic");
		welcomeMessage.append(" Following FKs: ").append(followForeignKeys);
		welcomeMessage.append(" Safe mode: ").append(isSafeMode);
		welcomeMessage.append("...");

		return welcomeMessage.toString();
	}

	private static String getTableNameAndDistributionModel(final TableStats table) {
		final StringBuilder value = new StringBuilder();
		value.append(table).append(": ").append(table.getTableDistributionModel());
		if (table.hasDistributionModelFreezed()) {
			value.append(" (").append("user defined").append(")");
		}
		
		return value.toString();
	}

	/**
	 * Get bonus % for a given column.
	 * Bonus AI columns which are generally safe and ideal for the Range
	 * distribution.
	 * Also slightly bonus integral types which are often good candidates for
	 * Ranging.
	 * 
	 * In the "Safe Mode" only AI columns get non-zero score.
	 */
	public static Set<ScoreBonus> getBonusesForColumn(final TableColumn column, final boolean isSafeMode) {
		if (isSafeMode) {
			if (!isAutoIncrement(column)) {
				return Collections.singleton(ScoreBonus.UNSAFE_RANGE_COLUMN);
			}
		} else {
			final Set<ScoreBonus> bonuses = new LinkedHashSet<ScoreBonus>();
			if (isAutoIncrement(column)) {
				bonuses.add(ScoreBonus.AI_RANGE_COLUMN);
			} else if (column.getType().isIntegralType()) {
				bonuses.add(ScoreBonus.INTEGRAL_RANGE_COLUMN);
			}

			if (column.getUpdateCount() > 0) {
				bonuses.add(ScoreBonus.UPDATED_RANGE_COLUMN);
			}

			return bonuses;
		}

		return Collections.EMPTY_SET;
	}

	public static boolean isAutoIncrement(final TableColumn column) {
		final Column<?> columnInstance = column.getColumnInstance();
		return ((columnInstance instanceof PEColumn) && ((PEColumn) columnInstance).isAutoIncrement());
	}

	public static boolean hasAutoIncrement(final Set<TableColumn> columns) {
		for (final TableColumn column : columns) {
			if (isAutoIncrement(column)) {
				return true;
			}
		}

		return false;
	}

	public static boolean hasRangeCompatible(final Set<TableColumn> columns) {
		for (final TableColumn column : columns) {
			if (column.getType().isAcceptableRangeType()) {
				return true;
			}
		}

		return false;
	}

	public static boolean isRangeToRangeRelationship(final Relationship relationship) {
		final TemplateItem lhsModel = relationship.getLHS().getTableDistributionModel();
		final TemplateItem rhsModel = relationship.getRHS().getTableDistributionModel();
		return ((lhsModel instanceof Range) && (rhsModel instanceof Range));
	}

	/**
	 * Although collocated, OUTER JOINs to Broadcast require redistribution
	 * if the Broadcast table is the first table in the join.
	 */
	public static boolean isJoinToBroadcastAndRequiresRedist(final Relationship join) {
		final RelationshipSpecification type = join.getType();
		if (type.isOuterJoin()) {
			final TemplateItem lhsModel = join.getLHS().getTableDistributionModel();
			final TemplateItem rhsModel = join.getRHS().getTableDistributionModel();
			if (type.isLeftOuterJoin()) {
				return ((lhsModel instanceof Broadcast) && (rhsModel instanceof Range));
			} else if (type.isRightOuterJoin()) {
				return ((rhsModel instanceof Broadcast) && (lhsModel instanceof Range));
			}
		}

		return false;
	}

	public static MultiMap<RedistCause, Object> getRedistOperations(final TableStats table, final Set<JoinStats> joins,
			final Set<? extends TemplateRangeItem> availableRanges) {
		final MultiMap<RedistCause, Object> operations = new MultiMap<RedistCause, Object>();
		final TemplateModelItem model = table.getTableDistributionModel();
		if (!model.isBroadcast()) {
			final TemplateRangeItem range = findRangeForTable(availableRanges, table);
			final Set<TableColumn> dv = (range != null) ? range.getRangeColumnsFor(table) : Collections.EMPTY_SET;

			if (table.hasStatements(StatementType.ORDERBY)) {
				operations.put(RedistCause.ORDER_BY, table.getStatementCounts(StatementType.ORDERBY));
			}

			if (table.hasStatements(StatementType.UPDATE)) {
				final Map<TableColumn, Long> updateColumns = table.getUpdateColumns();
				for (final Entry<TableColumn, Long> entry : updateColumns.entrySet()) {
					final TableColumn column = entry.getKey();
					if (column.isPrimary() || column.isUnique()) {
						operations.put(RedistCause.UNIQUE_UPDATE, entry);
					} else if (!dv.isEmpty() && dv.contains(column)) {
						operations.put(RedistCause.DV_UPDATE, entry);
					}
				}
			}

			if (table.hasStatements(StatementType.JOIN)) {
				for (final JoinStats join : CorpusStats.findJoinsForTable(joins, table)) {
					if ((model instanceof Random) || (isRangeToRangeRelationship(join) || isJoinToBroadcastAndRequiresRedist(join))) {
						if (dv.isEmpty()
								|| (table.equals(join.getLHS()) && !dv.equals(join.getLeftColumns()))
								|| (table.equals(join.getRHS()) && !dv.equals(join.getRightColumns()))) {
							operations.put(RedistCause.NON_COLLOCATED_JOIN, join);
						}
					}
				}
			}
		}

		return operations;
	}

	public static Set<ForeignRelationship> getNonCollocatedFks(final TableStats table, final Set<? extends TemplateRangeItem> availableRanges) {
		final Set<ForeignRelationship> nonCollocatedRelationships = new LinkedHashSet<ForeignRelationship>();
		for (final ForeignRelationship relationship : table.getForwardRelationships()) {
			final TableStats lhs = relationship.getLHS();
			final TableStats rhs = relationship.getRHS();

			if (rhs.getTableDistributionModel().isBroadcast()) {
				continue; // Range -> Broadcast and Broadcast -> Broadcast are always collocated.
			} else if (lhs.getTableDistributionModel().isBroadcast()) {
				nonCollocatedRelationships.add(relationship);
				continue; // Broadcast -> Range cannot be collocated.
			}

			final TemplateRangeItem leftRange = findRangeForTable(availableRanges, lhs);
			final TemplateRangeItem rightRange = findRangeForTable(availableRanges, rhs);

			if ((leftRange == null) || (rightRange == null)) {
				nonCollocatedRelationships.add(relationship); // Tables not in a single range.
				continue;
			}

			final Set<TableColumn> leftColumns = leftRange.getRangeColumnsFor(lhs);
			final Set<TableColumn> rightColumns = rightRange.getRangeColumnsFor(rhs);

			if (!leftRange.equals(rightRange)
					|| !leftColumns.equals(relationship.getLeftColumns())
					|| !rightColumns.equals(relationship.getRightColumns())) {
				nonCollocatedRelationships.add(relationship);
			}
		}

		return nonCollocatedRelationships;
	}

	/**
	 * Frequent writes to a broadcast table without granular locking support may
	 * lead to excessive table locking within XA transactions.
	 */
	public static boolean hasExcessiveBroadcastLocking(final TableStats table, final boolean avoidAllWriteBroadcasting) {
		return (table.getTableDistributionModel().isBroadcast()
		&& hasExcessiveLocking(table, avoidAllWriteBroadcasting));
	}

	public static boolean hasExcessiveLocking(final TableStats table, final boolean avoidAllWriteBroadcasting) {
		return (avoidAllWriteBroadcasting || !table.supportsRowLocking()) && isMostlyWritten(table);
	}

	public static boolean isMostlyWritten(final TableStats table) {
		return table.getWritePercentage() >= MOSTLY_WRITTEN_THRESHOLD_PC;
	}

	public static boolean isFkCompatibleJoin(final JoinStats join) {

		/*
		 * If this is a join to Broadcast verify that it can be collocated
		 * with FK relationships pointing into this table (@see PE-1504).
		 */
		if (isJoinToBroadcastAndRequiresRedist(join)) {
			final RelationshipSpecification type = join.getType();
			final TableStats rangeSide = (type.isLeftOuterJoin()) ? join.getRHS() : join.getLHS();

			final Set<Set<TableColumn>> targetColumns = rangeSide.getUniqueTargetColumnGroups();
			if (!targetColumns.isEmpty()) {
				final Set<TableColumn> rangeSideJoinColumns = (type.isLeftOuterJoin()) ? join.getRightColumns() : join.getLeftColumns();
				return (join.isFkCompatible() && targetColumns.contains(rangeSideJoinColumns));
			}
		}

		return join.isFkCompatible();
	}

	private static TemplateRangeItem findRangeForTable(final Set<? extends TemplateRangeItem> ranges,
			final TableStats table) {
		for (final TemplateRangeItem range : ranges) {
			if (range.contains(table)) {
				return range;
			}
		}
		return null;
	}

	private static boolean hasRangeForTable(final Set<? extends TemplateRangeItem> ranges, final TableStats table) {
		return (findRangeForTable(ranges, table) != null);
	}

	private final CorpusStats schemaStats;
	private final Template base;
	private final PrintStream outputStream;
	private final Collection<TableStats> tableStatistics;
	private final Set<JoinStats> joinStatistics;
	private boolean enableWildcards;
	private boolean isVerbose;
	private boolean enableFksAsJoins;
	private boolean enableIdentTuples;
	private boolean avoidAllWriteBroadcasting;
	private TemplateModelItem fallbackModel;

	public AiTemplateBuilder(final CorpusStats schemaStats, final Template base, final TemplateModelItem fallbackModel, final PrintStream outputStream)
			throws PEException {
		if ((schemaStats == null) || (fallbackModel == null) || (outputStream == null)) {
			throw new IllegalArgumentException();
		}

		this.schemaStats = schemaStats;
		this.base = base;
		this.fallbackModel = fallbackModel;
		this.outputStream = outputStream;
		this.tableStatistics = schemaStats.getStatistics();
		this.joinStatistics = schemaStats.getJoinsStatistics();

		if (this.tableStatistics.isEmpty()) {
			throw new PEException("The schema contains no tables.");
		}
	}

	public void setWildcardsEnabled(final boolean enableWildcards) {
		this.enableWildcards = enableWildcards;
	}

	public void setVerbose(final boolean setVerbose) {
		this.isVerbose = setVerbose;
	}

	public void setForeignKeysAsJoins(final boolean enableFksAsJoins) {
		this.enableFksAsJoins = enableFksAsJoins;
	}

	public void setUseIdentTuples(final boolean enableIdentTuples) {
		this.enableIdentTuples = enableIdentTuples;
	}

	public void setAvoidAllWriteBroadcasting(final boolean avoidAllWriteBroadcasting) {
		this.avoidAllWriteBroadcasting = avoidAllWriteBroadcasting;
	}

	public void setFallbackModel(final TemplateModelItem model) {
		this.fallbackModel = model;
	}

	public List<Template> buildBroadcastCutoffTemplates(final List<String> databases, final long broadcastCardinalityCutoff, boolean isRowWidthWeightingEnabled)
			throws PEException {
		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(getTemplate(database, this.tableStatistics, broadcastCardinalityCutoff, isRowWidthWeightingEnabled));
		}

		return templates;
	}

	public List<Template> buildTemplates(final List<String> databases, final Long broadcastCardinalityCutoff, final boolean followForeignKeys,
			final boolean isSafeMode, final boolean isRowWidthWeightingEnabled) throws Exception {
		log(getWelcomeMessage(databases, broadcastCardinalityCutoff, followForeignKeys, isSafeMode));

		runPrePassAnalysis(databases, followForeignKeys, isSafeMode);

		identifyCandidateModels(broadcastCardinalityCutoff, isRowWidthWeightingEnabled);

		final Set<? extends TemplateRangeItem> ranges = identifyBestRanges(this.tableStatistics,
				this.joinStatistics, followForeignKeys, isSafeMode, isRowWidthWeightingEnabled);

		runPostPassAnalysis(this.tableStatistics, this.joinStatistics, ranges, followForeignKeys);

		final List<Template> templates = new ArrayList<Template>();
		for (final String database : databases) {
			templates.add(getTemplate(database, this.tableStatistics, ranges));
		}

		return templates;
	}

	private void identifyCandidateModels(final Long broadcastCardinalityCutoff, final boolean isRowWidthWeightingEnabled) throws Exception {
		log("Identifying candidate distribution models...");

		if (broadcastCardinalityCutoff != null) {
			identifyCandidateModels(this.tableStatistics, broadcastCardinalityCutoff, isRowWidthWeightingEnabled);
		} else {
			identifyCandidateModels(this.tableStatistics, isRowWidthWeightingEnabled);
		}

		for (final TableStats table : this.tableStatistics) {
			if (hasExcessiveBroadcastLocking(table, this.avoidAllWriteBroadcasting)) {
				table.setTableDistributionModel(Range.SINGLETON_TEMPLATE_ITEM);
				final StringBuilder logMessage = new StringBuilder();
				logMessage.append(getTableNameAndDistributionModel(table))
						.append(" (model override: broadcasting may cause excessive locking)");
				log(logMessage.toString(), MessageSeverity.ALERT);
			}
		}
	}

	private TemplateModelItem findBaseModel(final TableStats table) throws PEException {
		final TableTemplateType item = findBaseTemplateItem(table);
		if (item != null) {
			return getModelForName(item.getModel().value());
		}

		return null;
	}

	/**
	 * Check if the relationship is compatible with all user-specified ranges
	 * (if any).
	 */
	private boolean isBaseRangeCompatible(final Relationship relationship) throws PEException {
		final Set<TableColumn> leftColumns = findBaseRangeColumns(relationship.getLHS());
		final Set<TableColumn> rightColumns = findBaseRangeColumns(relationship.getRHS());

		return (((leftColumns == null) || leftColumns.equals(relationship.getLeftColumns()))
		&& ((rightColumns == null) || rightColumns.equals(relationship.getRightColumns())));
	}

	private Set<TableColumn> findBaseRangeColumns(final TableStats table) throws PEException {
		final TableTemplateType item = findBaseTemplateItem(table);
		if (isRangeTableItem(item)) {
			final Set<String> dv = new LinkedHashSet<String>(item.getColumn());
			return table.getColumns(dv);
		}

		return null;
	}

	private Set<CommonRange> getBaseRanges(final boolean isSafeMode) throws PEException {
		final Map<String, UserDefinedCommonRange> baseRanges = new LinkedHashMap<String, UserDefinedCommonRange>();
		if (this.base != null) {
			for (final TableTemplateType tableItem : this.base.getTabletemplate()) {
				if (isRangeTableItem(tableItem)) {
					final TableStats table = this.schemaStats.findTable(new QualifiedName(tableItem.getMatch()));
					if (table != null) {
						final String rangeName = tableItem.getRange();
						UserDefinedCommonRange range = baseRanges.get(rangeName);
						if (range == null) {
							range = new UserDefinedCommonRange(rangeName, isSafeMode);
							baseRanges.put(rangeName, range);
						}

						final Set<String> dv = new LinkedHashSet<String>(tableItem.getColumn());
						range.addUserDefinedDistribution(table, dv);
					}
				}
			}
		}

		return new LinkedHashSet<CommonRange>(baseRanges.values());
	}

	private static boolean isRangeTableItem(final TableTemplateType item) {
		return ((item != null) && item.getModel().equals(ModelType.RANGE));
	}

	private TableTemplateType findBaseTemplateItem(final TableStats table) {
		if (this.base != null) {
			for (final TableTemplateType tableItem : this.base.getTabletemplate()) {
				final String name = tableItem.getMatch();
				if (name.equals(table.getFullTableName())) {
					return tableItem;
				}
			}
		}

		return null;
	}

	private Template getTemplate(final String databaseName, final Collection<TableStats> tables, final long broadcastCardinalityCutoff,
			final boolean isRowWidthWeightingEnabled) throws PEException {
		for (final TableStats table : tables) {
			if (databaseName.equals(table.getSchemaName())) {
				setCardinalityBasedDistributionModel(table, broadcastCardinalityCutoff, Broadcast.SINGLETON_TEMPLATE_ITEM, Random.SINGLETON_TEMPLATE_ITEM,
						isRowWidthWeightingEnabled);
			}
		}

		// Ignore the safe mode - use whatever specified by the user.
		return getTemplate(databaseName, tables, this.getBaseRanges(false));
	}

	/**
	 * Set table distribution model based on its predicted cardinality.
	 */
	private void setCardinalityBasedDistributionModel(final TableStats table, final long broadcastCardinalityCutoff, final TemplateModelItem smallTableModel,
			final TemplateModelItem largeTableModel, final boolean isRowWidthWeightingEnabled) throws PEException {
		final TemplateModelItem baseModel = findBaseModel(table);
		if (baseModel != null) {
			table.setTableDistributionModel(baseModel);
			table.setDistributionModelFreezed(true);
		} else {
			if (table.getPredictedFutureSize(isRowWidthWeightingEnabled) > broadcastCardinalityCutoff) {
				table.setTableDistributionModel(largeTableModel);
			} else {
				table.setTableDistributionModel(smallTableModel);
			}
		}

		logTableDistributionModel(table, MessageSeverity.ALERT);
	}

	private Template getTemplate(final String databaseName,
			final Collection<TableStats> tables,
			final Set<? extends TemplateRangeItem> ranges) throws PEException {
		log("Building a template for '" + databaseName + "'...");

		final SortedSet<TableStats> databaseTables = new TreeSet<TableStats>();
		final SortedSet<TemplateRangeItem> databaseRanges = new TreeSet<TemplateRangeItem>(
				new Comparator<TemplateRangeItem>() {
					@Override
					public int compare(TemplateRangeItem a, TemplateRangeItem b) {
						return a.getTemplateItemName().compareTo(b.getTemplateItemName());
					}
				});
		for (final TableStats table : tables) {
			if (databaseName.equals(table.getSchemaName())) {
				final TemplateItem distributionModel = table.getTableDistributionModel();
				databaseTables.add(table);
				if (distributionModel instanceof Range) {
					databaseRanges.add(findRangeForTable(ranges, table));
				}
			}
		}

		final String commonNamePrefix = getCommonTableNamePrefix(databaseTables);

		final TemplateBuilder builder = new TemplateBuilder(databaseName);

		/* Append range declarations. */
		for (final TemplateRangeItem range : databaseRanges) {
			final String rangeName = removeTableNamePrefix(range.getTemplateItemName(), commonNamePrefix);
			builder.withRequirement(builder.toCreateRangeStatement(rangeName, "#sg#",
					range.getUniqueColumnTypes()));
		}

		/* Append table items. */
		for (final TableStats table : databaseTables) {
			final String tableName = replaceTableNamePrefix(table.getTableName(), commonNamePrefix);
			final TemplateItem distributionModel = table.getTableDistributionModel();
			if (distributionModel instanceof Range) {
				final TemplateRangeItem tableRange = findRangeForTable(databaseRanges, table);
				final String rangeName = removeTableNamePrefix(tableRange.getTemplateItemName(), commonNamePrefix);
				final String[] rangeColumnNames = getColumnNames(tableRange.getRangeColumnsFor(table)).toArray(new String[] {});
				builder.withRangeTable(tableName, rangeName, rangeColumnNames);
			} else {
				builder.withTable(tableName, distributionModel.getTemplateItemName());
			}
		}

		return builder.toTemplate();
	}

	private void runPrePassAnalysis(final List<String> databases, final boolean followForeignKeys, final boolean isSafeMode) {
		log("Performing pre-pass analysis...");
		if (followForeignKeys && isSafeMode) {
			log("Template is generated with both <fk> and <safe> modes ON. This is a very constrained configuration which is likely to cause excessive broadcasting. Consider relaxing one of the constraints.",
					MessageSeverity.SEVERE);
		}

		for (final String database : databases) {
			checkFkRelationships(database, followForeignKeys);
		}

		checkCorpusCoverage();
	}

	private void checkFkRelationships(final String database, final boolean followForeignKeys) {
		final Pair<Integer, Integer> numRelationshipsAndActions = getNumOfFkRelationshipsIn(database);
		final int numFkRelationships = numRelationshipsAndActions.getFirst();
		final int numFkRelationshipsWithActions = numRelationshipsAndActions.getSecond();

		if (followForeignKeys && (numFkRelationships == 0)) {
			log("There are no FK relationships in the schema '" + database + "'. The the <fk> flag is not necessary.", MessageSeverity.ALERT);
		} else if (!followForeignKeys && (numFkRelationships > 0)) {
			final MessageSeverity severity = (numFkRelationshipsWithActions > 0) ? MessageSeverity.SEVERE : MessageSeverity.WARNING;
			log("There are FK relationships in the schema '" + database
					+ "'. Running the analysis without the <fk> flag ON is likely to produce invalid (non-collocated) templates.",
					severity);
		}

		if ((numFkRelationships > 0) && (numFkRelationshipsWithActions == 0)) {
			log("The FK relationships in the schema '" + database + "' do not have referential actions and may potentially be ignored.", MessageSeverity.ALERT);
		}
	}

	/**
	 * Return the total number of FK relationships and the number of those with
	 * referential actions found in the schema.
	 */
	private Pair<Integer, Integer> getNumOfFkRelationshipsIn(final String database) {
		int numFkRelationships = 0;
		int numFkRelationshipsWithActions = 0;
		for (final TableStats table : this.tableStatistics) {
			if (database.equals(table.getSchemaName())) {
				final Set<ForeignRelationship> tableFkRelationships = table.getForwardRelationships();
				numFkRelationships += tableFkRelationships.size();
				for (final ForeignRelationship relationship : tableFkRelationships) {
					if (relationship.hasReferentialActions()) {
						++numFkRelationshipsWithActions;
					}
				}
			}
		}

		return new Pair<Integer, Integer>(numFkRelationships, numFkRelationshipsWithActions);
	}

	private void checkCorpusCoverage() {
		final double pcTablesWithStatements = MathUtils.round(this.schemaStats.getCorpusCoverage(), NUMBER_DISPLAY_PRECISION);

		MessageSeverity severity = MessageSeverity.INFO;
		if (pcTablesWithStatements < LOWER_CORPUS_COVERAGE_THRESHOLD_PC) {
			severity = MessageSeverity.SEVERE;
		} else if (pcTablesWithStatements < UPPER_CORPUS_COVERAGE_THRESHOLD_PC) {
			severity = MessageSeverity.WARNING;
		}

		log("Corpus coverage of " + this.schemaStats + " is " + pcTablesWithStatements + "%.", severity);
	}

	private void identifyCandidateModels(final Collection<TableStats> tables, final boolean isRowWidthWeightingEnabled) throws Exception {
		final SortedSet<Long> sortedCardinalities = new TreeSet<Long>();
		for (final TableStats table : tables) {
			sortedCardinalities.add(table.getPredictedFutureSize(isRowWidthWeightingEnabled));
		}

		for (final TableStats table : tables) {
			final TemplateModelItem baseModel = findBaseModel(table);
			if (baseModel != null) {
				table.setTableDistributionModel(baseModel);
				table.setDistributionModelFreezed(true);
				logTableDistributionModel(table, MessageSeverity.ALERT);
			} else {
				final List<FuzzyTableDistributionModel> modelsSortedByScore = FuzzyLinguisticVariable
						.evaluateDistributionModels(
								new Broadcast(table, sortedCardinalities, isRowWidthWeightingEnabled),
								new Range(table, sortedCardinalities, isRowWidthWeightingEnabled));

				table.setTableDistributionModel(Collections.max(modelsSortedByScore,
						FuzzyLinguisticVariable.getScoreComparator()));

				log(table.toString().concat(": ").concat(StringUtils.join(modelsSortedByScore, ", ")));
			}
		}
	}

	private void identifyCandidateModels(final Collection<TableStats> tables, final long broadcastCardinalityCutoff, final boolean isRowWidthWeightingEnabled)
			throws Exception {
		for (final TableStats table : tables) {
			setCardinalityBasedDistributionModel(table, broadcastCardinalityCutoff, Broadcast.SINGLETON_TEMPLATE_ITEM, Range.SINGLETON_TEMPLATE_ITEM,
					isRowWidthWeightingEnabled);
		}
	}

	/*
	 * Try to identify the best ranges based on the following filter order.
	 * 1. JOIN
	 * 2. WHERE
	 * 3. GROUP BY
	 */
	private Set<? extends TemplateRangeItem> identifyBestRanges(
			final Collection<TableStats> tables, final Set<JoinStats> joins,
			boolean followForeignKeys, final boolean isSafeMode, final boolean isRowWidthWeightingEnabled) throws PEException {
		log("Identifying distribution ranges...");
		log("Processing join statistics...");

		final Ranges rangeToRangeRanges = new Ranges(isSafeMode, isRowWidthWeightingEnabled);

		/* Preload user defined ranges. */
		for (final CommonRange range : getBaseRanges(isSafeMode)) {
			rangeToRangeRanges.add(range);
		}

		Set<TemplateRangeItem> topRangeToRangeTopRanges = null;
		if (followForeignKeys) {

			/* Handle special foreign relationship cases. */
			resolveForeignCollocationConflicts(tables, isRowWidthWeightingEnabled);

			/* Add joins compatible with the FK relationships. */
			for (final JoinStats join : joins) {
				if (isFkCompatibleJoin(join)) {
					addRangeToRangeRelationship(join, rangeToRangeRanges);
				}
			}

			rangeToRangeRanges.generateAllPossibleCombinations();

			/* Add FK relationships. */
			for (final TableStats table : tables) {
				for (final ForeignRelationship relationship : table.getForwardRelationships()) {
					addRangeToRangeRelationship(relationship, rangeToRangeRanges);
				}
			}

			topRangeToRangeTopRanges = getTopRanges(rangeToRangeRanges);

			/* All Range tables with FK and without a range -> Broadcast. */
			for (final TableStats table : tables) {
				if ((table.getTableDistributionModel() instanceof Range) && !table.getBackwardRelationships().isEmpty()
						&& !hasRangeForTable(topRangeToRangeTopRanges, table)) {
					table.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
				}
			}

		} else {
			for (final JoinStats join : joins) {
				addRangeToRangeRelationship(join, rangeToRangeRanges);
			}

			if (this.enableFksAsJoins) {
				/* Add FK relationships. */
				for (final TableStats table : tables) {
					for (final ForeignRelationship relationship : table.getForwardRelationships()) {
						addRangeToRangeRelationship(relationship, rangeToRangeRanges);
					}
				}
			}

			rangeToRangeRanges.generateAllPossibleCombinations();

			topRangeToRangeTopRanges = getTopRanges(rangeToRangeRanges);
		}

		log("Processing orphaned range tables...");
		for (final TableStats table : tables) {
			final TemplateModelItem model = table.getTableDistributionModel();

			/*
			 * This Range candidate has no range.
			 * Either its proposed range was not identified as optimal
			 * or it is only being joined to Broadcast tables.
			 */
			if ((model instanceof Range) && !hasRangeForTable(topRangeToRangeTopRanges, table)) {

				final ColorStringBuilder logMessage = new ColorStringBuilder();
				logMessage.append(getTableNameAndDistributionModel(table)).append(" (");

				/*
				 * Try to range on OUTER JOIN columns if to a Broadcast
				 * table and would require redistribution.
				 */
				TemplateRangeItem newRange = PrivateRange.fromOuterJoinColumns(table, joins, topRangeToRangeTopRanges, isSafeMode);
				if (newRange != null) {
					topRangeToRangeTopRanges.add(newRange);
					logMessage.append("ranged on OUTER JOIN column(s): ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)));
				}

				final boolean hasRedistOperations = !getRedistOperations(table, joins, topRangeToRangeTopRanges).isEmpty();
				final boolean mayCauseExcessiveLocking = hasExcessiveLocking(table, this.avoidAllWriteBroadcasting);
				if (!this.fallbackModel.isBroadcast()
						|| !hasRedistOperations
						|| mayCauseExcessiveLocking) {

					if (this.fallbackModel.isBroadcast()) {
						logMessage.append("fallback model override: ", MessageSeverity.ALERT.getColor());
						if (!hasRedistOperations) {
							logMessage.append(" no redistribution required", MessageSeverity.ALERT.getColor());
						} else if (mayCauseExcessiveLocking) {
							logMessage.append(" broadcasting may cause excessive locking", MessageSeverity.ALERT.getColor());
						}
						logMessage.append(" -> ");
					}

					/*
					 * Try to Range on identity columns instead.
					 */
					if (newRange == null) {
						if (table.hasIdentColumns() || table.hasGroupByColumns()) {
							newRange = PrivateRange.fromWhereColumns(table, topRangeToRangeTopRanges,
									isSafeMode, this.enableIdentTuples);
							if (newRange == null) {
								newRange = PrivateRange.fromGroupByColumns(table, topRangeToRangeTopRanges, isSafeMode);
							}
							if (newRange != null) {
								topRangeToRangeTopRanges.add(newRange);
								logMessage.append("constant filter column ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)))
										.append(" found");
							}
						}
					}

					/*
					 * Look for AI columns and similar columns used in existing
					 * ranges.
					 */
					if (newRange == null) {
						newRange = PrivateRange.fromAllColumns(table, topRangeToRangeTopRanges,
								isSafeMode);
						if (newRange != null) {
							final Set<TableColumn> columnSet = newRange
									.getRangeColumnsFor(table);
							if (hasAutoIncrement(columnSet)) {
								topRangeToRangeTopRanges.add(newRange);
								logMessage.append("AI range column ").append(toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)))
										.append(" found");
							} else {
								boolean found = false;
								for (final TemplateRangeItem range : topRangeToRangeTopRanges) {
									if (range.hasCommonColumn(columnSet)) {

										topRangeToRangeTopRanges.add(newRange);
										found = true;
										logMessage.append("ranged on foreign range column(s): ", MessageSeverity.WARNING.getColor()).append(
												toStringOfDelimitedColumnNames(newRange.getRangeColumnsFor(table)), MessageSeverity.WARNING.getColor());

										break;
									}
								}

								if (!found) {
									newRange = null;
								}
							}
						}
					}
				}

				/* Fall back. */
				if (newRange == null) {
					table.setTableDistributionModel((this.fallbackModel.isBroadcast() && !mayCauseExcessiveLocking) ? Broadcast.SINGLETON_TEMPLATE_ITEM
							: Random.SINGLETON_TEMPLATE_ITEM);
					logMessage.append("no suitable range columns found", MessageSeverity.WARNING.getColor());
				}

				log(logMessage.append(")").toString());
			}
		}

		return topRangeToRangeTopRanges;
	}

	private void runPostPassAnalysis(
			final Collection<TableStats> tables, final Set<JoinStats> joins,
			Set<? extends TemplateRangeItem> ranges, boolean followForeignKeys) throws PEException {
		log("Performing post-pass analysis...");
		for (final TableStats table : tables) {
			final Set<ForeignRelationship> nonCollocatedFks = getNonCollocatedFks(table, ranges);
			if (!nonCollocatedFks.isEmpty()) {
				final Set<ForeignRelationship> userOverriden = new LinkedHashSet<ForeignRelationship>(nonCollocatedFks.size());
				for (final ForeignRelationship fk : nonCollocatedFks) {
					final TableStats lhs = fk.getLHS();
					final TableStats rhs = fk.getRHS();
					if (lhs.hasDistributionModelFreezed() || rhs.hasDistributionModelFreezed()) {
						userOverriden.add(fk);
					}
				}

				MessageSeverity severity = MessageSeverity.WARNING;
				if (followForeignKeys) {
					final Set<ForeignRelationship> nonOverriden = Sets.difference(nonCollocatedFks, userOverriden);
					if (!nonOverriden.isEmpty()) {
						throw new PEException("Failed to collocate all foreign keys: " + StringUtils.join(nonOverriden, ", "));
					}

					severity = MessageSeverity.SEVERE;
					log("The custom base template you specified breaks the collocation rules on the following FK relationships.", severity);
				} else {
					log("The following FKs are not collocated.", severity);
					log("You may need to re-run the analysis with <fk> mode ON.", severity);
				}

				for (final ForeignRelationship fk : nonCollocatedFks) {
					log(fk.toString(), severity, 1);
				}
			}

			if (hasExcessiveBroadcastLocking(table, this.avoidAllWriteBroadcasting)) {
				log("Locking on a frequently written broadcast table " + table.toString() + " may lead to reduced concurrency.", MessageSeverity.WARNING);
			}

			final MultiMap<RedistCause, Object> operations = getRedistOperations(table, joins, ranges);
			if (!operations.isEmpty()) {
				final StringBuilder message = new StringBuilder();
				message.append("The following operations on table ").append(table);
				if (table.hasDistributionModelFreezed()) {
					message.append(" distributed based on a user-defined model ").append(table.getTableDistributionModel());
				}
				message.append(" still require redistribution.");
				log(message.toString(), MessageSeverity.WARNING);
				for (final RedistCause cause : operations.keySet()) {
					for (final Object operation : operations.get(cause)) {
						log(cause.print(operation), MessageSeverity.WARNING, 1);
					}
				}
			}
		}
	}

	/**
	 * Joins to Broadcast tables will always be collocated.
	 * INNER JOINs to Broadcast tables should not trigger redistribution.
	 * OUTER JOINs with the first table being Broadcast tables will trigger
	 * redistribution of the second (Range) table unless it is ranged on the
	 * joined column. This is to prevent getting duplicate rows from the first
	 * (Broadcast) table.
	 */
	private void addRangeToRangeRelationship(final Relationship relationship, final Ranges rangeToRangeRanges)
			throws PEException {
		if ((isRangeToRangeRelationship(relationship) || isJoinToBroadcastAndRequiresRedist(relationship))
				&& isBaseRangeCompatible(relationship)) {
			rangeToRangeRanges.addJoin(relationship);
		}
	}

	private Set<TemplateRangeItem> getTopRanges(final Ranges ranges) {
		if (!ranges.isEmpty()) {
			final SortedSet<Long> sortedJoinCardinalities = new TreeSet<Long>(ranges.getPredictedFutureJoinCardinalities());
			final Set<Long> uniqueJoinFrequencies = new HashSet<Long>(ranges.getJoinFrequencies());
			ranges.evaluate(uniqueJoinFrequencies, sortedJoinCardinalities);
		}

		/*
		 * Retain only the highest score ranges,
		 * removing the lower score alternatives.
		 */
		final List<CommonRange> sortedRanges = ranges.getSortedCommonRanges();
		final Set<TemplateRangeItem> topRanges = new TreeSet<TemplateRangeItem>(new Comparator<TemplateRangeItem>() {
			@Override
			public int compare(TemplateRangeItem a, TemplateRangeItem b) {
				return a.getTemplateItemName().compareTo(b.getTemplateItemName());
			}
		});
		while (!sortedRanges.isEmpty()) {
			final CommonRange top = Collections.max(sortedRanges, FuzzyLinguisticVariable.getScoreComparator());
			if ((top.getScore() > 0.0) || (top instanceof UserDefinedCommonRange)) {
				topRanges.add(top);
				final ColorStringBuilder progress = new ColorStringBuilder();
				progress.append("Overlapping common ranges: ").append(LINE_SEPARATOR);
				final List<CommonRange> overlappingRanges = Ranges.getOverlapping(sortedRanges, top);
				sortedRanges.removeAll(overlappingRanges);
				for (final CommonRange groupRange : overlappingRanges) {
					progress.append(groupRange);
					progress.append(LINE_SEPARATOR);
				}
				log(progress.toString());
			} else {

				/*
				 * All ranges have zero score.
				 * Most likely because we are in the safe mode
				 * considering only auto-increment columns.
				 * Move on and try to find private ranges for the tables
				 * based on other (ident) columns or fall back to Random.
				 */
				sortedRanges.clear();
			}
		}

		return topRanges;
	}

	/**
	 * Handles collocation special cases.
	 * 
	 * 1. A table is being referenced on a single column group.
	 * 
	 * a) The table is referenced by two or more unique column groups from a
	 * single table.
	 * The solution which preserves collocation is to make the
	 * table and all the tables it points at Broadcast.
	 * 
	 * b) The table has unique foreign and target column groups. In other words,
	 * the table is being pointed at and points on two or more unique column
	 * groups.
	 * The only solution is making the pointed tables
	 * and all their descendants Broadcast.
	 * 
	 * 2. A table is being referenced on two or more unique column groups. The
	 * only solution in this case is making the table and all the tables it
	 * points at Broadcast.
	 * 
	 * 
	 * NOTE: Basic one-to-one collocation cases:
	 * 
	 * a) Range -> Broadcast: always collocated
	 * b) Range -> Range: collocated only if in the same range.
	 * c) Broadcast -> Range: make the referenced table Broadcast (a), or
	 * colocate the two tables on the same range (b).
	 * 
	 * NOTE: Same rules hold for self-referencing relationships (table with a
	 * foreign key into itself).
	 */
	private void resolveForeignCollocationConflicts(final Collection<TableStats> tables, final boolean isRowWidthWeightingEnabled) throws PEException {
		log("Resolving FK collocation...");

		/*
		 * Make sure there are no Broadcast -> Range relationships (c) by making
		 * both tables Range.
		 */
		for (final TableStats table : tables) {
			if (!table.hasDistributionModelFreezed() && (table.getTableDistributionModel() instanceof Range)) {
				for (final TableStats childTable : table.getReferencingForeignTables()) {
					if (childTable.getTableDistributionModel() instanceof Broadcast) {
						final Set<TableStats> affectedTables = makeBackwardTableTreeRange(childTable);
						log("FK forced range: range table '" + table.getFullTableName() + "' is referenced by a broadcast table '"
								+ childTable.getFullTableName() + "'. Had to range '" + affectedTables.size() + "' table(s).", MessageSeverity.ALERT);
					}
				}
			}
		}

		/*
		 * Now, we should have only Range -> Broadcast (a) and Range -> Range
		 * (b) relationships.
		 */

		final SortedSet<TableStats> forcedBroadcastTables = new TreeSet<TableStats>(
				Collections.reverseOrder(new TableSizeComparator(isRowWidthWeightingEnabled)));

		/* Resolve the special cases. */
		for (final TableStats table : tables) {

			/*
			 * Here we handle only the Range -> Range (b) case.
			 * Range -> Broadcast (a) is always collocated and we never change
			 * the parent table's model to anything other than Broadcast.
			 */
			if (!table.hasDistributionModelFreezed() && (table.getTableDistributionModel() instanceof Range)) {
				final Set<Set<TableColumn>> uniqueTargetColumnGroups = table
						.getUniqueTargetColumnGroups();

				if (uniqueTargetColumnGroups.size() == 1) { // Case (1)

					/* Case (1a) */
					final Set<ForeignRelationship> backwardRelationships = table.getBackwardRelationships();
					for (@SuppressWarnings("unused")
					final Set<TableColumn> targetColumnGroup : uniqueTargetColumnGroups) {
						final Set<TableStats> visitedReferencingTables = new HashSet<TableStats>();
						for (final ForeignRelationship relationship : backwardRelationships) {
							final TableStats targetTable = relationship.getRHS();
							if (!visitedReferencingTables.add(targetTable)) {
								final Set<TableStats> affectedTables = makeForwardTableTreeBroadcast(table);
								log("FK forced broadcast: table '" + table.getFullTableName() + "' referenced by '" + targetTable.getFullTableName()
										+ "' on two or more unique column groups. Had to broadcast '" + affectedTables.size()
										+ "' table(s) with total size of '"
										+ CorpusStats.computeTotalSizeKb(affectedTables) + "KB'", MessageSeverity.WARNING);
								forcedBroadcastTables.addAll(affectedTables);
								break;
							}
						}
					}

					if (!(table.getTableDistributionModel() instanceof Range)) {
						continue; // The case already resolved from above.
					}

					/* Case (1b) */
					final Set<ForeignRelationship> forwardRelationships = table.getForwardRelationships();
					final Set<TableStats> affectedTables = new LinkedHashSet<TableStats>();
					for (final Set<TableColumn> targetColumnGroup : uniqueTargetColumnGroups) {
						for (final ForeignRelationship relationship : forwardRelationships) {
							if (!targetColumnGroup.equals(relationship.getForeignColumns())) {
								final TableStats targetTable = relationship.getRHS();
								affectedTables.addAll(makeForwardTableTreeBroadcast(targetTable));
							}
						}
					}

					if (!affectedTables.isEmpty()) {
						log("FK forced broadcast: table '" + table.getFullTableName()
								+ "' has unique foreign and target column groups. Had to broadcast '" + affectedTables.size()
								+ "' table(s) with total size of '" + CorpusStats.computeTotalSizeKb(affectedTables) + "KB'", MessageSeverity.WARNING);
						forcedBroadcastTables.addAll(affectedTables);
					}

				} else if (uniqueTargetColumnGroups.size() > 1) { // Case (2)
					final Set<TableStats> affectedTables = makeForwardTableTreeBroadcast(table);
					log("FK forced broadcast: table '" + table.getFullTableName() + "' referenced on two or more unique column groups. Had to broadcast '"
							+ affectedTables.size() + "' table(s) with total size of '" + CorpusStats.computeTotalSizeKb(affectedTables) + "KB'",
							MessageSeverity.WARNING);
					forcedBroadcastTables.addAll(affectedTables);
				}
			}
		}

		/* Print out broadcasted tables. */
		log("The following tables were forced broadcast:", MessageSeverity.WARNING);
		for (final TableStats table : forcedBroadcastTables) {
			log(table.toString(), MessageSeverity.WARNING, 1);
		}
	}

	private Set<TableStats> makeBackwardTableTreeRange(final TableStats root)
			throws PEException {
		final Set<TableStats> traversed = new LinkedHashSet<TableStats>();
		makeBackwardTableTreeRange(root, traversed);
		return traversed;
	}

	private void makeBackwardTableTreeRange(final TableStats root, final Set<TableStats> traversedNodes)
			throws PEException {
		traversedNodes.add(root);
		for (final TableStats table : root.getReferencingForeignTables()) {
			if (!(table.getTableDistributionModel() instanceof Range)) {
				traversedNodes.add(table);
				/* Change model first, then recurse to avoid cycles. */
				table.setTableDistributionModel(Range.SINGLETON_TEMPLATE_ITEM);
				makeBackwardTableTreeRange(table, traversedNodes);
			}
		}
		root.setTableDistributionModel(Range.SINGLETON_TEMPLATE_ITEM);
	}

	private Set<TableStats> makeForwardTableTreeBroadcast(final TableStats root)
			throws PEException {
		final Set<TableStats> traversed = new LinkedHashSet<TableStats>();
		makeForwardTableTreeBroadcast(root, traversed);
		return traversed;
	}

	private void makeForwardTableTreeBroadcast(final TableStats root, final Set<TableStats> traversedNodes)
			throws PEException {
		traversedNodes.add(root);
		for (final TableStats table : root.getReferencedForeignTables()) {
			if (!(table.getTableDistributionModel() instanceof Broadcast)) {
				traversedNodes.add(table);
				/* Change model first, then recurse to avoid cycles. */
				table.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
				makeForwardTableTreeBroadcast(table, traversedNodes);
			}
		}
		root.setTableDistributionModel(Broadcast.SINGLETON_TEMPLATE_ITEM);
	}

	private String toStringOfDelimitedColumnNames(final Set<TableColumn> columns) {
		return "'" + StringUtils.join(getColumnNames(columns), "', '") + "'";
	}

	private List<String> getColumnNames(final Set<TableColumn> columns) {
		final List<String> names = new ArrayList<String>();
		for (final TableColumn column : columns) {
			names.add(column.getName().getUnquotedName().get());
		}

		return names;
	}

	private String getCommonTableNamePrefix(final SortedSet<TableStats> tables) {
		final String firstName = tables.first().getTableName();
		final String lastName = tables.last().getTableName();

		final int minLength = Math.min(firstName.length(), lastName.length());
		for (int i = 0; i < minLength; ++i) {
			if ((i > (TABLE_NAME_MIN_PREFIX_LENGTH - 1)) && (firstName.charAt(i) != lastName.charAt(i))) {
				return firstName.substring(0, i);
			}
		}

		return firstName.substring(0, minLength);
	}

	private String removeTableNamePrefix(final String value, final String prefix) {
		return replaceTableNamePrefix(value, prefix, "");
	}

	private String replaceTableNamePrefix(final String value, final String prefix) {
		return replaceTableNamePrefix(value, prefix, TABLE_NAME_WILDCARD);
	}

	private String replaceTableNamePrefix(final String value, final String prefix, final String replacement) {
		// Exclude prefixes shorter than 3 chars.
		if (this.enableWildcards && !value.equals(prefix) && (prefix.length() > TABLE_NAME_MIN_PREFIX_LENGTH)) {
			return value.replaceFirst(prefix, replacement);
		}

		return value;
	}

	private void logTableDistributionModel(final TableStats table, final MessageSeverity severity) {
		log(getTableNameAndDistributionModel(table), severity);
	}

	private void log(final String message) {
		log(message, MessageSeverity.INFO);
	}

	private void log(final String message, final MessageSeverity severity) {
		log(message, severity, 0);
	}

	private void log(final String message, final MessageSeverity severity, final int numIndents) {
		logger.log(severity.getLogLevel(), message);
		if (this.isVerbose && (this.outputStream != null)) {
			CLIBuilder.printInColor(StringUtils.repeat(LINE_INDENT, numIndents).concat(severity.toString()).concat(": ").concat(message), severity.getColor(),
					this.outputStream);
		}
	}
}