// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

public enum DMLExplainReason {

	// binary strategy builder
	BCAST_TO_PG_OUTER_JOIN("inner join to single pg site"),
	BOTH_CONSTRAINED_LEFT_UNIQUELY("both sides are constrained but left side is uniquely constrained"),
	BOTH_CONSTRAINED_RIGHT_UNIQUELY("both sides are constrained but right side is uniquely constrained"),
	BOTH_CONSTRAINED_LEFT_BETTER("both sides constrained, left side has better constraint"),
	BOTH_CONSTRAINED_RIGHT_BETTER("both sides constrained, right side has better constraint"),
	BOTH_CONSTRAINED_EQUALLY("both sides constrained, equally good"),
	BOTH_WC_NO_CONSTRAINTS("both sides have where clause (but no constraints)"),
	ONE_SIDE_CONSTRAINED("one side is uniquely constrained, other side unconstrained"),
	ONE_SIDE_UNIQUELY_CONSTRAINED("one side is uniquely constrained, other side unconstrained"),
	ONE_SIDE_CONSTRAINED_ONE_COLUMN("one side is constrained with single column result set, other side unconstrained"),
	ROW_ESTIMATES("using row estimates"),
	BOTH_NO_WC("neither side has where clause"),
	BASIC_PARTITION_QUERY("partition query"),
	CARTESIAN_JOIN("cartesian join"),
	LEFT_WC_NO_CONSTRAINT_RIGHT_NO_WC("left side has where clause but no constraint, right side has no where clause"),
	LEFT_NO_WC_RIGHT_WC_NO_CONSTRAINT("left side has no where clause, right side has where clause but no constraint"),
	LOOKUP_JOIN_LOOKUP_TABLE("lookup table for lookup join"),
	LOOKUP_JOIN("join of lookup table and partition"),
	JOIN("join"),
	CLUSTERED_JOIN("clustered join"),
	OJ_BOTH_CONSTRAINED_LEFT_UNIQUE_ALLOWED_BCAST("both sides constrained but left side is unique and allowed to be bcast"),
	OJ_BOTH_CONSTRAINED_RIGHT_UNIQUE_ALLOWED_BCAST("both sides constrained but right side is unique and allowed to be bcast"),
	OJ_LEFT_UNIQUE_ALLOWED_BCAST_RIGHT_UNCONSTRAINED("right side is unconstrained, left side is unique and allowed to be bcast"),
	OJ_RIGHT_UNIQUE_ALLOWED_BCAST_LEFT_UNCONSTRAINED("left side is unconstrained, right side is unique and allowed to be bcast"),


	// dist key
	DISTRIBUTION_KEY_MATCHED("found distribution key match"),
	ORDER_BY("order by"),
	UNION("union"),
	SINGLE_SITE("single site"),
	IN_MEMORY_LIMIT_NOT_APPLICABLE("limit expression too complex, cannot optimize"),
	GROUP_BY_NO_ORDER_BY("group by found, no explicit order by"),
	
	CORRELATED_SUBQUERY_LOOKUP_TABLE("lookup table for correlated subquery"),
	PROJECTION_CORRELATED_SUBQUERY("correlated subquery in projection"),
	
	MULTI_TABLE_DELETE("multi table delete"),
	SINGLE_TABLE_DELETE("single table delete"),
	
	COMPLEX_UPDATE_REDIST_TO_TARGET_TABLE("redist complex updates back to target table"),
	
	TENANT_DISTRIBUTION("container or tenant distribution"),
	
	GRAND_AGG_DISTINCT("grand agg distinct"),
	AGGREGATION("aggregation"),
	WRONG_DISTRIBUTION("no matching distribution"),
	
	ADHOC("ad hoc"),
	SESSION_TOO_COMPLEX("too complex, must delegate"),
	
	EXPLAIN_NOPLAN("planning inhibited"),
	RAWPLAN("raw plan");
	
	private final String description;
	private final DMLExplainRecord basic;
	
	private DMLExplainReason(String d) {
		description = d;
		basic = new DMLExplainRecord(this);
	}
	
	public String getDescription() {
		return description;
	}
	
	public DMLExplainRecord makeRecord() {
		return basic;
	}
}
