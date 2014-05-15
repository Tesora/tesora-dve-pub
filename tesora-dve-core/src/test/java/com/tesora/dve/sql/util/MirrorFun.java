// OS_STATUS: public
package com.tesora.dve.sql.util;

import com.tesora.dve.sql.SchemaTest;

public abstract class MirrorFun extends MirrorTest {
	
	protected boolean ignoreOrder = false;
	protected boolean explainOnFailure = true;
	protected boolean omitMetadataChecks = false;
	
	public MirrorFun(boolean expl) {
		this(false,expl);
	}

	public MirrorFun(boolean unordered, boolean ignoreMD, boolean explain) {
		ignoreOrder = unordered;
		omitMetadataChecks = ignoreMD;
		explainOnFailure = explain;
	}
	
	public MirrorFun(boolean unordered, boolean explain) {
		this(unordered,false,explain);
	}
	
	public String getContext() {
		return "mirrorfun";
	}
	
	public abstract ResourceResponse execute(TestResource mr) throws Throwable;

	@Override
	public void execute(TestResource checkdb, TestResource sysdb) throws Throwable {
		ResourceResponse cr = execute(checkdb);
		ResourceResponse sr = execute(sysdb);
		if (sysdb == null) return;
		if (cr != null && sr != null) {
			try {
				cr.assertEqualResponse(getContext(), sr);
				cr.assertEqualResults(getContext(), ignoreOrder, omitMetadataChecks, sr);
			} catch (AssertionError ae) {
				// annotate if we actually can get the underlying statement, otherwise don't bother
				if (explainOnFailure)
					throw SchemaTest.annotateFailureWithPlan(ae,getContext(),checkdb,sysdb,null);
				throw ae;
			}
		}
	}
}