// OS_STATUS: public
package com.tesora.dve.server.statistics;

import java.io.Serializable;

public class TimingSet implements Serializable {

	private static final long serialVersionUID = 1L;
	
	SiteStatKey ssk;
	
	TimingValue oneSec;
	TimingValue tenSec;
	TimingValue oneMin;
	TimingValue cumulative;
	
	public TimingSet() {
	}
	
	public TimingSet(SiteStatKey ssk) {
		super();
		this.ssk = ssk;
		this.oneSec = new TimingValue();
		this.tenSec = new TimingValue();
		this.oneMin = new TimingValue();
		this.cumulative = new TimingValue();
	}
	
	public TimingSet(SiteStatKey ssk, TimingValue oneSec, TimingValue tenSec, TimingValue oneMin, TimingValue cumulative) {
		super();
		this.ssk = ssk;
		this.oneSec = oneSec;
		this.tenSec = tenSec;
		this.oneMin = oneMin;
		this.cumulative = cumulative;
	}
	
	public SiteStatKey getStatKey() {
		return ssk;
	}
	
	public String getType() {
		return ssk.getType().name();
	}
	public String getName() {
		return ssk.getName();
	}
	public String getOperationClass() {
		return ssk.getOpClass().name();
	}

	public TimingValue getOneSec() {
		return oneSec;
	}
	public TimingValue getTenSec() {
		return tenSec;
	}
	public TimingValue getOneMin() {
		return oneMin;
	}

	public void setOneSec(TimingValue oneSec) {
		this.oneSec = oneSec;
	}

	public void setTenSec(TimingValue tenSec) {
		this.tenSec = tenSec;
	}

	public void setOneMin(TimingValue oneMin) {
		this.oneMin = oneMin;
	}

	public TimingValue getCumulative() {
		return cumulative;
	}

	public void setCumulative(TimingValue cumulative) {
		this.cumulative = cumulative;
	}
}
