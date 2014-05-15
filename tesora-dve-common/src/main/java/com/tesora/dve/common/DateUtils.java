// OS_STATUS: public
package com.tesora.dve.common;

import java.util.Calendar;
import java.util.Date;

public final class DateUtils {
	
	private DateUtils() {
	}

	public static Date getSpecifiedDate(int year, int month, int day) {
		Calendar cal = Calendar.getInstance();
		cal.clear();
		cal.set(year, month - 1, day, 0 ,0, 0);
		cal.set(Calendar.MILLISECOND, 0);

		return cal.getTime();
	}
}
