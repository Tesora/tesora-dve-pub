package com.tesora.dve.exceptions;

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

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.DefaultThrowableRenderer;
import org.apache.log4j.spi.ThrowableRenderer;

import com.tesora.dve.common.PEContext;

/**
 * Log4j exception renderer that logs the PEContext at the deepest point
 * possible in the exception chain.
 * 
 */
public class PEThrowableRenderer implements ThrowableRenderer {

	private final ThrowableRenderer target = new DefaultThrowableRenderer();

	@Override
	public String[] doRender(Throwable t) {
		String[] stackElems = target.doRender(t);

		Throwable[] throwables = ExceptionUtils.getThrowables(t);
		for (int i = throwables.length - 1; i >= 0; i--) {
			t = throwables[i];
			if (PEContextAwareException.class.isAssignableFrom(t.getClass())) {
				PEContext ctx = ((PEContextAwareException) t).getContext();
				if (ctx == null || ctx.isEmpty()) {
					continue;
				} else if (!ctx.isEnabled()) {
					break;
				}
				String text = ctx.toFormattedString();
				String[] contextElems = text.split("\\n");
				return (String[]) ArrayUtils.addAll(stackElems, contextElems);
			}
		}
		return stackElems;
	}

}
