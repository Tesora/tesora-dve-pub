package com.tesora.dve.errmap;

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

public final class InternalErrors {

	public static final OneParamErrorCodeFormatter<String> internalFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.INTERNAL, "Internal error: %s", 99, "99999");
	public static final TwoParamErrorCodeFormatter<String, String> invalidDiscriminantUpdateFormatter =
			new TwoParamErrorCodeFormatter<String, String>(AvailableErrors.INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE,
					"Invalid update: discriminant column '%s' of container base table '%s' cannot be updated",
					6000,
					"DVECO");
	public static final OneParamErrorCodeFormatter<String> invalidContainerDeleteFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.INVALID_CONTAINER_DELETE,
					"Invalid delete on container base table '%s'.  Not restricted by discriminant columns",
					6001,
					"DVECO");
	public static final OneParamErrorCodeFormatter<String> noUniqueKeyOnTriggerTableFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.NO_UNIQUE_KEY_ON_TRG_TARGET,
					"The target table '%s' must have a unique key.",
					6002,
					"DVETR");
	
	public static final ErrorCodeFormatter[] messages = new ErrorCodeFormatter[] {
		internalFormatter,
		invalidDiscriminantUpdateFormatter,
		invalidContainerDeleteFormatter,
		noUniqueKeyOnTriggerTableFormatter
	};

}
