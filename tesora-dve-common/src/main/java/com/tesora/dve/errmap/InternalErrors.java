package com.tesora.dve.errmap;

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
