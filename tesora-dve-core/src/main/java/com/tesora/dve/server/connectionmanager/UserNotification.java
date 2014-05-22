package com.tesora.dve.server.connectionmanager;

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

import java.util.Calendar;

public class UserNotification {
	
	public enum NotificationType {
		SiteFailover,
		SiteFailure
	};
	
	final Calendar eventDate = Calendar.getInstance();
	final String eventMessage;
	final NotificationType eventType;
	
	public UserNotification(NotificationType type, String eventMessage) {
		this.eventType = type;
		this.eventMessage = eventMessage;
	}

	public String getNotificationMessage() {
		return eventMessage;
	}
}
