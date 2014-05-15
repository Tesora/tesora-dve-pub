// OS_STATUS: public
package com.tesora.dve.server.connectionmanager;

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
