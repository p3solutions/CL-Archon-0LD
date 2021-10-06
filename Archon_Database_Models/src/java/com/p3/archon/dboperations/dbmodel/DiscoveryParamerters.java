package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

import com.p3.archon.dboperations.dbmodel.enums.ParameterType;
import com.p3.archon.dboperations.dbmodel.enums.ParameterProfile;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "DISCOVERY_PARAMERTERS")
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class DiscoveryParamerters {

	@Id
	@GeneratedValue(generator = "UUID")
	@GenericGenerator(name = "UUID", strategy = "org.hibernate.id.UUIDGenerator")
	@Column(name = "SESSION_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	private String sessonId;

	@Column(name = "USER_ID", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String userId;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "PARAMERTER_PROFILE", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	ParameterProfile ParamerterProfile;

	@Enumerated(EnumType.ORDINAL)
	@Column(name = "PARAMERTER_TYPE", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	ParameterType type;

	@Column(name = "DESCRIPTION", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String description;

	@Column(name = "DEFINITION", nullable = true, columnDefinition = "json DEFAULT NULL")
	String definition;

	@Column(name = "SESSISON_NAME", nullable = false, columnDefinition = "varchar(255) NOT NULL")
	String sessionName;

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

	public String getSessonId() {
		return sessonId;
	}

	public void setSessonId(String sessonId) {
		this.sessonId = sessonId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public ParameterProfile getParamerterProfile() {
		return ParamerterProfile;
	}

	public void setParamerterProfile(ParameterProfile paramerterProfile) {
		ParamerterProfile = paramerterProfile;
	}

	public ParameterType getType() {
		return type;
	}

	public void setType(ParameterType type) {
		this.type = type;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getSessionName() {
		return sessionName;
	}

	public void setSessionName(String sessionName) {
		this.sessionName = sessionName;
	}

}
