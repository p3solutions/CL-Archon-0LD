package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "APP_CONFIG_PARAMS", indexes = { @Index(name = "app_config_params_index", columnList = "PARAM") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class AppConfigSetting {

	@Id
	@Column(name = "PARAM", nullable = false, columnDefinition = "varchar(100) NOT NULL")
	String parameter;

	@Column(name = "VALUE", nullable = false, columnDefinition = "varchar(100) NOT NULL")
	String value;

	public AppConfigSetting() {

	}

	public AppConfigSetting(String param, int value) {
		setParameter(param);
		setValue(Integer.toString(value));
	}

	public AppConfigSetting(String param, boolean value) {
		setParameter(param);
		setValue(Boolean.toString(value));
	}

	public String getParameter() {
		return parameter;
	}

	public void setParameter(String parameter) {
		this.parameter = parameter;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
