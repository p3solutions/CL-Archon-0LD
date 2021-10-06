package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "FFG_REPLACEMENT_CHARACTER")
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class FFGReplacementCharacter {

	@Id
	@Column(name = "ID", unique = true, nullable = false, columnDefinition = "int(1) NOT NULL")
	Integer id;

	@Column(name = "REPLACEMENT_MAP", nullable = true, columnDefinition = "varchar(14000) DEFAULT NULL")
	String replacementMap;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getReplacementMap() {
		return replacementMap;
	}

	public void setReplacementMap(String replacementMap) {
		this.replacementMap = replacementMap;
	}

}
