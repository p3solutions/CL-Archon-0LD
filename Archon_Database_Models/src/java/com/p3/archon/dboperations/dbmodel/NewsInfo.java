package com.p3.archon.dboperations.dbmodel;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "NEWS_INFO", indexes = { @Index(name = "news_index", columnList = "ID") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class NewsInfo {

	@Id
	@GenericGenerator(name = "gen", strategy = "increment")
	@GeneratedValue(generator = "gen")
	@Column(name = "ID", unique = true, nullable = false, precision = 15, scale = 0)
	int id;

	@Column(name = "TYPE", nullable = false, columnDefinition = "varchar(20)")
	String type;

	@Column(name = "INFO", nullable = false, columnDefinition = "varchar(500)")
	String info;

	@Column(name = "CREATED_DATE", nullable = true, columnDefinition = "datetime(6) DEFAULT NULL")
	Timestamp addedDate;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public Timestamp getAddedDate() {
		return addedDate;
	}

	public void setAddedDate(Timestamp addedDate) {
		this.addedDate = addedDate;
	}

}
