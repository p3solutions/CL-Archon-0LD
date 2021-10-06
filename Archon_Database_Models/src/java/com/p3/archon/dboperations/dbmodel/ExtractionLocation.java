package com.p3.archon.dboperations.dbmodel;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;

@SuppressWarnings("deprecation")
@Entity
@Table(name = "EXTRACTION_LOCATION", indexes = { 
		@Index(name = "extraction_location_index1", columnList = "JOB_ID"),
		@Index(name = "extraction_location_index2", columnList = "RUN_ID"),
		@Index(name = "extraction_location_index3", columnList = "JOB_ATTEMPT") })
@org.hibernate.annotations.Entity(dynamicUpdate = true)
public class ExtractionLocation {

	@EmbeddedId
	RowId rowId;

	@Column(name = "OUTPUT_LOCATION", nullable = true, columnDefinition = "varchar(1000) NULL")
	String loc;

	public RowId getRowId() {
		return rowId;
	}

	public void setRowId(RowId rowId) {
		this.rowId = rowId;
	}

	public String getLoc() {
		return loc;
	}

	public void setLoc(String loc) {
		this.loc = loc;
	}
}
