package com.p3.archon.dboperations.dbmodel.enums;

public enum CAAState {

	NA("Not Applicable"), SCHEDULED("Scheduled"), ANALYSIS("Analysis"), EXTRACTION("Extraction"), SCREEN(
			"Screen Generation"), COMPLETE(
					"Complete"), READY("Ready for extraction Phase"), ABORT("Fatel Error. Cannot Proceed"), CANCELLED(
							"User Cancelled"), PREPARING("Preparing inputs"), EXTRACTION_SCREEN(
									"Extraction and Screen Generation"), SCHEDULED_PHASE1(
											"Scheduled for Validation"), SCHEDULED_PHASE2(
													"Scheduled for Extraction"), SCHEDULED_TO_STOP("Scheduled to stop");
	;
	;

	private String value;

	CAAState(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
