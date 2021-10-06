package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import com.opentext.ia.sdk.sip.DigitalObject;
import com.opentext.ia.sdk.sip.DigitalObjectsExtraction;
import com.p3.archon.coreprocess.beans.RecordData;

public class FilesToDigitalObjects implements DigitalObjectsExtraction<RecordData> {
	String attachmentLoc;

	FilesToDigitalObjects(String attachmentLoc) {
		this.attachmentLoc = attachmentLoc;
	}

	@Override
	public Iterator<? extends DigitalObject> apply(RecordData recordData) {
		return Arrays.stream(getData(recordData)).iterator();
	}

	private DigitalObject[] getData(RecordData recordData) {
		DigitalObject[] digObj = new DigitalObject[recordData.getAttachements().size()];
		int i = 0;
		for (String fileitem : recordData.getAttachements()) {
			fileitem = attachmentLoc + File.separator + fileitem;
			fileitem = fileitem.replace("/", File.separator);
			File file = new File(fileitem);
			digObj[i++] = (DigitalObject.fromFile(file.getName(), file));
		}
		return digObj;
	}
}
