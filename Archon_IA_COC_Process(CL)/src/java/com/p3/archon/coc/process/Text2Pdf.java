package com.p3.archon.coc.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Font.FontFamily;
import com.itextpdf.text.Image;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;
import com.p3.archon.coc.utils.FileUtil;

public class Text2Pdf {

	private static final Rectangle Rectangle = new Rectangle(900, 1200);

	@SuppressWarnings("deprecation")
	public void createPdf(String yourreport, String TEXT, String appName, String reportId)
			throws DocumentException, IOException {
		float left = 30;
		float right = 30;
		float top = 30;
		float bottom = 30;

		Document document = new Document(Rectangle, left, right, top, bottom);
		String tempReport = yourreport + "temp";
		File dest = new File(tempReport);
		System.out.println(dest);
		dest.createNewFile(); // if file already exists will do nothing

		FileOutputStream oFile = new FileOutputStream(dest, false);
		PdfWriter.getInstance(document, oFile);
		document.open();
		BufferedReader br = new BufferedReader(new FileReader(TEXT));
		String line;
		Paragraph p;
		Font normal = new Font(FontFamily.COURIER, 10);
		Font verynormal = new Font(FontFamily.TIMES_ROMAN, 10);
		Font bold = new Font(FontFamily.COURIER, 10, Font.BOLD);
		Font boldTitle = new Font(FontFamily.COURIER, 14, Font.BOLD);
		boolean title = false;

		p = new Paragraph("CHAIN OF CUSTODY REPORT (" + appName + ")", boldTitle);
		p.setAlignment(Element.ALIGN_CENTER);
		document.add(p);

		p = new Paragraph("\n", title ? bold : normal);
		document.add(p);

		while ((line = br.readLine()) != null) {
			if (line.isEmpty()) {
				p = new Paragraph("\n", title ? bold : normal);
				title = line.isEmpty();
				document.add(p);
			} else {
				p = new Paragraph(line, title ? bold : normal);
				p.setAlignment(Element.ALIGN_LEFT);
				title = line.isEmpty();
				document.add(p);
			}
		}
		br.close();

		p = new Paragraph("\n", title ? bold : normal);
		document.add(p);
		document.add(p);
		document.add(p);

		p = new Paragraph("Job Reference Id : " + reportId, verynormal);
		document.add(p);

		p = new Paragraph("Generated Date : " + new Date().toGMTString(), verynormal);
		document.add(p);

		p = new Paragraph("This is a system generated report.", verynormal);
		document.add(p);

		document.close();

		FileInputStream is = new FileInputStream(tempReport);
		PdfReader pdfReader = new PdfReader(is);
		FileOutputStream fileOutputStream = new FileOutputStream(new File(yourreport));
		PdfStamper pdfStamper = new PdfStamper(pdfReader, fileOutputStream);
		Image image = Image.getInstance("archon.png");
		image.scaleToFit(300f, 300f);
		image.setAbsolutePosition(600f, 20f);
		for (int i = 1; i <= pdfReader.getNumberOfPages(); i++) {
			PdfContentByte content = pdfStamper.getUnderContent(i);
			content.addImage(image);
		}
		pdfStamper.close();
		pdfReader.close();
		fileOutputStream.close();
		is.close();
		
		FileUtil.deleteFile(tempReport);

	}

}
