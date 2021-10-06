package com.p3.archon.coreprocess.executables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xerces.util.XMLChar;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.opentext.ia.sdk.sip.BatchSipAssembler;
import com.opentext.ia.sdk.sip.DefaultPackagingInformationFactory;
import com.opentext.ia.sdk.sip.OneSipPerDssPackagingInformationFactory;
import com.opentext.ia.sdk.sip.PackagingInformation;
import com.opentext.ia.sdk.sip.PackagingInformationFactory;
import com.opentext.ia.sdk.sip.PdiAssembler;
import com.opentext.ia.sdk.sip.SequentialDssIdSupplier;
import com.opentext.ia.sdk.sip.SipAssembler;
import com.opentext.ia.sdk.sip.SipSegmentationStrategy;
import com.opentext.ia.sdk.sip.TemplatePdiAssembler;
import com.opentext.ia.sdk.support.io.FileSupplier;
import com.opentext.ia.sip.assembly.stringtemplate.StringTemplate;
import com.p3.archon.coreprocess.beans.ArchonInputBean;
import com.p3.archon.coreprocess.beans.RecordData;
import com.p3.archon.coreprocess.common.Constants;
import com.p3.archon.coreprocess.validations.Validations;
import com.p3.archon.utils.FileUtil;

public class SipPackager {

	private static final boolean GENERATE_ATTACHEMENTS = true;
	private static final String BLOB_PREFIX = "BLOBS_";
	private static final String DEFAULT_PRODUCER = "Archon";
	private static final String ENCODING = "UTF-8";

	boolean version;
	String title;
	String schema;
	String outputPath;
	ArchonInputBean inputArgs;
	LinkedHashMap<String, Object[]> dataTypeMap;

	boolean hasblob = false;
	BatchSipAssembler<RecordData> batchAssembler;

	public SipPackager(String schema, String title, String outputPath, ArchonInputBean inputArgs, boolean version,
			LinkedHashMap<String, Object[]> dataTypeMap) {
		this.dataTypeMap = dataTypeMap;
		this.schema = schema;
		this.title = title;
		outputPath = outputPath.endsWith(File.separator) ? outputPath : outputPath + File.separator;
		this.outputPath = outputPath.endsWith(File.separator) ? outputPath.substring(0, outputPath.length() - 1)
				: outputPath;
		this.inputArgs = inputArgs;
		this.version = version;
	}

	public String generateNameSpace(String holding) {
		return URI.create("urn:x-emc:ia:schema:" + holding.toLowerCase() + ":1.0").toString();
	}

	public void generateSip() throws Exception {
		String formattedSchema = getTextFormatted(schema);
		String formattedTable = getTextFormatted(title);
		String appName = inputArgs.getApplicationName();
		String holding = inputArgs.getHoldingPrefix().replace(" ", "") + "_" + formattedSchema + "_" + formattedTable;
		String namespace = generateNameSpace(holding);

		PackagingInformation prototype = PackagingInformation.builder().dss().application(appName).holding(holding)
				.producer(DEFAULT_PRODUCER).entity(holding).schema(namespace).end().build();

		PackagingInformationFactory factory = new OneSipPerDssPackagingInformationFactory(
				new DefaultPackagingInformationFactory(prototype), new SequentialDssIdSupplier("ex6dss", 1));
		String sipHeader = "<" + formattedTable + " xmlns=\"" + namespace + "\">\n";
		String sipFooter = "\n" + "</" + formattedTable + ">\n";
		PdiAssembler<RecordData> pdiAssembler = new TemplatePdiAssembler<>(
				new StringTemplate<>(sipHeader, sipFooter, "$model.data$\n"));
		SipAssembler<RecordData> sipAssembler = SipAssembler.forPdiAndContent(factory, pdiAssembler,
				new FilesToDigitalObjects(outputPath));
		batchAssembler = new BatchSipAssembler<>(sipAssembler,
				SipSegmentationStrategy.byMaxSipSize(Constants.MAX_RECORD_PER_XML_FILE),
				FileSupplier.fromDirectory(new File(outputPath), "SIP-" + holding.toLowerCase() + "-", ".zip"));

		Set<String> fileNamesList = new TreeSet<String>();
		Files.newDirectoryStream(Paths.get(outputPath), path -> path.toString().startsWith(getFilterName()))
				.forEach(filePath -> fileNamesList.add(filePath.toString()));

		for (String file : fileNamesList) {
			covertXmlToSip(file, formattedSchema, formattedTable, version);
		}

		for (String file : fileNamesList) {
			FileUtil.deleteFile(file);
			String blobFolder = outputPath + File.separator + "BLOBs" + File.separator
					+ Validations.checkValidFolder(title.toUpperCase());
			if (FileUtil.checkForDirectory(blobFolder)) {
				FileUtil.deleteDirectory(blobFolder);
			}
		}

		batchAssembler.end();
		generatePdi(formattedSchema, formattedTable, appName, holding, namespace);
	}

	private void generatePdi(String formattedSchema, String formattedTable, String appName, String holding,
			String namespace) throws IOException {

		String pdipath = outputPath + File.separator + "PDIs";
		File path = new File(pdipath);
		if (!path.exists())
			path.mkdirs();

		Writer out = null;
		out = new OutputStreamWriter(
				new FileOutputStream(path.getAbsolutePath() + File.separator + "pdi-schema_" + holding + ".xsd"),
				ENCODING);

		out.write("<?xml version=\"1.0\" encoding=\"");
		out.write(ENCODING);
		out.write("\"?>\n");

		out.write(getTabSpace(level++)
				+ "<xs:schema attributeFormDefault=\"unqualified\" elementFormDefault=\"qualified\" ");
		out.write("targetNamespace=\"");
		out.write(namespace + "\" ");
		out.write("xmlns=\"");
		out.write(namespace + "\" ");
		out.write("xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"");
		out.write(">");
		out.write(getTabSpace(level++) + "<xs:element name=\"" + formattedTable + "\">");
		out.write(getTabSpace(level++) + "<xs:complexType>");
		out.write(getTabSpace(level++) + "<xs:sequence>");
		out.write(getTabSpace(level++) + "<xs:element maxOccurs=\"unbounded\" minOccurs=\"0\" name=\"" + "ROW\">");
		out.write(getTabSpace(level++) + "<xs:complexType>");
		out.write(getTabSpace(level++) + "<xs:sequence>");

		for (Entry<String, Object[]> column : dataTypeMap.entrySet()) {
			String columnName = column.getKey();

			String columnType = "xs:string";
			try {
				columnType = getColumnType(((String) column.getValue()[0]), ((Long) column.getValue()[2]),
						((Integer) column.getValue()[3]));
			} catch (Exception e) {
				e.printStackTrace();
				columnType = "xs:string";
			}

			String coltype = ((String) column.getValue()[0]).toUpperCase();

			int columnTypeCategory = 0;
			try {
				columnTypeCategory = ((int) column.getValue()[1]);
			} catch (Exception e) {
				columnTypeCategory = 0;
			}

			out.write(getTabSpace(level++) + "<xs:element name=\"" + columnName + "\" minOccurs=\"0\">");
			out.write(getTabSpace(level++) + "<xs:complexType>");
			out.write(getTabSpace(level++) + "<xs:simpleContent>");
			if (Constants.SHOW_DATETIME && coltype.equalsIgnoreCase("DATE"))
				out.write(getTabSpace(level++) + "<xs:extension base=\"xs:dateTime\">");
			else
				out.write(getTabSpace(level++) + "<xs:extension base=\"" + columnType + "\">");
			if (columnTypeCategory == 0) {
				out.write(getTabSpace(level) + "<xs:attribute name=\"null\" type=\"xs:boolean\" use=\"optional\" />");
			}
			if (columnTypeCategory == 1) {
				out.write(getTabSpace(level) + "<xs:attribute name=\"null\" type=\"xs:boolean\" use=\"optional\" />");
				out.write(getTabSpace(level) + "<xs:attribute name=\"type\" type=\"xs:string\" use=\"optional\" />");
			}
			if (columnTypeCategory == 2) {
				hasblob = true;
				out.write(getTabSpace(level) + "<xs:attribute name=\"null\" type=\"xs:boolean\" use=\"optional\" />");
				out.write(getTabSpace(level) + "<xs:attribute name=\"ref\" type=\"xs:string\" use=\"optional\" />");
				out.write(getTabSpace(level) + "<xs:attribute name=\"status\" type=\"xs:string\" use=\"optional\" />");
				out.write(getTabSpace(level) + "<xs:attribute name=\"size\" type=\"xs:string\" use=\"optional\" />");
				out.write(getTabSpace(level) + "<xs:attribute name=\"type\" type=\"xs:string\" use=\"optional\" />");
			}
			level--;
			out.write(getTabSpace(level--) + "</xs:extension>");
			out.write(getTabSpace(level--) + "</xs:simpleContent>");
			out.write(getTabSpace(level--) + "</xs:complexType>");
			out.write(getTabSpace(level) + "</xs:element>");

			if ((coltype.contains("DATETIME") || coltype.contains("TIMESTAMP"))) {
				if (Constants.SPLITDATE) {
					out.write(getTabSpace(level++) + "<xs:element name=\"" + columnName + "_DT_SPLIT" + "\">");
					out.write(getTabSpace(level++) + "<xs:complexType>");
					out.write(getTabSpace(level++) + "<xs:simpleContent>");
					out.write(getTabSpace(level++) + "<xs:extension base=\"" + "xs:date" + "\">");
					out.write(getTabSpace(level)
							+ "<xs:attribute name=\"createdBy\" type=\"xs:string\" use=\"optional\" />");
					level--;
					out.write(getTabSpace(level--) + "</xs:extension>");
					out.write(getTabSpace(level--) + "</xs:simpleContent>");
					out.write(getTabSpace(level--) + "</xs:complexType>");
					out.write(getTabSpace(level) + "</xs:element>");

					out.write(getTabSpace(level++) + "<xs:element name=\"" + columnName + "_TM_SPLIT" + "\">");
					out.write(getTabSpace(level++) + "<xs:complexType>");
					out.write(getTabSpace(level++) + "<xs:simpleContent>");
					out.write(getTabSpace(level++) + "<xs:extension base=\"" + "xs:time" + "\">");
					out.write(getTabSpace(level)
							+ "<xs:attribute name=\"createdBy\" type=\"xs:string\" use=\"optional\" />");
					level--;
					out.write(getTabSpace(level--) + "</xs:extension>");
					out.write(getTabSpace(level--) + "</xs:simpleContent>");
					out.write(getTabSpace(level--) + "</xs:complexType>");
					out.write(getTabSpace(level) + "</xs:element>");

					out.write(
							getTabSpace(level++) + "<xs:element name=\"" + columnName + "_TM_SPLIT_FORMATTED" + "\">");
					out.write(getTabSpace(level++) + "<xs:complexType>");
					out.write(getTabSpace(level++) + "<xs:simpleContent>");
					out.write(getTabSpace(level++) + "<xs:extension base=\"" + "xs:time" + "\">");
					out.write(getTabSpace(level)
							+ "<xs:attribute name=\"createdBy\" type=\"xs:string\" use=\"optional\" />");
					level--;
					out.write(getTabSpace(level--) + "</xs:extension>");
					out.write(getTabSpace(level--) + "</xs:simpleContent>");
					out.write(getTabSpace(level--) + "</xs:complexType>");
					out.write(getTabSpace(level) + "</xs:element>");

					out.write(getTabSpace(level++) + "<xs:element name=\"" + columnName + "_DTM_SPLIT" + "\">");
					out.write(getTabSpace(level++) + "<xs:complexType>");
					out.write(getTabSpace(level++) + "<xs:simpleContent>");
					out.write(getTabSpace(level++) + "<xs:extension base=\"" + "xs:dateTime" + "\">");
					out.write(getTabSpace(level)
							+ "<xs:attribute name=\"createdBy\" type=\"xs:string\" use=\"optional\" />");
					level--;
					out.write(getTabSpace(level--) + "</xs:extension>");
					out.write(getTabSpace(level--) + "</xs:simpleContent>");
					out.write(getTabSpace(level--) + "</xs:complexType>");
					out.write(getTabSpace(level) + "</xs:element>");
				}
			}
		}

		if (GENERATE_ATTACHEMENTS && hasblob) {
			out.write(getTabSpace(level++) + "<xs:element name=\"" + BLOB_PREFIX + formattedTable + "_ATTACHEMENTS\">");
			out.write(getTabSpace(level++) + "<xs:complexType>");
			out.write(getTabSpace(level++) + "<xs:sequence>");

			out.write(getTabSpace(level++) + "<xs:element maxOccurs=\"unbounded\" minOccurs=\"0\" name=\""
					+ "ATTACHEMENT" + "\">");
			out.write(getTabSpace(level++) + "<xs:complexType>");
			out.write(getTabSpace(level++) + "<xs:simpleContent>");
			out.write(getTabSpace(level++) + "<xs:extension base=\"" + "xs:string" + "\">");
			out.write(getTabSpace(level) + "<xs:attribute name=\"column\" type=\"xs:string\" use=\"optional\" />");
			level--;
			out.write(getTabSpace(level--) + "</xs:extension>");
			out.write(getTabSpace(level--) + "</xs:simpleContent>");
			out.write(getTabSpace(level--) + "</xs:complexType>");
			out.write(getTabSpace(level--) + "</xs:element>");
			out.write(getTabSpace(level--) + "</xs:sequence>");
			out.write(getTabSpace(level--) + "</xs:complexType>");
			out.write(getTabSpace(level) + "</xs:element>");
		}
		level--;
		out.write(getTabSpace(level) + "</xs:sequence>");
		out.write(getTabSpace(level--) + "<xs:attribute type=\"xs:int\" name=\"REC_ID\" use=\"optional\"/>");
		out.write(getTabSpace(level--) + "</xs:complexType>");
		out.write(getTabSpace(level--) + "</xs:element>");
		out.write(getTabSpace(level--) + "</xs:sequence>");
		out.write(getTabSpace(level--) + "</xs:complexType>");
		out.write(getTabSpace(level--) + "</xs:element>");
		out.write("\n</xs:schema>");
		out.flush();
		out.close();
		System.out.println("pdi schema file created at " + path.getAbsolutePath() + File.separator + "pdi-schema_"
				+ holding + ".xsd" + "\n");

	}

	private String getColumnType(String colType, long size, long decimalDigits) {
		switch (colType.toUpperCase()) {
		case "CHAR":
		case "VARCHAR":
		case "TEXT":
		case "TINYTEXT":
		case "MEDIUMTEXT":
		case "LONGTEXT":
		case "USERDEFINED":
			return "xs:string";
		case "INTEGER":
		case "INT":
		case "AUTONUMBER":
		case "SMALLINT":
		case "BIGINT":
		case "TINYINT":
			return "xs:int";
		case "LONG":
		case "LONGVARCHAR":
			return "xs:long";
		case "NUMERIC":
		case "NUMBER":
			if (decimalDigits > 0)
				return "xs:double";
			else if (size > 8)
				return "xs:long";
			else
				return "xs:int";
		case "DOUBLE":
		case "DECIMAL":
		case "MONEY":
		case "DEC":
		case "SMALLMONEY":
		case "BIGMONEY":
		case "CURRENCY":
			return "xs:double";
		case "FLOAT":
		case "REAL":
			return "xs:float";
		case "DATE":
			return "xs:date";
		case "TIME":
			return "xs:time";
		case "DATETIME":
		case "TIMESTAMP":
		case "TIMESTAMP_WITH_TIMEZONE":
		case "TIMESTAMP WITH LOCAL TIME ZONE":
		case "SMALLDATETIME":
			return "xs:string";
		default:
			return "xs:string";
		}
	}

	private int level = 0;
	private int i = 0;
	private String tabSpace = "\n";

	private String getTabSpace(int tabSize) {
		if (tabSize < 0)
			return "";
		i = 0;
		tabSpace = "\n";
		while (i++ != tabSize) {
			tabSpace += "\t";
		}
		return tabSpace;
	}

	protected String dataXmlCompitible(String data) {
		if (data == null)
			return "";
		return data.replace("&", "&amp;").replace(">", "&gt;").replace("<", "&lt;").replace("'", "&apos;").replace("\"",
				"&quot;");
	}

	protected String stripNonValidXMLCharacters(String in) {
		if (in == null || ("".equals(in)))
			return null;
		StringBuffer out = new StringBuffer();
		for (int i = 0; i < in.length(); i++) {
			char c = in.charAt(i);
			if (XMLChar.isValid(c))
				out.append(c);
			else
				out.append("");
		}
		return out.toString();
	}

	private String writeXmlDocumentToXmlFile(Document xmlDocument) throws TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer;

		transformer = tf.newTransformer();

		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(xmlDocument), new StreamResult(writer));
		String xmlString = writer.getBuffer().toString();
		return xmlString;
	}

	private static Document convertStringToXMLDocument(String xmlString)
			throws ParserConfigurationException, SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;

		builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xmlString)));
		return doc;

	}

	@SuppressWarnings("unchecked")
	private void covertXmlToSip(String fileItem, String schemaElement, String tableElement, boolean version)
			throws Exception {
		StringBuffer sb = new StringBuffer();
		String ref = null;
		String type = null;
		String status = null;
		boolean cdataflagstart = false;
		boolean cdataflagend = false;
		Map<String, String> attachments = new TreeMap<String, String>();
		String row = version ? "ROW" : tableElement + "-ROW";
		XMLInputFactory factory = XMLInputFactory.newInstance();
		InputStream file = new FileInputStream(fileItem);
		InputStreamReader isr = new InputStreamReader(file);
		XMLEventReader eventReader = factory.createXMLEventReader(isr);
		while (eventReader.hasNext()) {
			XMLEvent event = eventReader.nextEvent();
			switch (event.getEventType()) {
			case XMLStreamConstants.START_ELEMENT:
				String startElement = event.asStartElement().getName().getLocalPart();
				if (startElement.equals(row)) {
					sb = new StringBuffer();
					attachments.clear();
					sb.append("<ROW");
				} else
					sb.append("<").append(startElement);

				ref = null;
				type = null;
				status = null;
				Iterator<Attribute> attributes = event.asStartElement().getAttributes();
				while (attributes.hasNext()) {
					Attribute x = attributes.next();
					if (x.getName().toString().equals("ref"))
						ref = x.getValue();
					else if (x.getName().toString().equals("status"))
						status = x.getValue();
					else if (x.getName().toString().equals("type"))
						type = x.getValue();

					if (type != null && type.equals("BLOB"))
						hasblob = true;

					if (x.getName().toString().equals("type") && x.getValue().equals("CLOB"))
						cdataflagstart = true;

					sb.append(" ").append(x.getName()).append("=\"").append(dataXmlCompitible(x.getValue()))
							.append("\"");
				}
				if (ref != null && type != null && status != null && !ref.equals("") && type.equals("BLOB")
						&& status.equalsIgnoreCase("SUCCESS"))
					attachments.put(startElement, ref);
				sb.append(">");
				break;
			case XMLStreamConstants.CHARACTERS:
				Characters characters = event.asCharacters();
				if (cdataflagstart) {
					sb.append("<![CDATA[");
					cdataflagstart = false;
					cdataflagend = true;
				}
				if (cdataflagstart || cdataflagend)
					sb.append(characters.getData());
				else
					sb.append(dataXmlCompitible(characters.getData()));
				break;
			case XMLStreamConstants.END_ELEMENT:
				String endElement = event.asEndElement().getName().getLocalPart();
				if (cdataflagend) {
					sb.append("]]>");
				}
				cdataflagstart = false;
				cdataflagend = false;
				if (endElement.equals(row)) {
					if (GENERATE_ATTACHEMENTS && hasblob) {
						sb.append("\n\t\t<").append(BLOB_PREFIX + tableElement + "_ATTACHEMENTS").append(">");
						for (Entry<String, String> blobitem : attachments.entrySet()) {
							sb.append("\n\t\t\t<").append("ATTACHEMENT column=\"").append(blobitem.getKey())
									.append("\">");
							sb.append(getAttachments(blobitem.getValue()));
							sb.append("</ATTACHEMENT>");
						}
						sb.append("\n\t\t</").append(BLOB_PREFIX + tableElement + "_ATTACHEMENTS").append(">");
					}
					sb.append("</ROW>");
					String txt = writeXmlDocumentToXmlFile(
							convertStringToXMLDocument(stripNonValidXMLCharacters(sb.toString())));
					RecordData rec = new RecordData(txt, new ArrayList<String>(attachments.values()));
					batchAssembler.add(rec);
				} else
					sb.append("</").append(endElement).append(">");
				break;
			}
		}
		eventReader.close();
		isr.close();
		file.close();
	}

	private String getAttachments(String item) {
		if (item == null || item.equals(""))
			return "";
		return item.substring(item.lastIndexOf("/") + 1);
	}

	public static String getTextFormatted(String string) {
		string = string.trim().replaceAll("[^_^\\p{Alnum}.]", "_").replace("^", "_").replaceAll("\\s+", "_");
		string = ((string.startsWith("_") && string.endsWith("_") && string.length() > 2)
				? string.substring(1).substring(0, string.length() - 2)
				: string);
		return string.length() > 0 ? ((string.charAt(0) >= '0' && string.charAt(0) <= '9') ? "_" : "") + string
				: string;
	}

	private String getFilterName() {
		System.out.println(outputPath + File.separator + (version ? Validations.checkValidFile(schema.toUpperCase()) + "-" : "")
				+ Validations.checkValidFile(title.toUpperCase()) + "-");
		return outputPath + File.separator + (version ? Validations.checkValidFile(schema.toUpperCase()) + "-" : "")
				+ Validations.checkValidFile(title.toUpperCase()) + "-";
	}
}
