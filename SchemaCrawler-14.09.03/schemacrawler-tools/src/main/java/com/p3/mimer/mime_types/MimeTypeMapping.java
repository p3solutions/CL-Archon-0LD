package com.p3.mimer.mime_types;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Provides mapping between mime types and file extensions.
 * 
 * @author metamaker (Dmytro Yurchenko)
 * @author www.binaryspaceship.com
 * @version 1.0.0
 */
public class MimeTypeMapping {
	/**
	 * Get mime type that matches filename extension.
	 * 
	 * For example, {@code extensionToMimeType("jpeg")} will result in "image/jpeg".
	 * 
	 * @param extension
	 *            filename extension to find mime type for
	 * @return mime type, if there exists known mime type for specified extension or
	 *         {@code null} if no mime type is known
	 */
	public static String extensionToMimeType(String extension) {
		return fileExtensionToMimeType.get(extension);
	}

	/**
	 * Get filename extension that matches mime type.
	 * 
	 * For example, {@code mimeTypeToExtension("image/jpeg")} will result in "jpeg"
	 * without dot symbol before extension.
	 * 
	 * @param mimetype
	 *            mime type to find filename extension for
	 * @return filename extension, if there exists known extension for specified
	 *         mime type or {@code null} if no extension is known
	 */
	public static String mimeTypeToExtension(String mimetype) {
		String type = mimeTypeToFileExtension.get(mimetype);
		return (type == null?".unknown":type.equals("[blank]")?"":"."+type);
	}

	/**
	 * This is utility class with static methods. It must not be instantiated.
	 */
	private MimeTypeMapping() {
	}

	/*
	 * ====================================================
	 * 
	 * Below go implementation details for current class.
	 * 
	 * ====================================================
	 */

	/*
	 * Hashmaps with registered extensions and mime types.
	 */
	private static HashMap<String, String> fileExtensionToMimeType;
	private static HashMap<String, String> mimeTypeToFileExtension;

	static {
		/*
		 * Initialize extensions and mime types hashmaps with values from mime.types
		 * resource.
		 */

		String[] lines = readMimeTypeLines();

		final int EXPECTED_MIMETYPES_COUNT = lines.length;
		fileExtensionToMimeType = new HashMap<String, String>(EXPECTED_MIMETYPES_COUNT);
		mimeTypeToFileExtension = new HashMap<String, String>(EXPECTED_MIMETYPES_COUNT);

		for (String line : lines) {
			registerMimeTypeLine(line);
		}
	}

	/**
	 * Get text of mime.types file as array of strings.
	 * 
	 * mime.types file must packaged as resource. It is not read from filesystem.
	 * 
	 * @return Array of lines from mime.types text file
	 */
	private static String[] readMimeTypeLines() {
		InputStream mimetypesStream = MimeTypeMapping.class.getResourceAsStream("/mime.types");

		Scanner s = new Scanner(mimetypesStream);
		s.useDelimiter("\\A");
		String body = s.hasNext() ? s.next() : "";
		s.close();

		return body.split("\n");
	}

	/**
	 * Split single lime from mime.types file and register its values in hashmaps.
	 * 
	 * @param line
	 */
	private static void registerMimeTypeLine(String line) {
		line = line.trim();

		if (line.equals("")) {
			// Skip empty lines
			return;
		}

		if (line.charAt(0) == '#') {
			// Skip comments
			return;
		}

		String[] parts = line.split("\\s+");

		if (parts.length < 2) {
			// Skip lines without mimetype and file extension association
			return;
		}

		String mimetype = parts[0];
		String[] extensions = Arrays.copyOfRange(parts, 1, parts.length);

		register(mimetype, extensions);
	}

	/**
	 * Register mime type and associated filename extensions in hashmaps.
	 * 
	 * @param mimetype
	 * @param extensions
	 */
	private static void register(String mimetype, String extensions[]) {
		for (String extension : extensions) {
			if (!fileExtensionToMimeType.containsKey(extension)) {
				fileExtensionToMimeType.put(extension, mimetype);
			}

			if (!mimeTypeToFileExtension.containsKey(mimetype)) {
				mimeTypeToFileExtension.put(mimetype, extension);
			}
		}
	}
}