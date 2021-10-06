package com.p3.archon.coc.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.UUID;

import com.p3.archon.coc.beans.InputBean;
import com.p3.archon.coc.utils.FileUtil;
import com.p3.archon.coc.utils.OSIdentifier;

public class ScriptRunner {

	String path;
	InputBean inputArgs;
	Writer out;
	String runScriptPath;

	public ScriptRunner(String path, InputBean inputArgs) throws IOException {
		this.path = path;
		this.inputArgs = inputArgs;
		runScriptPath = FileUtil.createTempJobScheuleFolder(inputArgs.reportId) + "coc_RunScript_"
				+ UUID.randomUUID().toString().substring(0, 8) + (OSIdentifier.checkOS() ? ".bat" : "");
		out = new FileWriter(runScriptPath, false);
	}

	public String runJob() throws IOException {
		if (OSIdentifier.checkOS()) {
			write("@echo off");
			write("call " + ((inputArgs.infoarchivePath + "\\bin\\iashell").replace("\\", File.separator)) + " --cmdfile " + path
					+ " --cmdfile_echo");
		} else {
			write("#!/bin/sh");
			write(((inputArgs.infoarchivePath + "\\bin\\iashell").replace("\\", File.separator)) + " --cmdfile " + path
					+ " --cmdfile_echo");
		}
		out.flush();
		out.close();
		return runScriptPath;
	}

	private void write(String string) throws IOException {
		out.write(string);
		out.write("\n");
	}
}
