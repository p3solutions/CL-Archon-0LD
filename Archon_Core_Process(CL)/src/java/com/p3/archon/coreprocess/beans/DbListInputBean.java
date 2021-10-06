package com.p3.archon.coreprocess.beans;

import org.kohsuke.args4j.Option;

public class DbListInputBean {

	@Option(name = "-jid", aliases = {
			"--jobid" }, usage = "Job for the job", required = true)
	public String jobId;

	@Option(name = "-rid", aliases = { "--runId" }, usage = "Run id for the job", required = true)
	public int runId;

	@Option(name = "-ja", aliases = { "--jobattempt" }, usage = "Attempts taken", required = true)
	public int jobAttempt;

}
