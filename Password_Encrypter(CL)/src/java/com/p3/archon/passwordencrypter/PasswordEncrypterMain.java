package com.p3.archon.passwordencrypter;

import java.util.Date;

import com.p3.archon.securitymodule.SecurityModule;

public class PasswordEncrypterMain {
	public static void main(String[] args) {

//		args = new String[] { "secret" };

		if (args.length == 0) {
			System.err.println("No arguments specified.\nTerminating ... ");
			System.out.println("Job Terminated = " + new Date());
			System.exit(1);
		}

		String p = SecurityModule.perfromEncrypt("5nPqgr3erA5K1P05xtuknA==", "fgqLxNs8tyzrW7XgQOIb7w==", args[0]);
		System.out.println(p);

		System.exit(0);

	}

}
