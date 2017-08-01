package org.owen.parser;

import org.apache.log4j.Logger;
import org.owen.helper.UtilHelper;

public class ParsingEngine {

	public void startParsing(String jdPath) {
		Logger.getLogger(ParsingEngine.class).debug("Starting python process for " + jdPath);
		runPythonScript("python " + UtilHelper.getConfigProperty("python_script_path") + "\\socgen_" + jdPath + ".py");
		Logger.getLogger(ParsingEngine.class).debug("Ending python process for " + jdPath);

	}

	private void runPythonScript(final String scriptPath) {
		Runnable task = new Runnable() {

			@Override
			public void run() {
				try {
					Process p = Runtime.getRuntime().exec(scriptPath);
					p.waitFor();
					p.destroy();
					org.apache.log4j.Logger.getLogger(ParsingEngine.class).debug("Python script execution completed");
				} catch (Exception e) {
					Logger.getLogger(ParsingEngine.class).error("Exception while starting the parsing", e);
				}
			}

		};
		new Thread(task, "ServiceThread").start();

	}
}
