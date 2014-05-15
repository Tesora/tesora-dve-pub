// OS_STATUS: public
package com.tesora.dve.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import jline.ConsoleReader;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PELogUtils;
import com.tesora.dve.exceptions.PEException;

public class CLIBuilder implements CLIBuilderCallback {

	private static final Logger logger = Logger.getLogger(CLIBuilder.class);

	private static final String CMD_NAME_PREFIX = "cmd";
	private static final String CMD_NAME_DELIMITER = "_";

	private static final Parameter[] ARGS = {
			new FlaggedOption("file", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'f', JSAP.NO_LONGFLAG,
					"File containing commands to execute."),
			new FlaggedOption("cmd", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', JSAP.NO_LONGFLAG,
					"A single command to execute and then exit."),
			new Switch("suppressWelcome", 'w', "suppressWelcome", "Suppress printing welcome banner."),
			new Switch("emulateReader", 'e', "emulateReader", "Emulate the console reader."),
			new Switch("version", 'v', "version", "Display product version and then exit.") };

	private final Map<String, CommandMapType> commandMapMap = new HashMap<String, CommandMapType>();
	private final CommandMapType globalCommandMap = createGlobalCommandMap();
	private CommandMapType currentCommandMap = null;

	private static final String DEFAULT_MAP = "default";
	private static final String GLOBAL_MAP = "global";
	private static final String DEFAULT_PROMPT = "> ";

	private String prompt = DEFAULT_PROMPT;
	private InputStream fileInputStream;
	private PrintStream printStream = System.out;
	private String singleCommand;
	private boolean suppressWelcome = false;
	private boolean displayVersion = false;
	private boolean emulateReader = false;
	private final String name;
	private boolean debugMode = false;
	private int returnCode = 0;
	private String question = null;
	private QuestionCallback questionCallback = null;

	protected static String buildToolBannerName(final String toolName) {
		return PEConstants.DVE_SERVER_VERSION_COMMENT.concat(" ").concat(toolName);
	}

	protected CLIBuilder(String[] args, String name) throws PEException {

		this.name = buildToolBannerName(name);

		logger.debug("Create CLIBuilder " + this.name);

		currentCommandMap = createCommandMap(DEFAULT_MAP);

		changeMode(currentCommandMap);

		if ((args != null) && (args.length > 0)) {
			try {
				final SimpleJSAP jsap = new SimpleJSAP(this.name, null, ARGS);
				final JSAPResult options = jsap.parse(args);

				if (jsap.messagePrinted()) {
					System.exit(0);
				}

				processArgs(options);
			} catch (final JSAPException e) {
				throw new PEException("Error parsing " + this.name + " command line parameters", e);
			}
		}
	}

	protected void setDebugMode(final boolean isDebug) {
		debugMode = isDebug;
	}

	public void start() throws Exception {
		if (displayVersion) {
			println(PELogUtils.getBuildVersionString(false));
			return; /* displayVersion option exits CLI */
		}

		if (!suppressWelcome) {
			println("Welcome to the " + name);
			println();
			println("Version : " + PEConstants.DVE_SERVER_VERSION_COMMENT + ", " + PELogUtils.getBuildVersionString(false));
			println();
			println(PEConstants.DVE_SERVER_COPYRIGHT_COMMENT);
			println();
			println("Type 'help' for available commands.");
			println();
		}

		if (singleCommand != null) {
			parseLine(singleCommand);
			cmd_quit();
		}

		parse();
	}

	private void processArgs(JSAPResult options) throws PEException {
		displayVersion = options.getBoolean("version");
		suppressWelcome = options.getBoolean("suppressWelcome");
		emulateReader = options.getBoolean("emulateReader");
		final String filename = options.getString("file");
		final String cmd = options.getString("cmd");

		if (filename != null) {
			try {
				fileInputStream = new FileInputStream(filename);
			} catch (final Exception e) {
				throw new PEException("Failed to open file '" + filename + "'", e);
			}
		} else if (cmd != null) {
			singleCommand = cmd;
		}
	}

	public void cmd_help() {
		if (currentCommandMap.isDefaultMap()) {
			println("Available commands are:");
		} else {
			println("Available commands in '" + currentCommandMap.getMode() + "' mode are:");
		}

		// Indent the description, but put it on a new line if it's too long.
		final int maxCmdLen = 30;
		final int maxDescLen = 71;
		final int initialIndentSize = 2;
		final int gapSize = 2;
		final String cmdIndent = StringUtils.rightPad("", initialIndentSize);
		final String descIndent = StringUtils.rightPad("", initialIndentSize + maxCmdLen + gapSize);

		final ArrayList<CommandType> fullSet = new ArrayList<CommandType>(globalCommandMap.values());
		fullSet.addAll(currentCommandMap.values());

		for (final CommandType command : fullSet) {
			if (!command.m_internal || debugMode) {
				String remainingDesc = command.m_desc;
				String cmd = command.toString();
				boolean moreDesc = true;
				int index = -1;
				do {
					String desc;
					if (remainingDesc.length() <= maxDescLen) {
						desc = remainingDesc;
						moreDesc = false;
					} else {
						int tempIndex = remainingDesc.indexOf(" ", index);
						while (((tempIndex = remainingDesc.indexOf(" ", tempIndex + 1)) > -1) && (tempIndex < maxDescLen)) {
							index = tempIndex;
						}

						desc = remainingDesc.substring(0, index);
						remainingDesc = remainingDesc.substring(index + 1);
					}
					if (cmd.length() > maxCmdLen) {
						println(cmdIndent + cmd);
						println(descIndent + desc);
					} else {
						println(cmdIndent + StringUtils.rightPad(cmd, maxCmdLen + gapSize) + desc);
					}
					cmd = "";
				} while (moreDesc);
			}
		}
	}

	public void cmd_quit() {
		if (!suppressWelcome) {
			printlnDots("Exiting");
		}

		close();

		System.exit(returnCode);
	}

	public void cmd_exit() {
		if (currentCommandMap.isDefaultMap()) {
			cmd_quit();
		}

		changeMode(findCommandMap(DEFAULT_MAP));

		printlnDots("mode changed to '" + DEFAULT_MAP + "'");
	}

	public void close() {
		try {
			if (fileInputStream != null) {
				fileInputStream.close();
				fileInputStream = null;
			}
		} catch (final Exception e) {
			printlnDots("Error closing input stream - " + e.getMessage());
		}

		if (printStream != null) {
			printStream.flush();
			printStream.close();
			printStream = null;
		}
	}

	/**
	 * Toggle debug mode on / off - in debug mode we display more information on
	 * exceptions and also show any hidden commands in the help text.
	 */
	public void cmd_debug() {
		debugMode = !debugMode;

		printlnDots("debug mode is now '" + debugMode + "'");
	}

	public void cmd_modes() {
		final Set<String> keys = commandMapMap.keySet();

		printlnDots("available modes are:");
		for (final String key : keys) {
			printlnIndent(key);
		}
	}

	public void cmd_mode(Scanner scanner) throws PEException {
		String mode = scan(scanner);

		if (mode == null) {
			mode = DEFAULT_MAP;
		}

		final CommandMapType map = findCommandMap(mode);

		if (map == null) {
			throw new PEException("'" + mode + "' isn't a valid mode");
		}

		changeMode(map);

		printlnDots("mode changed to '" + mode + "'");
	}

	private void changeMode(CommandMapType map) {
		currentCommandMap = map;

		if (currentCommandMap.isDefaultMap()) {
			prompt = DEFAULT_PROMPT;
		} else {
			prompt = "(" + map.getMode() + ") " + DEFAULT_PROMPT;
		}
	}

	public void setPrintStream(PrintStream printStream) {
		this.printStream = printStream;
	}

	public PrintStream getPrintStream() {
		return printStream;
	}

	@Override
	public void printlnIndent(String message) {
		println("    " + message);
	}

	@Override
	public void printlnDots(String message) {
		println("... " + message);
	}

	@Override
	public void println(String message) {
		if (printStream != null) {
			printStream.println(message);
		}

		logger.debug(message);
	}

	@Override
	public void println() {
		if (printStream != null) {
			printStream.println();
		}
	}

	protected String scan(Scanner scanner) throws PEException {
		return scan(scanner, null);
	}

	protected String scan(Scanner scanner, String exists) throws PEException {
		if (hasRequiredArg(scanner, exists)) {
			return scanner.next();
		}

		return null;
	}

	protected File scanFile(Scanner scanner, String exists) throws PEException {
		if (hasRequiredArg(scanner, exists)) {
			try {
				final File file = new File(scanFilePath(scanner));
				return file;
			} catch (final Exception e) {
				throw new PEException("Failed to parse file parameter", e);
			}
		}

		return null;
	}

	protected File scanFile(Scanner scanner) throws PEException {
		return scanFile(scanner, null);
	}

	protected static String scanFilePath(Scanner scanner) {
		if (scanner.hasNext()) {
			final String token = scanner.next();
			if (token.startsWith("\"") || token.startsWith("'")) {
				final String quote = String.valueOf(token.charAt(0));
				if (!token.endsWith(quote)) {
					final String remainder = scanner.findInLine(".+?" + quote);
					return token.substring(1) + remainder.substring(0, remainder.length() - 1);
				}

				return token.substring(1, token.length() - 1);
			}

			return token;
		}

		return StringUtils.EMPTY;
	}

	protected Integer scanInteger(Scanner scanner) throws PEException {
		return scanInteger(scanner, null);
	}

	protected Integer scanInteger(Scanner scanner, String exists) throws PEException {
		if (hasRequiredArg(scanner, exists)) {
			try {
				return scanner.nextInt();
			} catch (final Exception e) {
				throw new PEException("Failed to parse integer parameter", e);
			}
		}

		return null;
	}

	protected Long scanLong(Scanner scanner, String exists) throws PEException {
		if (hasRequiredArg(scanner, exists)) {
			try {
				return scanner.nextLong();
			} catch (final Exception e) {
				throw new PEException("Failed to parse long parameter", e);
			}
		}

		return null;
	}

	protected Boolean scanBoolean(Scanner scanner) throws PEException {
		return scanBoolean(scanner, null);
	}

	protected Boolean scanBoolean(Scanner scanner, String exists) throws PEException {
		if (hasRequiredArg(scanner, exists)) {
			try {
				return scanner.nextBoolean();
			} catch (final Exception e) {
				throw new PEException("Failed to parse boolean parameter", e);
			}
		}

		return null;
	}

	private boolean hasRequiredArg(final Scanner scanner, final String argName) throws PEException {
		if (!scanner.hasNext()) {
			if (argName != null) {
				throw new PEException("Expecting argument '" + argName + "'");
			}

			return false;
		}

		return true;
	}

	private CommandType findCommand(String line) {
		final CommandType cmd = currentCommandMap.findCommand(line);

		if (cmd != null) {
			return cmd;
		}

		return globalCommandMap.findCommand(line);
	}

	/**
	 * Register a new command in the default command mode.
	 */
	protected void registerCommand(CommandType type) {
		registerCommand(DEFAULT_MAP, type);
	}

	/**
	 * Register a new command in the specified command mode.
	 */
	protected void registerCommand(String mode, CommandType type) {
		CommandMapType map = findCommandMap(mode);

		if (map == null) {
			map = createCommandMap(mode);
		}

		map.registerCommand(type);
	}

	protected CommandMapType findCommandMap(String mode) {
		return commandMapMap.get(mode);
	}

	private CommandMapType createGlobalCommandMap() {

		final CommandMapType map = new CommandMapType(GLOBAL_MAP);

		map.registerCommand(new CommandType(new String[] { "help" }, "List available commands."));
		map.registerCommand(new CommandType(new String[] { "exit" },
				"Exit the current mode or quit if in default mode."));
		map.registerCommand(new CommandType(new String[] { "quit" }, "Quit this command line tool."));
		map.registerCommand(new CommandType(new String[] { "debug" }, "Toggle debug mode.", true));
		map.registerCommand(new CommandType(new String[] { "modes" }, "List available modes.", true));
		map.registerCommand(new CommandType(new String[] { "mode" }, "<mode name>",
				"Switch into the specified command mode.", true));

		return map;
	}

	protected CommandMapType createCommandMap(String mode) {
		if (commandMapMap.containsKey(mode)) {
			return commandMapMap.get(mode);
		}

		final CommandMapType map = new CommandMapType(mode);

		commandMapMap.put(mode, map);

		return map;
	}

	@SuppressWarnings("resource")
	private void parse() throws Exception {
		if (emulateReader) {
			BufferedReader bufferedReader;

			if (fileInputStream != null) {
				bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
			} else {
				bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			}

			while (true) {
				println(prompt);

				final String line = bufferedReader.readLine();

				if (line == null) {
					break;
				}

				parseLine(line);

				if ((question != null) && (questionCallback != null)) {
					println(question);

					question = null;
					parseAnswer(line, bufferedReader.readLine());
					questionCallback = null;
				}
			}
		} else {
			ConsoleReader reader;

			if (fileInputStream != null) {
				reader = new ConsoleReader(fileInputStream, new NullWriter());
			} else {
				reader = new ConsoleReader();
			}

			reader.setDefaultPrompt(prompt);

			String line = null;
			while ((line = reader.readLine()) != null) {
				parseLine(line);

				if ((question != null) && (questionCallback != null)) {
					reader.setDefaultPrompt(question);

					question = null;
					parseAnswer(line, reader.readLine());
					questionCallback = null;
				}
				reader.setDefaultPrompt(prompt);
			}
		}
		cmd_quit();
	}

	public interface QuestionCallback {
		public void answer(String line) throws PEException;
	}

	protected void askQuestion(String question, QuestionCallback questionCallback) {
		this.question = question;
		this.questionCallback = questionCallback;
	}

	public void parseAnswer(String command, String answer) {
		try {
			questionCallback.answer(answer);
		} catch (final Throwable e) {
			println("Error: Failed to execute '" + command + "'"
					+ (e.getMessage() != null ? " - " + e.getMessage() : ""));
			final Throwable cause = e.getCause();
			if (cause != null) {
				printlnIndent("Caused by: " + cause.getMessage());
				if (debugMode && (printStream != null)) {
					cause.printStackTrace(printStream);
				}
			}
			returnCode = 2;
		}
	}

	public void parseLine(String line) {
		if (StringUtils.isBlank(line)) {
			return;
		}

		final CommandType command = findCommand(line);

		if (command != null) {

			// We need to create a new scanner based on the command we found.
			try (final Scanner scanner = new Scanner(line)) {

				// And skip the length of the command to get to the args.
				for (int i = 0; i < command.getCmdLength(); i++) {
					scanner.next();
				}

				// Try the currentCommandMap first to map this command to a method name.
				String methodName = currentCommandMap.buildMethodName(command);

				if (methodName == null) {
					methodName = globalCommandMap.buildMethodName(command);
				}

				try {
					final Class<? extends CLIBuilder> cls = this.getClass();

					try {
						final Method method = cls.getMethod(methodName);
						method.invoke(this);
					} catch (final NoSuchMethodException e) {
						final Method method = cls.getMethod(methodName, Scanner.class);
						method.invoke(this, scanner);
					}
				} catch (final NoSuchMethodException nsme) {
					println("Error: Failed to locate method for '" + methodName + "'");
					returnCode = 3;
				} catch (final Throwable e) {
					println("Error: Failed to execute '" + line + "'"
							+ (e.getMessage() != null ? " - " + e.getMessage() : ""));
					final Throwable cause = e.getCause();
					if (cause != null) {
						printlnIndent("Caused by: " + cause.getMessage());
						if (debugMode && (printStream != null)) {
							cause.printStackTrace(printStream);
						}
					}
					returnCode = 2;
				}
			}
		} else {
			println("Error: Failed to find matching command for '" + line + "'");
			returnCode = 1;
		}
	}

	public static class CommandMapType extends LinkedHashMap<String, CommandType> {

		private static final long serialVersionUID = 1L;

		private final String mode;

		public CommandMapType(String mode) {
			this.mode = mode;
		}

		public String getMode() {
			return mode;
		}

		public boolean isDefaultMap() {
			return mode.equalsIgnoreCase(DEFAULT_MAP);
		}

		public boolean isGlobalMap() {
			return mode.equalsIgnoreCase(GLOBAL_MAP);
		}

		public void registerCommand(CommandType type) {
			put(type.getKey(), type);
		}

		public CommandType findCommand(String line) {
			final Scanner scanner = new Scanner(line);

			String test = scanner.next();

			CommandType found = null;

			while (true) {
				// See if we have a valid command.
				final CommandType cmd = get(test);

				if (cmd != null) {
					// See if this new command is a better choice than
					// any we already have.
					if ((found == null) || (cmd.getCmdLength() > found.getCmdLength())) {
						found = cmd;
					}
				}
				if (!scanner.hasNext()) {
					break;
				}

				test += CMD_NAME_DELIMITER + scanner.next();
			}

			scanner.close();

			return found;
		}

		public String buildMethodName(CommandType cmd) {
			if (!containsKey(cmd.getKey())) {
				return null;
			}

			if (isDefaultMap() || isGlobalMap()) {
				return CMD_NAME_PREFIX + CMD_NAME_DELIMITER + cmd.getKey();
			}

			return CMD_NAME_PREFIX + CMD_NAME_DELIMITER + mode + CMD_NAME_DELIMITER + cmd.getKey();
		}
	}

	public static class CommandType {
		String[] m_cmds;
		String m_args;
		String m_desc;
		boolean m_internal;

		public CommandType(String[] cmds, String desc) {
			this(cmds, null, desc, false);
		}

		public CommandType(String[] cmds, String args, String desc) {
			this(cmds, args, desc, false);
		}

		public CommandType(String[] cmds, String desc, boolean internal) {
			this(cmds, null, desc, internal);
		}

		public CommandType(String[] cmds, String args, String desc, boolean internal) {
			this.m_cmds = cmds;
			this.m_args = args;
			this.m_desc = desc;
			this.m_internal = internal;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof CommandType) {
				return Arrays.equals(this.m_cmds, ((CommandType) obj).m_cmds);
			}

			return super.equals(obj);
		}

		public int getCmdLength() {
			return m_cmds.length;
		}

		public String getFullName() {
			return buildName(" ");
		}

		public String getKey() {
			return buildName(CMD_NAME_DELIMITER);
		}

		private String buildName(String delim) {
			final StringBuilder builder = new StringBuilder();
			for (final String cmd : m_cmds) {
				if (builder.length() > 0) {
					builder.append(delim);
				}
				builder.append(cmd);
			}
			return builder.toString();
		}

		@Override
		public int hashCode() {
			return getKey().hashCode();
		}

		@Override
		public String toString() {
			String ret = buildName(" ");

			if (m_args != null) {
				ret += (" " + m_args);
			}
			return ret;
		}
	}
}
