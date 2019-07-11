/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Locale;

/**
 * A Terminal wraps access to reading input and writing output for a cli.
 *
 * The available methods are similar to those of {@link Console}, with the ability
 * to read either normal text or a password, and the ability to print a line
 * of text. Printing is also gated by the {@link Verbosity} of the terminal,
 * which allows {@link #println(Verbosity,String)} calls which act like a logger,
 * only actually printing if the verbosity level of the terminal is above
 * the verbosity of the message.
*/
public abstract class Terminal {

    /** The default terminal implementation, which will be a console if available, or stdout/stderr if not. */
    public static final Terminal DEFAULT = ConsoleTerminal.isSupported() ? new ConsoleTerminal() : new SystemTerminal();

    /** Defines the available verbosity levels of messages to be printed. */
    public enum Verbosity {
        SILENT, /* always printed */
        NORMAL, /* printed when no options are given to cli */
        VERBOSE /* printed only when cli is passed verbose option */
    }

    /** The current verbosity for the terminal, defaulting to {@link Verbosity#NORMAL}. */
    private Verbosity verbosity = Verbosity.NORMAL;

    /** The newline used when calling println. */
    private final String lineSeparator;

    protected Terminal(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    /** Visible for testing in subclasses: set an input other than standard input */
    protected void setInput(InputStream inputStream) throws IOException {
        throw new AssertionError("unsupported operation");
    }

    /** Sets the verbosity of the terminal. */
    public void setVerbosity(Verbosity verbosity) {
        this.verbosity = verbosity;
    }

    /** Reads clear text from the terminal input. See {@link Console#readLine()}. */
    public abstract String readText(String prompt);

    /** Reads password text from the terminal input. See {@link Console#readPassword()}}. */
    public abstract char[] readSecret(String prompt);

    /** Returns a Writer which can be used to write to the terminal directly. */
    public abstract PrintWriter getWriter();

    /** Prints a line to the terminal at {@link Verbosity#NORMAL} verbosity level. */
    public final void println(String msg) {
        println(Verbosity.NORMAL, msg);
    }

    /** Prints a line to the terminal at {@code verbosity} level. */
    public final void println(Verbosity verbosity, String msg) {
        print(verbosity, msg + lineSeparator);
    }

    /** Prints message to the terminal at {@code verbosity} level, without a newline. */
    public final void print(Verbosity verbosity, String msg) {
        if (isPrintable(verbosity)) {
            getWriter().print(msg);
            getWriter().flush();
        }
    }

    /** Checks if is enough {@code verbosity} level to be printed */
    public final boolean isPrintable(Verbosity verbosity) {
        return this.verbosity.ordinal() >= verbosity.ordinal();
    }

    /**
     * Prompt for a yes or no answer from the user. This method will loop until 'y' or 'n'
     * (or the default empty value) is entered.
     */
    public final boolean promptYesNo(String prompt, boolean defaultYes) {
        String answerPrompt = defaultYes ? " [Y/n]" : " [y/N]";
        while (true) {
            String answer = readText(prompt + answerPrompt);
            if (answer == null || answer.isEmpty()) {
                return defaultYes;
            }
            answer = answer.toLowerCase(Locale.ROOT);
            boolean answerYes = answer.equals("y");
            if (answerYes == false && answer.equals("n") == false) {
                println("Did not understand answer '" + answer + "'");
                continue;
            }
            return answerYes;
        }
    }

    private static class ConsoleTerminal extends Terminal {

        private static final Console CONSOLE = System.console();

        ConsoleTerminal() {
            super(System.lineSeparator());
        }

        static boolean isSupported() {
            return CONSOLE != null;
        }

        @Override
        public PrintWriter getWriter() {
            return CONSOLE.writer();
        }

        @Override
        public String readText(String prompt) {
            return CONSOLE.readLine("%s", prompt);
        }

        @Override
        public char[] readSecret(String prompt) {
            return CONSOLE.readPassword("%s", prompt);
        }
    }

    private static class SystemTerminal extends Terminal {

        private static final PrintWriter WRITER = newWriter();

        private BufferedReader reader;

        SystemTerminal() {
            super(System.lineSeparator());
        }

        @SuppressForbidden(reason = "Writer for System.out")
        private static PrintWriter newWriter() {
            return new PrintWriter(System.out);
        }

        private BufferedReader getReader() {
            if (reader == null) {
                reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()));
            }
            return reader;
        }

        /** Visible for testing the system terminal with custom input */
        @Override
        protected void setInput(InputStream inputStream) throws IOException {
            if (reader != null) {
                reader.close();
            }
            reader = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
        }

        @Override
        public PrintWriter getWriter() {
            return WRITER;
        }

        @Override
        public String readText(String text) {
            getWriter().print(text);
            try {
                final String line = getReader().readLine();
                if (line == null) {
                    throw new IllegalStateException("unable to read from standard input; is standard input open and a tty attached?");
                }
                return line;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        public char[] readSecret(String text) {
            return readText(text).toCharArray();
        }
    }
}
