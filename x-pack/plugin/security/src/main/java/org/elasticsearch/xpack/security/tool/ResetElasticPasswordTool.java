/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.tool;

import joptsimple.OptionSet;

import joptsimple.OptionSpecBuilder;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.function.Function;

public class ResetElasticPasswordTool extends BaseRunAsSuperuserCommand {

    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final OptionSpecBuilder interactive;
    private final OptionSpecBuilder auto;
    private final OptionSpecBuilder batch;

    ResetElasticPasswordTool() {
        this(environment -> new CommandLineHttpClient(environment), environment -> KeyStoreWrapper.load(environment.configFile()));
    }

    public static void main(String[] args) throws Exception {
        exit(new ResetElasticPasswordTool().main(args, Terminal.DEFAULT));
    }

    protected ResetElasticPasswordTool(
        Function<Environment, CommandLineHttpClient> clientFunction,
        CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction) {
        super(clientFunction, keyStoreFunction);
        interactive = parser.acceptsAll(List.of("i", "interactive"));
        auto = parser.acceptsAll(List.of("a", "auto")); // default
        batch = parser.acceptsAll(List.of("b", "batch"));
        this.clientFunction = clientFunction;
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final SecureString elasticPassword;
        if (options.has(interactive)) {
            if (options.has(batch) == false) {
                terminal.println("This tool will reset the password of the [elastic] user.");
                terminal.println("You will be prompted to enter the password.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }
            elasticPassword = promptForPassword(terminal);
        } else {
            if (options.has(batch) == false) {
                terminal.println("This tool will reset the password of the [elastic] user to an autogenerated value.");
                terminal.println("The password will be printed in the console.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }
            elasticPassword = new SecureString(generatePassword(20));
        }
        try (SecureString fileRealmSuperuserPassword = getPassword()) {
            final String fileRealmSuperuser = getUsername();
            final CommandLineHttpClient client = clientFunction.apply(env);
            final URL changePasswordUrl = createURL(new URL(client.getDefaultURL()), "_security/user/elastic/_password", "?pretty");
            final HttpResponse httpResponse = client.execute(
                "POST",
                changePasswordUrl,
                fileRealmSuperuser,
                fileRealmSuperuserPassword,
                () -> requestBodySupplier(elasticPassword),
                this::responseBuilder
            );
            if (httpResponse.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                throw new UserException(ExitCodes.TEMP_FAILURE, "Failed to reset password for the elastic user");
            } else {
                if (options.has(interactive)) {
                    terminal.println("Password for the elastic user successfully reset.");
                } else {
                    terminal.println("Password for the elastic user successfully reset.");
                    terminal.println("New value: " + elasticPassword);
                }
            }
        } catch (Exception e) {
            throw new UserException(ExitCodes.TEMP_FAILURE, "Failed to reset password for the elastic user", e);
        } finally {
            elasticPassword.close();
        }
    }

    private SecureString promptForPassword(Terminal terminal) {
        while (true) {
            SecureString password1 = new SecureString(terminal.readSecret("Enter password for [elastic]: "));
            Validation.Error err = Validation.Users.validatePassword(password1);
            if (err != null) {
                terminal.errorPrintln(err.toString());
                terminal.errorPrintln("Try again.");
                password1.close();
                continue;
            }
            try (SecureString password2 = new SecureString(terminal.readSecret("Reenter password for [elastic]: "))) {
                if (password1.equals(password2) == false) {
                    terminal.errorPrintln("Passwords do not match.");
                    terminal.errorPrintln("Try again.");
                    password1.close();
                    continue;
                }
            }
            return password1;
        }
    }

    private String requestBodySupplier(SecureString pwd) throws Exception {
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        xContentBuilder.startObject().field("password", pwd.toString()).endObject();
        return Strings.toString(xContentBuilder);
    }

    @Override
    protected void validate(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if ((options.has("i") || options.has("interactive")) && (options.has("a") || options.has("auto"))) {
            throw new UserException(ExitCodes.USAGE, "You can only run the tool in one of [auto] or [interactive] modes");
        }
    }

}
