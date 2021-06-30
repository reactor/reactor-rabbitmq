/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rabbitmq;

import com.rabbitmq.client.Connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Host {

    public static String capture(InputStream is)
        throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuilder buff = new StringBuilder();
        while ((line = br.readLine()) != null) {
            buff.append(line).append("\n");
        }
        return buff.toString();
    }

    public static Process executeCommand(String command) throws IOException {
        Process pr = executeCommandProcess(command);

        int ev = waitForExitValue(pr);
        if (ev != 0) {
            String stdout = capture(pr.getInputStream());
            String stderr = capture(pr.getErrorStream());
            throw new IOException("unexpected command exit value: " + ev +
                "\ncommand: " + command + "\n" +
                "\nstdout:\n" + stdout +
                "\nstderr:\n" + stderr + "\n");
        }
        return pr;
    }

    private static int waitForExitValue(Process pr) {
        while (true) {
            try {
                pr.waitFor();
                break;
            } catch (InterruptedException ignored) {
            }
        }
        return pr.exitValue();
    }

    private static Process executeCommandProcess(String command) throws IOException {
        String[] finalCommand;
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            finalCommand = new String[4];
            finalCommand[0] = "C:\\winnt\\system32\\cmd.exe";
            finalCommand[1] = "/y";
            finalCommand[2] = "/c";
            finalCommand[3] = command;
        } else {
            finalCommand = new String[3];
            finalCommand[0] = "/bin/sh";
            finalCommand[1] = "-c";
            finalCommand[2] = command;
        }
        return Runtime.getRuntime().exec(finalCommand);
    }

    public static Process rabbitmqctl(String command) throws IOException {
        return executeCommand(rabbitmqctlCommand() +
            " " + command);
    }

    public static String rabbitmqctlCommand() {
        String rabbitmqCtl = System.getProperty("rabbitmqctl.bin", System.getenv("RABBITMQCTL_BIN"));
        if (rabbitmqCtl == null) {
            throw new IllegalStateException("Please define the rabbitmqctl.bin system property or "
                + "the RABBITMQCTL_BIN environment variable");
        }
        if (rabbitmqCtl.startsWith("DOCKER:")) {
            String containerId = rabbitmqCtl.split(":")[1];
            return "docker exec " + containerId + " rabbitmqctl";
        } else if ("sudo_rabbitmqctl".equals(rabbitmqCtl)) {
            return "sudo rabbitmqctl";
        } else {
                return rabbitmqCtl;
        }
    }

    private static void closeConnection(String pid) throws IOException {
        rabbitmqctl("close_connection '" + pid + "' 'Closed via rabbitmqctl'");
    }

    public static void closeConnection(Connection c) throws IOException {
        Host.ConnectionInfo ci = findConnectionInfoFor(Host.listConnections(), c);
        closeConnection(ci.getPid());
    }

    public static List<ConnectionInfo> listConnections() throws IOException {
        String output = capture(rabbitmqctl("list_connections -q pid peer_port client_properties")
            .getInputStream());
        // output (header line presence depends on broker version):
        // pid	peer_port
        // <rabbit@mercurio.1.11491.0>	58713 [{"product","RabbitMQ"},{"...
        String[] allLines = output.split("\n");

        ArrayList<ConnectionInfo> result = new ArrayList<ConnectionInfo>();
        for (String line : allLines) {
            // line: <rabbit@mercurio.1.11491.0>	58713
            String[] columns = line.split("\t");
            // can be also header line, so ignoring NumberFormatException
            try {
                Integer.valueOf(columns[1]); // just to ignore header line
                result.add(new ConnectionInfo(columns[0], connectionName(columns[2])));
            } catch (NumberFormatException e) {
                // OK
            }
        }
        return result;
    }

    private static String connectionName(String clientProperties) {
        String beginning = "\"connection_name\",\"";
        int begin = clientProperties.indexOf(beginning);
        if (begin > 0) {
            int start = clientProperties.indexOf(beginning) + beginning.length();
            int end = clientProperties.indexOf("\"", start);
            return clientProperties.substring(start, end);
        } else {
            return null;
        }
    }

    private static Host.ConnectionInfo findConnectionInfoFor(List<ConnectionInfo> xs, Connection c) {
        Host.ConnectionInfo result = null;
        for (Host.ConnectionInfo ci : xs) {
            if (c.getClientProvidedName().equals(ci.getName())) {
                result = ci;
                break;
            }
        }
        return result;
    }

    public static class ConnectionInfo {

        private final String pid;
        private final String name;

        public ConnectionInfo(String pid, String name) {
            this.pid = pid;
            this.name = name;
        }

        public String getPid() {
            return pid;
        }

        public String getName() {
            return name;
        }
    }
}
