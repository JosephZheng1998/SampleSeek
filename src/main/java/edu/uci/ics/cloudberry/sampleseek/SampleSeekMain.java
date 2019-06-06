package edu.uci.ics.cloudberry.sampleseek;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.uci.ics.cloudberry.sampleseek.core.QueryExecutor;
import edu.uci.ics.cloudberry.sampleseek.core.SampleManager;
import edu.uci.ics.cloudberry.sampleseek.core.SeekManager;
import edu.uci.ics.cloudberry.sampleseek.model.Query;
import edu.uci.ics.cloudberry.sampleseek.util.Config;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class SampleSeekMain extends AllDirectives {

    public static Config config = null;
    public static Connection conn = null;
    public SampleManager sampleManager;
    public SeekManager seekManager;
    public QueryExecutor queryExecutor;

    public SampleSeekMain () {
        sampleManager = new SampleManager();
        seekManager = new SeekManager();
        queryExecutor = new QueryExecutor(sampleManager, seekManager);
    }

    public static Config loadConfig(String configFilePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Config config = mapper.readValue(new File(configFilePath), Config.class);
        return config;
    }

    public static boolean connectDB() {
        try {
            conn = DriverManager.getConnection(config.getDbConfig().getUrl(),
                    config.getDbConfig().getUsername(), config.getDbConfig().getPassword());
            System.out.println("Connected to the PostgreSQL server successfully.");
            return true;
        } catch (SQLException e) {
            System.err.println("Connecting to the PostgreSQL server failed. Exceptions:");
            System.err.println(e.getMessage());
            return false;
        }
    }

    public static void disconnectDB() {
        try {
            conn.close();
            System.out.println("Disconnected from the PostgreSQL server successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void initialize() {
        boolean success = false;

        // check sample table exist or not
        System.out.println("=== Sample exist? ===");
        success = sampleManager.isSampeExist();
        System.out.println(success ? "YES!" : "NO!");

        // if not exist, generate the sample table
        if (!success) {
            System.out.println("=== Generating sample into database ===");
            long start = System.currentTimeMillis();
            success = sampleManager.generateSample();
            long end = System.currentTimeMillis();
            System.out.println("successful? : " + success);
            if (!success) {
                System.err.println("exit ...");
                return;
            }
            System.out.println("time: " + String.format("%.3f", (end - start)/1000.0) + " seconds");
        }

        // load the sample table into memory
        System.out.println("=== Loading sample into memory ===");
        long start = System.currentTimeMillis();
        success = sampleManager.loadSample();
        long end = System.currentTimeMillis();
        System.out.println("successful? : " + success);
        if (!success) {
            System.err.println("exit ...");
        }
        System.out.println("time: " + String.format("%.3f", (end - start)/1000.0) + " seconds");

        System.out.println("=== Sample loaded ===");
        sampleManager.printSample();

        // build indexes on base table
        System.out.println("=== Building indexes on base table ===");
        start = System.currentTimeMillis();
        int successCount = seekManager.buildIndexes();
        end = System.currentTimeMillis();
        System.out.println("# of indexes built : " + successCount);
        System.out.println("time: " + String.format("%.3f", (end - start)/1000.0) + " seconds");
    }

    public void test() {

        // answer a query
        String queryString = "{\"select\": [\"x\", \"y\"], \"filters\": [{\"attribute\": \"create_at\", \"operator\": \"IN\", \"operands\": [\"2017-06-01 00:00:00\", \"2017-07-01 00:00:00\"]}]}";
        ObjectMapper objectMapper = new ObjectMapper();
        Query query = null;
        try {
            query = objectMapper.readValue(queryString, Query.class);
        } catch (IOException e) {
            System.err.println("=== Can not parse the query input ===");
            System.err.println(queryString);
            e.printStackTrace();
            return;
        }

        JsonNode result = queryExecutor.executeQuery(query);

        System.out.println("=== Execution result ===");
        System.out.println(result);
    }

    public static void main(String[] args) throws IOException {

        // parse arguments
        String configFilePath = null;
        for (int i = 0; i < args.length; i ++) {
            switch (args[i].toLowerCase()) {
                case "--config" :
                case "-c" :
                    try {
                        configFilePath = args[i + 1];
                    }
                    catch (ArrayIndexOutOfBoundsException e) {
                        System.err.println("Config file path should follow -c [--config].");
                        return;
                    }
            }
        }

        if (configFilePath == null) {
            System.err.println("Please indicate config file path.\nUsage: --config [file] or -c [file].\n");
            //return;
            System.err.println("Use default config file path: ./src/sampleseek.yaml\n");
            configFilePath = "/Users/white/IdeaProjects/sampleseek/src/sampleseek.yaml";
        }

        // load config file
        try {
            config = loadConfig(configFilePath);
        } catch (IOException e) {
            System.err.println("Config file: [" + configFilePath + "] does not exist.");
            e.printStackTrace();
            return;
        }

        // connect to database
        boolean ok = connectDB();
        if (!ok) {
            System.err.println("Cannot connect to database, exit.");
            return;
        }

        // initialize HTTP server
        String hostname = config.getServerConfig().getOrDefault("hostname", "localhost");
        int port = Integer.valueOf(config.getServerConfig().getOrDefault("port", "8080"));
        String serverUrl = "http://" + hostname + ":" + port;
        String queryUrl = serverUrl + "/query";

        // Refer to Akka-http: https://doc.akka.io/docs/akka-http/current/introduction.html
        // boot up server using the route as defined below
        ActorSystem system = ActorSystem.create("routes");

        final Http http = Http.get(system);
        final ActorMaterializer materializer = ActorMaterializer.create(system);

        //In order to access all directives we need an instance where the routes are define.
        SampleSeekMain app = new SampleSeekMain();
        app.initialize();

        final Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = app.createRoute().flow(system, materializer);
        final CompletionStage<ServerBinding> binding = http.bindAndHandle(routeFlow,
                ConnectHttp.toHost(hostname, port), materializer);

        System.out.println("=== Server started ===");
        System.out.println("Server online at " + serverUrl);
        System.out.println("Query url: " + queryUrl);
        System.out.println("POST http request to query result: e.g. ");
        String sampleQueryJson = "{\"select\": [\"x\", \"y\", \"create_at\"], \"filters\": [{\"attribute\": \"create_at\", \"operator\": \"IN\", \"operands\": [\"2017-06-01 00:00:00\", \"2017-07-01 00:00:00\"]}]}";
        System.out.println("curl -H \"Content-Type: application/json\" -X POST -d '" + sampleQueryJson + "' " + queryUrl);
        System.out.println("\nPress RETURN to stop...");
        System.in.read(); // let it run until user presses return

        disconnectDB();

        binding
                .thenCompose(ServerBinding::unbind) // trigger unbinding from the port
                .thenAccept(unbound -> system.terminate()); // and shutdown when done
    }

    private Route createRoute() {

        return concat(
                post(() ->
                        path("query", () ->
                                entity(Jackson.unmarshaller(Query.class), query -> {
                                    CompletionStage<JsonNode> futureResult = executeQuery(query);
                                    return onSuccess(futureResult, result ->
                                            completeOK(result, Jackson.marshaller())
                                    );
                                })))
        );
    }

    private CompletionStage<JsonNode> executeQuery(final Query query) {
        return CompletableFuture.completedFuture(queryExecutor.executeQuery(query));
    }
}
