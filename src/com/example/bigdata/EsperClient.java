package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 2;
            howLongInSec = 20;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        EPCompiled epCompiled = getEPCompiled(config);

        // Connect to the EPRuntime server and deploy the statement
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        } catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "answer");

        resultStatement.addListener((newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

        Faker faker = new Faker();
        String[] eventRecords = createInputData();
        int element = 0;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
//                generateMountainEventData(runtime, faker);
                runtime.getEventService().sendEventJson(eventRecords[element], "MountainEvent");
                element++;
            }
            waitToEpoch();
        }
    }

    private static EPCompiled getEPCompiled(Configuration config) {
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;

        try {
            // # 0
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//                    amount_people int, ets string, its string);
//
//                    @name('answer')
//                    SELECT *
//                    FROM MountainEvent;
//                    """, compilerArgs);

            // # 1 OK
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT result, SUM(amount_people) AS sum_people
//            FROM MountainEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 10 sec)
//            WHERE result = 'base-reached' OR result = 'summit-reached';
//            """, compilerArgs);

            // # 2 OK
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT peak_name, trip_leader, result, amount_people
//            FROM MountainEvent(amount_people < 3)
//            WHERE result = 'resignation-someone-missing';
//            """, compilerArgs);

            // # 3 OK
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT
//                a.peak_name AS peak_name,
//                a.trip_leader AS trip_leader,
//                a.result AS result,
//                a.amount_people AS amount_people,
//                MAX(a.amount_people) AS max_people,
//                a.its as its
//            FROM PATTERN [ EVERY a=MountainEvent(result = 'resignation-someone-missing') ]#length(20)
//            HAVING a.amount_people = MAX(a.amount_people);
//            """, compilerArgs);

            // # 4 ???
            epCompiled = compiler.compile("""
            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
            amount_people int, ets string, its string);

            @name('answer')
            SELECT peak_name, how_many
            FROM (
                SELECT peak_name, COUNT(peak_name) AS how_many
                FROM MountainEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
                GROUP BY peak_name
                ORDER BY COUNT(peak_name) DESC LIMIT 10;
            );
            """, compilerArgs);

            // # 4.1
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT peak_name, count(peak_name) as how_many
//                FROM MountainEvent#ext_timed_batch_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
//                GROUP BY peak_name
//                ORDER BY COUNT(peak_name) DESC LIMIT 999 OFFSET 10;
//            );
//            """, compilerArgs);

            // # 5 ????????????????????
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT
//            a[0].result as result_1,
//            a[1].result as result_2,
//            a[2].result as result_3,
//            a[0].its as its_1,
//            a[1].its as its_2,
//            a[2].its as its_3,
//            java.sql.Timestamp.valueOf(a[2].its).getTime() - java.sql.Timestamp.valueOf(a[1].its).getTime() as diff
//            FROM PATTERN[
//                EVERY (
//                [3:] a=MountainEvent(result IN ('summit-reached', 'base-reached'))
//                UNTIL (b=MountainEvent(result NOT IN ('summit-reached', 'base-reached'))
//                AND (java.sql.Timestamp.valueOf(b.its).getTime() - java.sql.Timestamp.valueOf(a[0].its).getTime() > 5000))
//                )
//            ];
//            """, compilerArgs);

            // # 6.0 - EPL
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT a[0].trip_leader as trip_leader, a[0].result as result_1, a[1].result as result_2, a[2].result as result_3
//            FROM PATTERN[
//                EVERY (
//                [3] a=MountainEvent(result IN ('summit-reached', 'base-reached'))
//                UNTIL MountainEvent(trip_leader != a[0].trip_leader)
//                )
//                OR
//                EVERY (
//                [3] a=MountainEvent(result NOT IN ('summit-reached', 'base-reached'))
//                UNTIL MountainEvent(trip_leader != a[0].trip_leader)
//                )
//            ];
//            """, compilerArgs);

            // # 6.3 - OK?
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT
//            a.trip_leader,
//            a.result as result_1,
//            b.result as result_2,
//            c.result as result_3
//            FROM PATTERN[ (
//                EVERY a=MountainEvent(result IN ('summit-reached', 'base-reached')) ->
//                        (b=MountainEvent(result IN ('summit-reached', 'base-reached') AND trip_leader=a.trip_leader) ->
//                        c=MountainEvent(result IN ('summit-reached', 'base-reached') AND trip_leader=a.trip_leader))
//                        AND NOT MountainEvent(trip_leader!=a.trip_leader)
//                        AND NOT MountainEvent(result NOT IN ('summit-reached', 'base-reached')))
//                OR (
//                EVERY a=MountainEvent(result NOT IN ('summit-reached', 'base-reached')) ->
//                        (b=MountainEvent(result NOT IN ('summit-reached', 'base-reached') AND trip_leader=a.trip_leader) ->
//                        c=MountainEvent(result NOT IN ('summit-reached', 'base-reached') AND trip_leader=a.trip_leader))
//                        AND NOT MountainEvent(trip_leader!=a.trip_leader)
//                        AND NOT MountainEvent(result IN ('summit-reached', 'base-reached'))
//                ) ];
//
//            """, compilerArgs);

            // # 7 OK
//            epCompiled = compiler.compile("""
//            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string,
//            amount_people int, ets string, its string);
//
//            @name('answer')
//            SELECT *
//            FROM MountainEvent
//            MATCH_RECOGNIZE (
//                PARTITION BY trip_leader
//                MEASURES
//                    A.its AS its_a,
//                    A.trip_leader AS trip_leader,
//                    A.result AS result_a,
//                    FIRST(B.result) AS result_b_first,
//                    C.result AS result_c,
//                    A.amount_people AS amount_people_a,
//                    FIRST(B.amount_people) AS amount_people_b_first,
//                    C.amount_people AS amount_people_c
//                AFTER MATCH SKIP PAST LAST ROW
//                PATTERN (A B{3,5} C)
//                DEFINE
//                    A AS A.result NOT IN ('summit-reached', 'base-reached'),
//                    B AS (B.result IN ('summit-reached', 'base-reached')
//                        AND B.amount_people < A.amount_people),
//                    C AS (C.result NOT IN ('summit-reached', 'base-reached')
//                        AND C.amount_people >= A.amount_people)
//            );
//            """, compilerArgs);



        } catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }
        return epCompiled;
    }

    private static void generateMountainEventData(EPRuntime runtime, Faker faker) {
        Random random = new Random();
        ArrayList<String> possibleResults = new ArrayList<>() {
            {
                add("summit-reached");
                add("base-reached");
                add("resignation-injury");
                add("resignation-weather");
                add("resignation-someone-missing");
                add("resignation-other");
            }
        };

        String record;
        String name = faker.mountain().name();
        String mountaineer = faker.mountaineering().mountaineer();

        Timestamp timestamp = faker.date().past(42, TimeUnit.SECONDS);
        timestamp.setNanos(0);

        Timestamp timestampITS = Timestamp.valueOf(LocalDateTime.now().withNano(0));

        record = Format.toJson()
                .set("peak_name", () -> name)
                .set("trip_leader", () -> mountaineer)
                .set("result", () -> possibleResults.get(random.nextInt(possibleResults.size())))
                .set("amount_people", () -> random.nextInt(12) + 1)
                .set("ets", timestamp::toString)
                .set("its", timestampITS::toString)
                .build()
                .generate();
        runtime.getEventService().sendEventJson(record, "MountainEvent");

    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis);
        Instant instantTrunc = instant.truncatedTo(ChronoUnit.SECONDS);
        long millis2 = instantTrunc.toEpochMilli();
        TimeUnit.MILLISECONDS.sleep(millis2 + 1000 - millis);
    }

    static String[] createInputData() {
        return new String[]{
                "{\"peak_name\":\"Mount Whitney\",\"trip_leader\":\"Junko Tabei\",\"result\":\"summit-reached\",\"amount_people\":5,\"ets\":\"2024-04-02 12:16:51.0\",\"its\":\"2024-04-02 12:17:27.0\"}",
                "{\"peak_name\":\"Nanga Parbat\",\"trip_leader\":\"Junko Tabei\",\"result\":\"base-reached\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:11.0\",\"its\":\"2024-04-02 12:17:27.0\"}",
                "{\"peak_name\":\"Skil Brum\",\"trip_leader\":\"Junko Tabei\",\"result\":\"base-reached\",\"amount_people\":3,\"ets\":\"2024-04-02 12:17:13.0\",\"its\":\"2024-04-02 12:17:28.0\"}",
                "{\"peak_name\":\"Gasherbrum III\",\"trip_leader\":\"Junko Tabei\",\"result\":\"resignation-someone-missing\",\"amount_people\":11,\"ets\":\"2024-04-02 12:16:45.0\",\"its\":\"2024-04-02 12:17:28.0\"}",
                "{\"peak_name\":\"Nanga Parbat\",\"trip_leader\":\"Caroline Gleich\",\"result\":\"base-reached\",\"amount_people\":5,\"ets\":\"2024-04-02 12:17:07.0\",\"its\":\"2024-04-02 12:17:29.0\"}",
                "{\"peak_name\":\"Gongga Shan\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"resignation-weather\",\"amount_people\":9,\"ets\":\"2024-04-02 12:16:46.0\",\"its\":\"2024-04-02 12:17:29.0\"}",
                "{\"peak_name\":\"Masherbrum\",\"trip_leader\":\"Conrad Anker\",\"result\":\"resignation-injury\",\"amount_people\":11,\"ets\":\"2024-04-02 12:17:19.0\",\"its\":\"2024-04-02 12:17:30.0\"}",
                "{\"peak_name\":\"Saraghrar\",\"trip_leader\":\"Catherine Destivelle\",\"result\":\"resignation-other\",\"amount_people\":8,\"ets\":\"2024-04-02 12:17:20.0\",\"its\":\"2024-04-02 12:17:30.0\"}",
                "{\"peak_name\":\"Dhaulagiri I\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"resignation-someone-missing\",\"amount_people\":1,\"ets\":\"2024-04-02 12:17:16.0\",\"its\":\"2024-04-02 12:17:31.0\"}",
                "{\"peak_name\":\"Jengish Chokusu\",\"trip_leader\":\"Fred Beckey\",\"result\":\"base-reached\",\"amount_people\":2,\"ets\":\"2024-04-02 12:17:01.0\",\"its\":\"2024-04-02 12:17:31.0\"}",
                "{\"peak_name\":\"Noijin Kangsang\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"summit-reached\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:01.0\",\"its\":\"2024-04-02 12:17:32.0\"}",
                "{\"peak_name\":\"Jengish Chokusu\",\"trip_leader\":\"Junko Tabei\",\"result\":\"summit-reached\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:26.0\",\"its\":\"2024-04-02 12:17:39.0\"}",
                "{\"peak_name\":\"Putha Hiunchuli\",\"trip_leader\":\"Fred Beckey\",\"result\":\"resignation-weather\",\"amount_people\":9,\"ets\":\"2024-04-02 12:16:53.0\",\"its\":\"2024-04-02 12:17:33.0\"}",
                "{\"peak_name\":\"Nanga Parbat\",\"trip_leader\":\"Junko Tabei\",\"result\":\"resignation-someone-missing\",\"amount_people\":2,\"ets\":\"2024-04-02 12:17:09.0\",\"its\":\"2024-04-02 12:17:33.0\"}",
                "{\"peak_name\":\"Nanda Devi\",\"trip_leader\":\"Steve House\",\"result\":\"resignation-weather\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:07.0\",\"its\":\"2024-04-02 12:17:34.0\"}",
                "{\"peak_name\":\"Mount Shasta\",\"trip_leader\":\"Catherine Destivelle\",\"result\":\"resignation-injury\",\"amount_people\":6,\"ets\":\"2024-04-02 12:17:09.0\",\"its\":\"2024-04-02 12:17:34.0\"}",
                "{\"peak_name\":\"Makalu\",\"trip_leader\":\"Catherine Destivelle\",\"result\":\"resignation-other\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:00.0\",\"its\":\"2024-04-02 12:17:35.0\"}",
                "{\"peak_name\":\"Kumbhakarna\",\"trip_leader\":\"Steve House\",\"result\":\"resignation-weather\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:22.0\",\"its\":\"2024-04-02 12:17:35.0\"}",
                "{\"peak_name\":\"Siguang Ri\",\"trip_leader\":\"Catherine Destivelle\",\"result\":\"summit-reached\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:00.0\",\"its\":\"2024-04-02 12:17:36.0\"}",
                "{\"peak_name\":\"Dhaulagiri VI\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"resignation-weather\",\"amount_people\":8,\"ets\":\"2024-04-02 12:16:54.0\",\"its\":\"2024-04-02 12:17:36.0\"}",
                "{\"peak_name\":\"Nanga Parbat\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"resignation-someone-missing\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:12.0\",\"its\":\"2024-04-02 12:17:37.0\"}",
                "{\"peak_name\":\"Sherpi Kangri\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"resignation-other\",\"amount_people\":5,\"ets\":\"2024-04-02 12:17:09.0\",\"its\":\"2024-04-02 12:17:37.0\"}",
                "{\"peak_name\":\"Molamenqing\",\"trip_leader\":\"Caroline Gleich\",\"result\":\"resignation-injury\",\"amount_people\":6,\"ets\":\"2024-04-02 12:17:05.0\", \"its\":\"2024-04-02 12:17:38.0\"}",
                "{\"peak_name\":\"The Crown\",\"trip_leader\":\"George Mallory\",\"result\":\"resignation-injury\",\"amount_people\":3,\"ets\":\"2024-04-02 12:17:10.0\",\"its\":\"2024-04-02 12:17:38.0\"}",
                "{\"peak_name\":\"Dhaulagiri I\",\"trip_leader\":\"Conrad Anker\",\"result\":\"resignation-injury\",\"amount_people\":12,\"ets\":\"2024-04-02 12:17:37.0\",\"its\":\"2024-04-02 12:17:39.0\"}",
                "{\"peak_name\":\"Dhaulagiri II\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"summit-reached\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:29.0\",\"its\":\"2024-04-02 12:17:39.0\"}",
                "{\"peak_name\":\"Annapurna I\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"resignation-injury\",\"amount_people\":10,\"ets\":\"2024-04-02 12:17:00.0\",\"its\":\"2024-04-02 12:17:40.0\"}",
                "{\"peak_name\":\"Annapurna I\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"summit-reached\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:01.0\",\"its\":\"2024-04-02 12:17:40.0\"}",
                "{\"peak_name\":\"Saser Kangri II E\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"summit-reached\",\"amount_people\":4,\"ets\":\"2024-04-02 12:17:11.0\",\"its\":\"2024-04-02 12:17:41.0\"}",
                "{\"peak_name\":\"Teram Kangri III\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"base-reached\",\"amount_people\":3,\"ets\":\"2024-04-02 12:17:39.0\",\"its\":\"2024-04-02 12:17:41.0\"}",
                "{\"peak_name\":\"Annapurna I\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"base-reached\",\"amount_people\":6,\"ets\":\"2024-04-02 12:17:24.0\",\"its\":\"2024-04-02 12:17:42.0\"}",
                "{\"peak_name\":\"Annapurna II\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"resignation-other\",\"amount_people\":11,\"ets\":\"2024-04-02 12:17:39.0\",\"its\":\"2024-04-02 12:17:42.0\"}",
                "{\"peak_name\":\"Jomolhari\",\"trip_leader\":\"Junko Tabei\",\"result\":\"resignation-other\",\"amount_people\":7,\"ets\":\"2024-04-02 12:17:08.0\",\"its\":\"2024-04-02 12:17:43.0\"}",
                "{\"peak_name\":\"Kula Kangri\",\"trip_leader\":\"Caroline Gleich\",\"result\":\"base-reached\",\"amount_people\":5,\"ets\":\"2024-04-02 12:17:00.0\",\"its\":\"2024-04-02 12:17:43.0\"}",
                "{\"peak_name\":\"Muztagh Ata\",\"trip_leader\":\"George Mallory\",\"result\":\"resignation-weather\",\"amount_people\":10,\"ets\":\"2024-04-02 12:17:22.0\",\"its\":\"2024-04-02 12:17:44.0\"}",
                "{\"peak_name\":\"Nanga Parbat\",\"trip_leader\":\"Edmund Hillary\",\"result\":\"summit-reached\",\"amount_people\":4,\"ets\":\"2024-04-02 12:17:15.0\",\"its\":\"2024-04-02 12:17:44.0\"}",
                "{\"peak_name\":\"Labuche Kang\",\"trip_leader\":\"Fred Beckey\",\"result\":\"resignation-injury\",\"amount_people\":4,\"ets\":\"2024-04-02 12:17:13.0\",\"its\":\"2024-04-02 12:17:45.0\"}",
                "{\"peak_name\":\"The Crown\",\"trip_leader\":\"Conrad Anker\",\"result\":\"resignation-injury\",\"amount_people\":8,\"ets\":\"2024-04-02 12:17:35.0\",\"its\":\"2024-04-02 12:17:45.0\"}",
                "{\"peak_name\":\"The Crown\",\"trip_leader\":\"Sasha DiGiulian\",\"result\":\"base-reached\",\"amount_people\":12,\"ets\":\"2024-04-02 12:17:21.0\",\"its\":\"2024-04-02 12:17:46.0\"}",
                "{\"peak_name\":\"Mana Peak\",\"trip_leader\":\"Caroline Gleich\",\"result\":\"resignation-other\",\"amount_people\":12,\"ets\":\"2024-04-02 12:17:31.0\",\"its\":\"2024-04-02 12:17:46.0\"}",
                "{\"peak_name\":\"The Crown\",\"trip_leader\":\"Junko Tabei\",\"result\":\"resignation-weather\",\"amount_people\":9,\"ets\":\"2024-04-02 12:17:34.0\",\"its\":\"2024-04-02 12:17:47.0\"}",
                "{\"peak_name\":\"Kumbhakarna\",\"trip_leader\":\"Catherine Destivelle\",\"result\":\"resignation-someone-missing\",\"amount_people\":12,\"ets\":\"2024-04-02 12:17:39.0\",\"its\":\"2024-04-02 12:17:47.0\"}"

        };
    }
}

