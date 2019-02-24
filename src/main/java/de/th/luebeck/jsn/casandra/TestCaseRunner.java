package de.th.luebeck.jsn.casandra;

import com.datastax.driver.core.*;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


@ShellComponent
public class TestCaseRunner {

    public static final int NUMBER_OF_RUNS = 10;
    public static final String PASSWORD = "123456789Abc";

    @ShellMethod("Führe all Test durch")
    public void runAllTest() {
        runTestQueryOne("QUORUM");
        runTestQueryTwoOneDay("QUORUM");
        runTestQueryTwoTwoDays("QUORUM");
        runTestQueryTwoOneMonth("QUORUM");
        runTestQueryThreeOneDay("QUORUM");
    }

    @ShellMethod("Führt den Test für Query One durch")
    public void runTestQueryOne(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {

        System.out.println("Führe Test Query One aus.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryOne = session.prepare(
                    "SELECT PK_ID from wp.queryOne WHERE DEBTORAGENTBIC = ?\n" +
                            "                                                AND   TRANSACTIONID = ?\n" +
                            "                                                AND   SETTLEMENT = ?");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query One Params
            final String[] debtorBic = {"MARKDEF1500", "DEUTDEFFXXX", "COBADEFFXXX", "MELIDEHHXXX", "NTSBDEB1XXX"};
            final String[] transacionId = {"TRX1082085", "TRX57492", "TRX258879", "TRX1152936", "TRX557001"};
            final int[][] date = {{2017, 02, 01},
                    {2018, 05, 27},
                    {2017, 12, 06},
                    {2018, 02, 27},
                    {2017, 9, 03}
            };

            System.out.println("Abfrage 1. Debtoragentbic, Transactionsid, Settlement.");

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryOne.bind(debtorBic[i % 5], transacionId[i % 5], LocalDate.fromYearMonthDay(date[i % 5][0], date[i % 5][1], date[i % 5][2])));

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q1");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Two")
    public void runTestQueryTwoOneDay(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT >= ? and SETTLEMENT <= ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"GT20AGRO00000000001234567890", "DE89370400440532013000", "FR7630006000011234567890189", "MK07200002785123453", "FO9264600123456789"};
            final int[][] dateFrom = {
                    {2017, 9, 9},
                    {2018, 6, 7},
                    {2017, 8, 19},
                    {2017, 11, 13},
                    {2018, 3, 1}
            };

            final int[][] dateTo = {
                    {2017, 9, 9},
                    {2018, 6, 7},
                    {2017, 8, 19},
                    {2017, 11, 13},
                    {2018, 3, 1}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryTwo.bind(LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]),
                        LocalDate.fromYearMonthDay(dateTo[i % 5][0], dateTo[i % 5][1], dateTo[i % 5][2]),
                        icdeb[i % 5])
                );

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 1 Tag");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Two - 2 Tage")
    public void runTestQueryTwoTwoDays(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT >= ? and SETTLEMENT <= ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"CY21002001950000357001234567'", "UA903052992990004149123456789", "SM76P0854009812123456789123", "BR1500000000000010932840814P2", "HU93116000060000000012345676"};
            final int[][] dateFrom = {
                    {2017, 3, 9},
                    {2018, 9, 7},
                    {2017, 11, 19},
                    {2017, 4, 23},
                    {2018, 6, 1}
            };

            final int[][] dateTo = {
                    {2017, 3, 10},
                    {2018, 9, 8},
                    {2017, 11, 20},
                    {2017, 4, 24},
                    {2018, 6, 2}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryTwo.bind(LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]),
                        LocalDate.fromYearMonthDay(dateTo[i % 5][0], dateTo[i % 5][1], dateTo[i % 5][2]),
                        icdeb[i % 5])
                );

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 2 Tag");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }


    @ShellMethod("Führt den Test für Query Two - 7 Tage")
    public void runTestQueryTwoOneWeek(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {


        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT >= ? and SETTLEMENT <= ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"JO71CBJO0000000000001234567890'", "LC14BOSL123456789012345678901234", "PL10105000997603123456789123", "MD21EX000000000001234567", "QA54QNBA000000000000693123456"};
            final int[][] dateFrom = {
                    {2017, 8, 9},
                    {2018, 8, 7},
                    {2017, 10, 19},
                    {2017, 7, 23},
                    {2018, 5, 1}
            };

            final int[][] dateTo = {
                    {2017, 8, 16},
                    {2018, 8, 14},
                    {2017, 10, 26},
                    {2017, 7, 30},
                    {2018, 5, 8}
            };


            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryTwo.bind(LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]),
                        LocalDate.fromYearMonthDay(dateTo[i % 5][0], dateTo[i % 5][1], dateTo[i % 5][2]),
                        icdeb[i % 5])
                );

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 7 Tage");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Two - 1 Monat")
    public void runTestQueryTwoOneMonth(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT >= ? and SETTLEMENT <= ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"GE60NB0000000123456789'", "LV97HABA0012345678910", "RS35105008123123123173", "NO8330001234567", "FR7630006000011234567890189"};
            final int[][] dateFrom = {
                    {2017, 8, 1},
                    {2018, 6, 7},
                    {2017, 9, 19},
                    {2017, 2, 23},
                    {2017, 12, 22}
            };

            final int[][] dateTo = {
                    {2017, 8, 30},
                    {2018, 7, 7},
                    {2017, 10, 19},
                    {2017, 3, 23},
                    {2018, 1, 22}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryTwo.bind(LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]),
                        LocalDate.fromYearMonthDay(dateTo[i % 5][0], dateTo[i % 5][1], dateTo[i % 5][2]),
                        icdeb[i % 5])
                );

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 - 1 Monat");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Three - 1 Tag")
    public void runTestQueryThreeOneDay(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {

        System.out.println("Führe Test Query Three aus - 1 Tag.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryThree = session.prepare(
                    "SELECT PK_ID from wp.queryThree WHERE SETTLEMENT >= ? and SETTLEMENT <= ?\n" +
                            "                                                AND   INTACCDEBIT = ?" +
                            "                                                AND   INSTRUCTIONID = ?" +
                            "                                                AND   TRANSACTIONID = ?" +
                            "                                                AND   INTACCCREDIT = ?" +
                            "                                                AND   AMNT >= ? and AMNT <= ?" +
                            "                                                AND   INTBICDEBIC = ?" +
                            "                                                AND   MESSAGE_ID = ?" +
                            " allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Three Params
            final String[] intaccdebit = {"MR1300020001010000123456753'", "HU93116000060000000012345676", "LT601010012345678901", "GL8964710123456789", "TL380080012345678910157"};
            final String[] transactionid = {"TRX1132483'", "TRX1187421", "TRX1045865", "TRX1048654", "TRX126016"};
            final String[] instructionid = {"PmtId'", "PmtId", "CdtTrfTxInf", "InstrId", "InstrId"};
            final String[] messageid = {"MSG_1132483'", "MSG_1187421", "MSG_1045865", "MSG_1048654", "MSG_126016"};
            final String[] intaccredit = {"BH02CITI00001077181611'", "KW81CBKU0000000000001234560101", "AL35202111090000000001234567", "CY21002001950000357001234567", "VG21PACG0000000123456789"};
            final String[] intbicdebic = {"SCFBDE33XXX'", "COBADEFFXXX", "SKPADEB1XXX", "BFSWDE33BER", "GENODEFFXXX"};
            final double[] amnt_min = {100.7, 1.0, 99.3, 356432.9, 438.01};
            final double[] amnt_max = {5.0, 1000.6, 4532.7, 5483875.0, 765438.01};

            final int[][] dateFrom = {
                    {2017, 7, 9},
                    {2017, 5, 7},
                    {2018, 3, 19},
                    {2017, 11, 23},
                    {2015, 5, 1}
            };

            final int[][] dateTo = {
                    {2017, 7, 9},
                    {2017, 5, 7},
                    {2018, 3, 19},
                    {2017, 11, 23},
                    {2015, 5, 1}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                final ResultSet execute = session.execute(preparedSelectQueryThree.bind(LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]),
                        LocalDate.fromYearMonthDay(dateTo[i % 5][0], dateTo[i % 5][1], dateTo[i % 5][2]),
                        intaccdebit[i % 5],
                        instructionid[i % 5],
                        transactionid[i % 5],
                        intaccredit[i % 5],
                        amnt_min[i % 5],
                        amnt_max[i % 5],
                        intbicdebic[i % 5],
                        messageid[i % 5]
                        )
                );

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Count of retrieved rows: " + execute.all().size());
                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }
            System.out.println("Ergebnis Q3 - 1 Tag");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");


        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Two - 7 Tage - Async")
    public void runAsyncOneWeek(@ShellOption(value = {"-c", "--consistency"}, defaultValue = "QUORUM") final String consistencyLevel) {


        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT = ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"JO71CBJO0000000000001234567890'", "LC14BOSL123456789012345678901234", "PL10105000997603123456789123", "MD21EX000000000001234567", "QA54QNBA000000000000693123456"};
            final int[][] dateFrom = {
                    {2017, 8, 9},
                    {2018, 8, 7},
                    {2017, 10, 19},
                    {2017, 7, 23},
                    {2018, 5, 1}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                List<ResultSetFuture> resultSetFutures = new ArrayList<>(7);

                LocalDate from = LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]);
                for (int t = 0; t < 7; t++) {


                    resultSetFutures.add(session.executeAsync(preparedSelectQueryTwo.bind(from.add(Calendar.DATE, t),
                            icdeb[i % 5])
                    ));
                }

                for(ResultSetFuture future : resultSetFutures){
                    while(!future.isDone()){
                        Thread thread = Thread.currentThread();
                        synchronized (thread){
                            try {
                                thread.wait(2);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 7 Tage - Asynchron");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Führt den Test für Query Two - 28 Tage - Async")
    public void runAsyncOneMonth() {


        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();

            final PreparedStatement preparedSelectQueryTwo = session.prepare(
                    "SELECT PK_ID from wp.queryTwo WHERE SETTLEMENT = ?\n" +
                            "                                                AND   INTACCDEBIT = ? allow filtering");


            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

            // Query Two Params
            final String[] icdeb = {"GE60NB0000000123456789'", "LV97HABA0012345678910", "RS35105008123123123173", "NO8330001234567", "FR7630006000011234567890189"};
            final int[][] dateFrom = {
                    {2017, 8, 1},
                    {2018, 6, 7},
                    {2017, 9, 19},
                    {2017, 2, 23},
                    {2017, 12, 22}
            };

            for (int i = 0; i < NUMBER_OF_RUNS; i++) {


                long startMillies = System.currentTimeMillis();

                List<ResultSetFuture> resultSetFutures = new ArrayList<>(7);

                LocalDate from = LocalDate.fromYearMonthDay(dateFrom[i % 5][0], dateFrom[i % 5][1], dateFrom[i % 5][2]);
                for (int t = 0; t < 28; t++) {

                    resultSetFutures.add(session.executeAsync(preparedSelectQueryTwo.bind(from.add(Calendar.DATE, t),
                            icdeb[i % 5])
                    ));
                }

                for(ResultSetFuture future : resultSetFutures){
                    while(!future.isDone()){
                        Thread thread = Thread.currentThread();
                        synchronized (thread){
                            try {
                                thread.wait(2);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");
            }

            System.out.println("Ergebnis Q2 7 Tage - Asynchron");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / NUMBER_OF_RUNS / 1000 + " Sekunden, " + allMillies / NUMBER_OF_RUNS % 1000 + " allMillies.");
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }
}
