package de.th.luebeck.jsn.casandra;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@ShellComponent
public class TestDataCreator {

    private static final Random rand = new Random();
    public static final String PASSWORD = "123456789Abc";

    private final String[] debtoragentbic = {"BKMADE61XXX", "HASPDEHHXXX", "PBNKDEFF", "DEUTDEBBXXX", "DEUTDEBB", "CMCIDEDD", "HSHNDEHH", "HSHNDEHHXXX", "SOLADEST", "SOLADESTXXX"};
    private final String[] intbicdebic = {"SCFBDE33XXX", "GENODEFF120", "DGZFDEFFBER", "HYVEDEMM488", "HSTBDEHHXXX", "COBADEBB120", "GENODEF1OGK", "EIEGDEB1XXX", "BELADEBEXXX", "GENODED1PA6"};
    private final String[] messageId = {"EXT_106504", "EXT_104711", "EXT_100815", "EXT_109432", "EXT_102796", "EXT_105632", "EXT_101001", "EXT_102100", "EXT_109843", "EXT_1097464"};
    private final String[] instructionId = {"InstrId", "CdtTrfTxInf", "PmtId", "CdtfTxdfInf", "CdtTrfTxInf", "CdtTrfTxInf", "PmtId", "CdtTrfTx", "PmtInstId", "InsPmtId"};
    private final String[] intaccdebig = {"DE41500105170123456789", "FO9264600123456789", "IT60X0542811101000000123456", "KW81CBKU0000000000001234560101", "ME25505000012345678951", "PS92PALS000000000400123456702", "PT50002700000001234567833", "SE1412345678901234567890", "TN4401000067123456789123", "GL8964710123456789"};
    private final String[] tntacccredit = {"SE1412345678901234567890", "DE41500105170123456789", "TN4401000067123456789123", "PS92PALS000000000400123456702", "FO9264600123456789", "ME25505000012345678951", "IT60X0542811101000000123456", "KW81CBKU0000000000001234560101", "PT50002700000001234567833", "MT31MALT01100000000000000000123"};
    private final String[] visualdata = {"A=Donald;B=Daiys;C=Dagobert;", "A=test;B=Donalt;C=4;", "A=1;B=2;C=Tick;", "A=Trick;B=2;C=4;", "A=1;B=Dagobert;C=4;", "A=Tick;B=Trick;C=Track;", "A=Donald;B=2;C=Duck;", "A=1;B=Daisy;C=Duck;", "A=1;B=Entenhausen;C=4;", "A=Gustav;B=Gans;C=4;"};

    private final String paymentData = "hdgfikjsdfghaÃ¶lughÃ¶ufghÃ¶jehtguoraebvÃ¶oereijbsÃ¶khugqÃ¶qÃ¶qÃ¶qÃ¶ghÃ¶eaobvbvbvbvbvbvbvbvbvbvbvbvbvbvraÃ¼eovboafebgÃ¼oaebfrvgoeqobfqÃ¼eoruvbeqjfebvggeuirgueqqiuprpghrpizbgirqehpiqzbfgiqurpqefbvgqzpigfqpi3ugibvweqzpiÃ¼9iufbgiq94ztr9qpbfg794t197fg";

    @ShellMethod("Erstellt Testdaten in der lokalen Cassandra Datenbank")
    public void createTestData(@ShellOption(value = {"-a", "--anzahl"}, defaultValue = "10", help = "Anzahl der zu erstellenden Testdatensätze") final int totalTestDataCound,
                               @ShellOption(value = {"-s", "--start-index"}, defaultValue = "0", help = "Möglichkeit zum Setzen des Beginns der Indexierung") final int startIndex) {

        System.out.println("Starte Testdatenerstellung für " + totalTestDataCound + " Datansätze.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL))
                    .withPoolingOptions(new PoolingOptions())
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();                                           // (2)

            final ResultSet rs = session.execute("select release_version from system.local");    // (3)
            final Row row = rs.one();
            System.out.println(row.getString("release_version"));


            final PreparedStatement preparedPayment = session.prepare(
                    "INSERT INTO wp.PAYMENT (\n" +
                            "                     PK_ID\n" +
                            "                    ,TRANSACTIONID\n" +
                            "                    ,DEBTORAGENTBIC\n" +
                            "                    ,ACCEPTED\n" +
                            "                    ,SETTLEMENT\n" +
                            "                    ,INTACCDEBIT\n" +
                            "                    ,INTACCCREDIT\n" +
                            "                    ,INTBICDEBIC\n" +
                            "                    ,INSTRUCTIONID\n" +
                            "                    ,AMNT\n" +
                            "                    ,EXTERNAL_ID\n" +
                            "                    ,MESSAGE_ID\n" +
                            "                    ,TIMETOLIVE\n" +
                            "                    ,VISUAL_DATA) \n" +
                            "                VALUES (\n" +
                            "                     ?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?)");

            final PreparedStatement inserQueryOne = session.prepare(
                    "INSERT INTO wp.queryOne (\n" +
                            "                     PK_ID\n" +
                            "                    ,TRANSACTIONID\n" +
                            "                    ,DEBTORAGENTBIC\n" +
                            "                    ,SETTLEMENT)\n" +
                            "                VALUES (\n" +
                            "                     ?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?)");

            final PreparedStatement inserQueryTwo = session.prepare(
                    "INSERT INTO wp.queryTwo (\n" +
                            "                     PK_ID\n" +
                            "                    ,INTACCDEBIT\n" +
                            "                    ,SETTLEMENT)\n" +
                            "                VALUES (\n" +
                            "                     ?\n" +
                            "                    ,?\n" +
                            "                    ,?)");

            final PreparedStatement inserQueryThree = session.prepare(
                    "INSERT INTO wp.queryThree (\n" +
                            "                     PK_ID\n" +
                            "                    ,TRANSACTIONID\n" +
                            "                    ,SETTLEMENT\n" +
                            "                    ,INTACCDEBIT\n" +
                            "                    ,INTACCCREDIT\n" +
                            "                    ,INTBICDEBIC\n" +
                            "                    ,INSTRUCTIONID\n" +
                            "                    ,AMNT\n" +
                            "                    ,MESSAGE_ID)\n" +
                            "                VALUES (\n" +
                            "                     ?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?\n" +
                            "                    ,?)");


            final LocalDate today = LocalDate.now();

            long startMillies = System.currentTimeMillis();

            int currentIndex = startIndex;

            while (currentIndex < totalTestDataCound) {

                try {

                    List<ResultSetFuture> asyncWrites = new ArrayList<>(1100);

                    for (; currentIndex < totalTestDataCound; currentIndex++) {

                        int i = currentIndex;
                        double rand = Math.random();

                        // NICE jsn: Eigentlich sollten die Daten für jede Tabelle gleich sein. Für die Verteilung der Testdaten ändert es nichts. Es macht nur keinen Sinn.
                        asyncWrites.add(session.executeAsync(preparedPayment.bind(
                                i,
                                "TRX" + i,
                                debtoragentbic[getRandom(debtoragentbic.length - 1)],
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.minusDays((long) (rand * 1.0)).toEpochDay()),
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.minusDays((long) (rand * 397.0)).toEpochDay()),
                                intaccdebig[getRandom(intaccdebig.length - 1)],
                                tntacccredit[getRandom(tntacccredit.length - 1)],
                                intbicdebic[getRandom(intbicdebic.length - 1)],
                                instructionId[getRandom(instructionId.length - 1)],
                                100000 * rand,
                                "EXTID_" + i,
                                messageId[getRandom(messageId.length - 1)],
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.plusDays((long) (750.0 * rand)).toEpochDay()),
                                visualdata[getRandom(visualdata.length - 1)])));

                        asyncWrites.add(session.executeAsync(inserQueryOne.bind(
                                i,
                                "TRX" + i,
                                debtoragentbic[getRandom(debtoragentbic.length - 1)],
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.minusDays((long) (rand * 397.0)).toEpochDay()))));

                        asyncWrites.add(session.executeAsync(inserQueryTwo.bind(
                                i,
                                intaccdebig[getRandom(intaccdebig.length - 1)],
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.minusDays((long) (rand * 397.0)).toEpochDay()))));

                        asyncWrites.add(session.executeAsync(inserQueryThree.bind(
                                i,
                                "TRX" + i,
                                com.datastax.driver.core.LocalDate.fromDaysSinceEpoch((int) today.minusDays((long) (rand * 397.0)).toEpochDay()),
                                intaccdebig[getRandom(intaccdebig.length - 1)],
                                tntacccredit[getRandom(tntacccredit.length - 1)],
                                intbicdebic[getRandom(intbicdebic.length - 1)],
                                instructionId[getRandom(instructionId.length - 1)],
                                100000 * rand,
                                messageId[getRandom(messageId.length - 1)])));

                        if (currentIndex % 1000 == 0) {
                            System.out.println("Index " + currentIndex + " at " + LocalDateTime.now());
                        }

                        if (asyncWrites.size() >= 1000) {
                            System.out.println("Waiting for async calls to finish.");
                            for (ResultSetFuture resultSetFuture : asyncWrites) {
                                while (!resultSetFuture.isDone()) {
                                    Thread thread = Thread.currentThread();
                                    synchronized (thread) {
                                        thread.wait(100);
                                    }
                                }
                                if (!resultSetFuture.get().wasApplied()) {
                                    System.err.println("Was not applied.");
                                }
                            }
                            asyncWrites.clear();
                        }
                    }
                } catch (Exception ex) {
                    System.err.println("Ignoring Exception possible timeout. Index " + currentIndex);
                    System.err.println(ex.getMessage());
                    ex.printStackTrace();
                }
            }
            long endMillis = System.currentTimeMillis();

            long diffMillis = endMillis - startMillies;
            System.out.println("Testdatenerstellung beendet. Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

            // (4)
        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Erstelle keyspace")
    public void createKeySpace(@ShellOption(value = {"-r", "--replication"}, defaultValue = "1", help = "Anzahl der Replikationen") final int replications) {


        System.out.println("Lösche Keyspace.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();                                           // (2)

            final ResultSet rs = session.execute("select release_version from system.local");    // (3)
            final Row row = rs.one();
            System.out.println(row.getString("release_version"));

            session.execute("CREATE KEYSPACE wp\n" +
                    "        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : " + replications + "};");

        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Erstelle Schema")
    public void createSchema() {
        System.out.println("Starte Testschemaerstellung.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();                                           // (2)

            final ResultSet rs = session.execute("select release_version from system.local");    // (3)
            final Row row = rs.one();
            System.out.println(row.getString("release_version"));

            session.execute("CREATE TABLE wp.PAYMENT \n" +
                    "(\n" +
                    "\t PK_ID    int PRIMARY KEY\n" +
                    "\t,TRANSACTIONID    varchar\n" +
                    "\t,DEBTORAGENTBIC    varchar\n" +
                    "\t,ACCEPTED    DATE\n" +
                    "\t,SETTLEMENT    DATE\n" +
                    "\t,INTACCDEBIT    varchar\n" +
                    "\t,INTACCCREDIT    varchar\n" +
                    "\t,INTBICDEBIC    varchar\n" +
                    "\t,INSTRUCTIONID    varchar\n" +
                    "\t,AMNT    double\n" +
                    "\t,EXTERNAL_ID    varchar\n" +
                    "\t,MESSAGE_ID    varchar\n" +
                    "\t,TIMETOLIVE    DATE\n" +
                    "\t,VISUAL_DATA    varchar\n" +
                    ");");

            session.execute("CREATE TABLE wp.queryOne \n" +
                    "(\n" +
                    "\t PK_ID    int\n" +
                    "\t,TRANSACTIONID    varchar\n" +
                    "\t,DEBTORAGENTBIC    varchar\n" +
                    "\t,SETTLEMENT    DATE\n" +
                    ",PRIMARY KEY (SETTLEMENT, TRANSACTIONID,DEBTORAGENTBIC)" +
                    ");");

            session.execute("CREATE TABLE wp.queryTwo \n" +
                    "(\n" +
                    "\t PK_ID    int\n" +
                    "\t,INTACCDEBIT    varchar\n" +
                    "\t,SETTLEMENT    DATE\n" +
                    ",PRIMARY KEY ((PK_ID, INTACCDEBIT),SETTLEMENT)" +
                    ");");

            session.execute("CREATE TABLE wp.queryThree \n" +
                    "(\n" +
                    "\t PK_ID    int\n" +
                    "\t,TRANSACTIONID    varchar\n" +
                    "\t,SETTLEMENT    DATE\n" +
                    "\t,INTACCDEBIT    varchar\n" +
                    "\t,INTACCCREDIT    varchar\n" +
                    "\t,INTBICDEBIC    varchar\n" +
                    "\t,INSTRUCTIONID    varchar\n" +
                    "\t,AMNT    double\n" +
                    "\t,MESSAGE_ID    varchar\n" +
                    ",PRIMARY KEY ((INTACCDEBIT, INSTRUCTIONID, TRANSACTIONID, INTACCCREDIT, INTBICDEBIC, MESSAGE_ID),SETTLEMENT,AMNT)" +
                    ");");

        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }

    @ShellMethod("Lösche Inhalt")
    public void deleteData() {
        System.out.println("Lösche Tablleninhalte.");

        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();                                           // (2)

            final ResultSet rs = session.execute("select release_version from system.local");    // (3)
            final Row row = rs.one();
            System.out.println(row.getString("release_version"));

            session.execute("truncate wp.payment;");
            session.execute("truncate wp.queryOne;");
            session.execute("truncate wp.queryTwo;");
            session.execute("truncate wp.queryThree;");

        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }

    }

    @ShellMethod("Lösche Tabellen")
    public void dropTables() {

        System.out.println("Lösche Tabllen.");


        Cluster cluster = null;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .withCredentials("cassandra", PASSWORD)
                    .build();
            final Session session = cluster.connect();                                           // (2)

            final ResultSet rs = session.execute("select release_version from system.local");    // (3)
            final Row row = rs.one();
            System.out.println(row.getString("release_version"));

            session.execute("drop table wp.payment;");
            session.execute("drop table wp.queryOne;");
            session.execute("drop table wp.queryTwo;");
            session.execute("drop table wp.queryThree;");

            // session.execute("drop table wp.payment;");

        } finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
    }


    private int getRandom(int max) {
        return rand.nextInt(max);
    }

}
