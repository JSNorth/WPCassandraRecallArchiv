package de.th.luebeck.jsn.casandra;

import com.datastax.driver.core.ResultSetFuture;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;

@ShellComponent
public class SqlTestRunner {

    private static final Random rand = new Random();

    private final String[] debtoragentbic = {"BKMADE61XXX", "HASPDEHHXXX", "PBNKDEFF", "DEUTDEBBXXX", "DEUTDEBB", "CMCIDEDD", "HSHNDEHH", "HSHNDEHHXXX", "SOLADEST", "SOLADESTXXX"};
    private final String[] intbicdebic = {"SCFBDE33XXX", "GENODEFF120", "DGZFDEFFBER", "HYVEDEMM488", "HSTBDEHHXXX", "COBADEBB120", "GENODEF1OGK", "EIEGDEB1XXX", "BELADEBEXXX", "GENODED1PA6"};
    private final String[] messageId = {"EXT_106504", "EXT_104711", "EXT_100815", "EXT_109432", "EXT_102796", "EXT_105632", "EXT_101001", "EXT_102100", "EXT_109843", "EXT_1097464"};
    private final String[] instructionId = {"InstrId", "CdtTrfTxInf", "PmtId", "CdtfTxdfInf", "CdtTrfTxInf", "CdtTrfTxInf", "PmtId", "CdtTrfTx", "PmtInstId", "InsPmtId"};
    private final String[] intaccdebig = {"DE41500105170123456789", "FO9264600123456789", "IT60X0542811101000000123456", "KW81CBKU0000000000001234560101", "ME25505000012345678951", "PS92PALS000000000400123456702", "PT50002700000001234567833", "SE1412345678901234567890", "TN4401000067123456789123", "GL8964710123456789"};
    private final String[] tntacccredit = {"SE1412345678901234567890", "DE41500105170123456789", "TN4401000067123456789123", "PS92PALS000000000400123456702", "FO9264600123456789", "ME25505000012345678951", "IT60X0542811101000000123456", "KW81CBKU0000000000001234560101", "PT50002700000001234567833", "MT31MALT01100000000000000000123"};
    private final String[] visualdata = {"A=Donald;B=Daiys;C=Dagobert;", "A=test;B=Donalt;C=4;", "A=1;B=2;C=Tick;", "A=Trick;B=2;C=4;", "A=1;B=Dagobert;C=4;", "A=Tick;B=Trick;C=Track;", "A=Donald;B=2;C=Duck;", "A=1;B=Daisy;C=Duck;", "A=1;B=Entenhausen;C=4;", "A=Gustav;B=Gans;C=4;"};

    @ShellMethod("Erstellt die SQL Datenbank")
    public void initSqlDatabase() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            // Statements allow to issue SQL queries to the database
            Statement statement = connect.createStatement();

            statement.execute("CREATE TABLE wp.PAYMENT \n" +
                    "(\n" +
                    "\t PK_ID    int PRIMARY KEY\n" +
                    "\t,TRANSACTIONID    varchar(35)\n" +
                    "\t,DEBTORAGENTBIC    varchar(11)\n" +
                    "\t,ACCEPTED    DATE\n" +
                    "\t,SETTLEMENT    DATE\n" +
                    "\t,INTACCDEBIT    varchar(34)\n" +
                    "\t,INTACCCREDIT    varchar(34)\n" +
                    "\t,INTBICDEBIC    varchar(11)\n" +
                    "\t,INSTRUCTIONID    varchar(35)\n" +
                    "\t,AMNT    double\n" +
                    "\t,EXTERNAL_ID    varchar(35)\n" +
                    "\t,MESSAGE_ID    varchar(35)\n" +
                    "\t,TIMETOLIVE    DATE\n" +
                    "\t,VISUAL_DATA    varchar(500)\n" +
                    ");");

            statement.execute("CREATE INDEX query_two_index \n" +
                    "ON wp.PAYMENT (INTACCDEBIT, SETTLEMENT);");

            statement.execute("commit;");


        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        } finally {

        }
    }

    @ShellMethod("Erzeugt die SQL Testdaten")
    public void createSqlData(@ShellOption(value = {"-a", "--anzahl"}, defaultValue = "10", help = "Anzahl der zu erstellenden Testdatensätze") final int totalTestDataCound,
                              @ShellOption(value = {"-s", "--start-index"}, defaultValue = "0", help = "Möglichkeit zum Setzen des Beginns der Indexierung") final int startIndex) {


        final LocalDate today = LocalDate.now();

        long startMillies = System.currentTimeMillis();

        int currentIndex = startIndex;

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            ComboPooledDataSource cpds = new ComboPooledDataSource();
            cpds.setDriverClass("com.mysql.cj.jdbc.Driver"); //loads the jdbc driver
            cpds.setJdbcUrl("jdbc:mysql://localhost/wp?"
                    + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
            // cpds.setUser("swaldman");
            // cpds.setPassword("test-password");

            // the settings below are optional -- c3p0 can work with defaults
            cpds.setMinPoolSize(20);
            cpds.setAcquireIncrement(5);
            cpds.setMaxPoolSize(20);

            while (currentIndex < totalTestDataCound) {

                try {

                    List<Future<Integer>> asyncWrites = new ArrayList<>(25);


                    for (; currentIndex < totalTestDataCound; currentIndex++) {

                        int i = currentIndex;

                        insertAsync(today, i, cpds);

                        if (currentIndex % 1000 == 0) {
                            System.out.println("Index " + currentIndex + " at " + LocalDateTime.now());
                        }

                        /*if (asyncWrites.size() >= 20) {
                            System.out.println("Waiting for async calls to finish.");
                            for (Future<Integer> resultSetFuture : asyncWrites) {
                                while (!resultSetFuture.isDone()) {
                                    Thread thread = Thread.currentThread();
                                    synchronized (thread) {
                                        thread.wait(100);
                                    }
                                }
                                if (!resultSetFuture.get().equals(1)) {
                                    System.err.println("Was not applied.");
                                }
                            }
                            asyncWrites.clear();
                        }*/
                    }
                } catch (Exception ex) {
                    System.err.println(ex.getMessage());
                    System.err.println("Ignoring Exception possible timeout. Index " + currentIndex);
                }
            }

            long endMillis = System.currentTimeMillis();

            long diffMillis = endMillis - startMillies;
            System.out.println("Testdatenerstellung beendet. Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

        } catch (Exception ex) {
            System.err.println(ex.getMessage());

        }
    }

    private PreparedStatement getPreparedStatement(Connection connect) throws SQLException {

        return connect
                .prepareStatement("INSERT INTO wp.PAYMENT (\n" +
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
    }

    @Async("threadPoolTaskExecutor")
    public Future<Integer> insertAsync(LocalDate today, int i, ComboPooledDataSource pool) throws SQLException {
        double rand = Math.random();

        Connection connect = pool.getConnection();
        try {
            PreparedStatement preparedStatement = getPreparedStatement(connect);
            preparedStatement.setString(1, Integer.toString(i));
            preparedStatement.setString(2, "TRX" + i);
            preparedStatement.setString(3, debtoragentbic[getRandom(debtoragentbic.length - 1)]);
            preparedStatement.setDate(4, java.sql.Date.valueOf(today.minusDays((long) (rand * 1.0))));
            preparedStatement.setDate(5, java.sql.Date.valueOf(today.minusDays((long) (rand * 397.0))));
            preparedStatement.setString(6, intaccdebig[getRandom(intaccdebig.length - 1)]);
            preparedStatement.setString(7, tntacccredit[getRandom(tntacccredit.length - 1)]);
            preparedStatement.setString(8, intbicdebic[getRandom(intbicdebic.length - 1)]);
            preparedStatement.setString(9, instructionId[getRandom(instructionId.length - 1)]);
            preparedStatement.setDouble(10, 100000 * rand);
            preparedStatement.setString(11, "EXTID_" + i);
            preparedStatement.setString(12, messageId[getRandom(messageId.length - 1)]);
            preparedStatement.setDate(13, java.sql.Date.valueOf(today.plusDays((long) (750.0 * rand))));
            preparedStatement.setString(14, visualdata[getRandom(visualdata.length - 1)]);

            int effectedRows = preparedStatement.executeUpdate();

            return new AsyncResult<Integer>(effectedRows);
        } finally {
            if (connect != null) {
                connect.close();
            }
        }
    }

    @ShellMethod("Lösche die Daten aus der Tabelle")
    public void deleteSqlData() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            // Statements allow to issue SQL queries to the database
            Statement statement = connect.createStatement();

            statement.execute("delete from wp.PAYMENT");
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @ShellMethod("Führt alle Tests nacheinander aus")
    public void runSqlAllTests() {
        runSqlTestOneDay();
        runSqlTestTwoDays();
        runSqlTestOneWeek();
        runSqlTestOneMonth();
    }

    @ShellMethod("Führt den SQL Test durch - 1 Tag")
    public void runSqlTestOneDay() {

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            PreparedStatement preparedStatement = connect
                        .prepareStatement("SELECT PK_ID FROM wp.PAYMENT WHERE SETTLEMENT between  ? and ?  AND   INTACCDEBIT = ?");

            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

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

            for (int i = 0; i < 250; i++) {

                long startMillies = System.currentTimeMillis();

                preparedStatement.setDate(1, new Date(dateFrom[i % 5][0] - 1900, dateFrom[i % 5][1], dateFrom[i % 5][2]));
                preparedStatement.setDate(2, new Date(dateTo[i % 5][0] - 1900, dateTo[i % 5][1], dateTo[i % 5][2]));
                preparedStatement.setString(3, icdeb[i % 5]);
                preparedStatement.executeQuery();

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

            }

            System.out.println("Ergebnis Abfrage SQL - 1 Tag.");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / 250 / 1000 + " Sekunden, " + allMillies / 250 % 1000 + " allMillies.");


        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @ShellMethod("Führt den SQL Test durch - 2 Tage")
    public void runSqlTestTwoDays() {

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            PreparedStatement preparedStatement = connect
                    .prepareStatement("SELECT PK_ID FROM wp.PAYMENT WHERE SETTLEMENT between  ? and ?  AND   INTACCDEBIT = ?");

            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

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

            for (int i = 0; i < 250; i++) {

                long startMillies = System.currentTimeMillis();

                preparedStatement.setDate(1, new Date(dateFrom[i % 5][0] - 1900, dateFrom[i % 5][1], dateFrom[i % 5][2]));
                preparedStatement.setDate(2, new Date(dateTo[i % 5][0] - 1900, dateTo[i % 5][1], dateTo[i % 5][2]));
                preparedStatement.setString(3, icdeb[i % 5]);
                preparedStatement.executeQuery();

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

            }

            System.out.println("Ergebnis Abfrage SQL - 2 Tag.");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / 250 / 1000 + " Sekunden, " + allMillies / 250 % 1000 + " allMillies.");


        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @ShellMethod("Führt den SQL Test durch - 7 Tage")
    public void runSqlTestOneWeek() {

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            PreparedStatement preparedStatement = connect
                    .prepareStatement("SELECT PK_ID FROM wp.PAYMENT WHERE SETTLEMENT between  ? and ?  AND   INTACCDEBIT = ?");

            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

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

            for (int i = 0; i < 250; i++) {

                long startMillies = System.currentTimeMillis();

                preparedStatement.setDate(1, new Date(dateFrom[i % 5][0] - 1900, dateFrom[i % 5][1], dateFrom[i % 5][2]));
                preparedStatement.setDate(2, new Date(dateTo[i % 5][0] - 1900, dateTo[i % 5][1], dateTo[i % 5][2]));
                preparedStatement.setString(3, icdeb[i % 5]);
                preparedStatement.executeQuery();

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

            }

            System.out.println("Ergebnis Abfrage SQL - 7 Tag.");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / 250 / 1000 + " Sekunden, " + allMillies / 250 % 1000 + " allMillies.");


        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    @ShellMethod("Führt den SQL Test durch - 28 Tage")
    public void runSqlTestOneMonth() {

        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://localhost/wp?"
                            + "user=test&password=test&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");

            PreparedStatement preparedStatement = connect
                    .prepareStatement("SELECT PK_ID FROM wp.PAYMENT WHERE SETTLEMENT between  ? and ?  AND   INTACCDEBIT = ?");

            long maxMillis = Long.MIN_VALUE;
            long minMillies = Long.MAX_VALUE;
            long allMillies = 0;

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

            for (int i = 0; i < 250; i++) {

                long startMillies = System.currentTimeMillis();

                preparedStatement.setDate(1, new Date(dateFrom[i % 5][0] - 1900, dateFrom[i % 5][1], dateFrom[i % 5][2]));
                preparedStatement.setDate(2, new Date(dateTo[i % 5][0] - 1900, dateTo[i % 5][1], dateTo[i % 5][2]));
                preparedStatement.setString(3, icdeb[i % 5]);
                preparedStatement.executeQuery();

                long endMillis = System.currentTimeMillis();

                long diffMillis = endMillis - startMillies;

                maxMillis = Long.max(maxMillis, diffMillis);
                minMillies = Long.min(minMillies, diffMillis);
                allMillies += diffMillis;

                System.out.println("Dauer: " + diffMillis / 1000 + " Sekunden, " + diffMillis % 1000 + " Millis.");

            }

            System.out.println("Ergebnis Abfrage SQL - 1 Monat.");
            System.out.println("Max: " + maxMillis / 1000 + " Sekunden, " + maxMillis % 1000 + " Millis.");
            System.out.println("Min: " + minMillies / 1000 + " Sekunden, " + minMillies % 1000 + " Millis.");
            System.out.println("All: " + allMillies / 1000 + " Sekunden, " + allMillies % 1000 + " allMillies.");
            System.out.println("Avg: " + allMillies / 250 / 1000 + " Sekunden, " + allMillies / 250 % 1000 + " allMillies.");


        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    private int getRandom(int max) {
        return rand.nextInt(max);
    }

}
