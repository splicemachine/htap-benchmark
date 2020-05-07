
package com.oltpbenchmark;

import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DriverManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;


public class SpliceHtapSchema {

    private static final Logger LOG = Logger.getLogger(SpliceHtapSchema.class);
    String jdbcurl = null;
    String schema = "htap";
    String user = "htap";
    String password = "htapuser";
    String sourceSchema = "htap";
    String backupDirectory = null;
    boolean validate = false;
    long backupId = -1;    
    String dataDirectory = null;

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(
                "a",
                "action",
                true,
                "[required] Action to be performed.  Valid values are create, destroy or restore.");
        options.addOption(
                "j", 
                "jdbcurl", 
                true,
                "[required] Full JDBC Url including user and password");
        options.addOption(
                "s",
                "schema",
                true,
                "[required] The schema to use for the htap tables.  Defaults to htap.");
        options.addOption(
                "u",
                "user",
                true,
                "The user to use for the schema.  Defaults to htap.  Required when action is create or restore.");
        options.addOption(
                "p",
                "password",
                true,
                "If creating a user, need the password to use.  Defaults to htapuser.  Required when action is create or restore.");
        options.addOption(
                "b",
                "backupDirectory",
                true,
                "If restoring a schema the location of the backup - for example: s3a://splice-benchmark-data/database/HTAP/100.  Rrquired when action is restore.");
        options.addOption(
                "i",
                "backupId",
                true,
                "If restoring a schema the id of the backup");
        options.addOption(
                "m",
                "backupSourceSchema",
                true,
                "If restoring a schema the source schema.  Defaults to htap.");
        options.addOption(
                "d",
                "dataDirectory",
                true,
                "If loading data, the locationn of the data.  For example: s3a://splice-benchmark-data/flat/HTAP/htap-$SCALE");

        CommandLineParser parser = new PosixParser();
        CommandLine argsLine = parser.parse(options, args);
        if (argsLine.hasOption("j") == false) {
            LOG.fatal("Missing jdbc url");
            System.exit(1);
        } else if (argsLine.hasOption("a") == false) {
            LOG.fatal("Missing action");
            System.exit(1);
        }

        String action = argsLine.getOptionValue("a");
        if (!"restore".equals(action) && !"create".equals(action) && !"destroy".equals(action)) {
            LOG.error("Invalid action:" + action);
            System.exit(1);
        }
        SpliceHtapSchema spliceHtapDB = new SpliceHtapSchema(argsLine.getOptionValue("j"));
        spliceHtapDB.schema = argsLine.getOptionValue("s",spliceHtapDB.schema);
        spliceHtapDB.user = argsLine.getOptionValue("u",spliceHtapDB.user);
        spliceHtapDB.password = argsLine.getOptionValue("p",spliceHtapDB.password);
        if ("restore".equals(action)) {
            spliceHtapDB.sourceSchema = argsLine.getOptionValue("m",spliceHtapDB.sourceSchema);
            spliceHtapDB.backupDirectory = argsLine.getOptionValue("b",spliceHtapDB.backupDirectory);
            spliceHtapDB.backupId = Long.parseLong(argsLine.getOptionValue("i"));
        } else if ("create".equals(action)) {
            spliceHtapDB.dataDirectory = argsLine.getOptionValue("d",spliceHtapDB.dataDirectory);
        }

        spliceHtapDB.processRequest(action);
    }

    public SpliceHtapSchema(String jdbcUrl) {
        this.jdbcurl = jdbcUrl;
    }

    public void processRequest(String action) {
        boolean success = false;
        if ("restore".equals(action)) {
            success = restoreSchema(this.jdbcurl, this.user, this.password, this.schema, this.sourceSchema, this.backupDirectory, this.backupId, this.validate);
            if (!success) {
                System.out.println("Error restoring schema");
                System.exit(1);
            }
        } else if ("destroy".equals(action)) {
            success = cleanupDatabase(this.jdbcurl, this.schema, this.user);
            if (!success) {
                System.out.println("Error destroying database");
                System.exit(1);
            }
        } else if ("create".equals(action)) {
            if (createDDL(this.jdbcurl, this.user, this.password, this.schema)) {
                success = loadData(this.jdbcurl, this.schema, this.dataDirectory);
                if (!success) {
                    System.out.println("Error loading data");
                    System.exit(1);
                }
            } else {
                System.out.println("Error creating DDL");
                System.exit(1);
            }
        }
    }

    public boolean createDDL(String jdbcurl, String user, String password, String schema) {
        boolean success = false;

        try{
            //For the JDBC Driver 
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        }catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return success; //exit early if we can't find the driver
        }

        try(Connection conn = DriverManager.getConnection(jdbcurl)) {
            Statement stmt = null;
            try {

                if (!createUser(jdbcurl, user, password)) {
                    return false;
                }

                stmt = conn.createStatement();
                String tableSql = "CREATE TABLE " + schema + ".CUSTOMER ( "
                    + "C_W_ID INT NOT NULL, "
                    + "C_D_ID INT NOT NULL, "
                    + "C_ID INT NOT NULL, "
                    + "C_DISCOUNT DECIMAL(4,4) NOT NULL, "
                    + "C_CREDIT CHAR(2) NOT NULL, "
                    + "C_LAST VARCHAR(16) NOT NULL, "
                    + "C_FIRST VARCHAR(16) NOT NULL, "
                    + "C_CREDIT_LIM DECIMAL(12,2) NOT NULL, "
                    + "C_BALANCE DECIMAL(12,2) NOT NULL, "
                    + "C_YTD_PAYMENT FLOAT NOT NULL, "
                    + "C_PAYMENT_CNT INT NOT NULL, "
                    + "C_DELIVERY_CNT INT NOT NULL, "
                    + "C_STREET_1 VARCHAR(20) NOT NULL, "
                    + "C_STREET_2 VARCHAR(20) NOT NULL, "
                    + "C_CITY VARCHAR(20) NOT NULL, "
                    + "C_STATE CHAR(2) NOT NULL, "
                    + "C_ZIP CHAR(9) NOT NULL, "
                    + "C_NATIONKEY INT NOT NULL, "
                    + "C_PHONE CHAR(16) NOT NULL, "
                    + "C_SINCE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "C_MIDDLE CHAR(2) NOT NULL, "
                    + "C_DATA VARCHAR(500) NOT NULL, "
                    + "PRIMARY KEY (C_W_ID,C_D_ID,C_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".DISTRICT ( "
                    + "D_W_ID INT NOT NULL, "
                    + "D_ID INT NOT NULL, "
                    + "D_YTD DECIMAL(12,2) NOT NULL, "
                    + "D_TAX DECIMAL(4,4) NOT NULL, "
                    + "D_NEXT_O_ID INT NOT NULL, "
                    + "D_NAME VARCHAR(10) NOT NULL, "
                    + "D_STREET_1 VARCHAR(20) NOT NULL, "
                    + "D_STREET_2 VARCHAR(20) NOT NULL, "
                    + "D_CITY VARCHAR(20) NOT NULL, "
                    + "D_STATE CHAR(2) NOT NULL, "
                    + "D_ZIP CHAR(9) NOT NULL, "
                    + "D_NATIONKEY INT NOT NULL, "
                    + "PRIMARY KEY (D_W_ID,D_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);

                  tableSql = "CREATE TABLE " + schema + ".HISTORY ( "
                    + "H_C_ID INT NOT NULL, "
                    + "H_C_D_ID INT NOT NULL, "
                    + "H_C_W_ID INT NOT NULL, "
                    + "H_D_ID INT NOT NULL, "
                    + "H_W_ID INT NOT NULL, "
                    + "H_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "H_AMOUNT DECIMAL(6,2) NOT NULL, "
                    + "H_DATA VARCHAR(24) NOT NULL "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".ITEM ( "
                    + "I_ID INT NOT NULL, "
                    + "I_NAME VARCHAR(24) NOT NULL, "
                    + "I_PRICE DECIMAL(5,2) NOT NULL, "
                    + "I_DATA VARCHAR(50) NOT NULL, "
                    + "I_IM_ID INT NOT NULL, "
                    + "PRIMARY KEY (I_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".NEW_ORDER ( "
                    + "NO_W_ID INT NOT NULL, "
                    + "NO_D_ID INT NOT NULL, "
                    + "NO_O_ID INT NOT NULL, "
                    + "PRIMARY KEY (NO_W_ID,NO_D_ID,NO_O_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".OORDER ( "
                    + "O_W_ID INT NOT NULL, "
                    + "O_D_ID INT NOT NULL, "
                    + "O_ID INT NOT NULL, "
                    + "O_C_ID INT NOT NULL, "
                    + "O_CARRIER_ID INT DEFAULT NULL, "
                    + "O_OL_CNT DECIMAL(2,0) NOT NULL, "
                    + "O_ALL_LOCAL DECIMAL(1,0) NOT NULL, "
                    + "O_ENTRY_D TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "PRIMARY KEY (O_W_ID,O_D_ID,O_ID), "
                    + "UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".ORDER_LINE ( "
                    + "OL_W_ID INT NOT NULL, "
                    + "OL_D_ID INT NOT NULL, "
                    + "OL_O_ID INT NOT NULL, "
                    + "OL_NUMBER INT NOT NULL, "
                    + "OL_I_ID INT NOT NULL, "
                    + "OL_DELIVERY_D TIMESTAMP DEFAULT NULL, "
                    + "OL_AMOUNT DECIMAL(6,2) NOT NULL, "
                    + "OL_SUPPLY_W_ID INT NOT NULL, "
                    + "OL_QUANTITY DECIMAL(2,0) NOT NULL, "
                    + "OL_DIST_INFO CHAR(24) NOT NULL, "
                    + "PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".STOCK ( "
                    + "S_W_ID INT NOT NULL, "
                    + "S_I_ID INT NOT NULL, "
                    + "S_QUANTITY DECIMAL(4,0) NOT NULL, "
                    + "S_YTD DECIMAL(8,2) NOT NULL, "
                    + "S_ORDER_CNT INT NOT NULL, "
                    + "S_REMOTE_CNT INT NOT NULL, "
                    + "S_DATA VARCHAR(50) NOT NULL, "
                    + "S_DIST_01 CHAR(24) NOT NULL, "
                    + "S_DIST_02 CHAR(24) NOT NULL, "
                    + "S_DIST_03 CHAR(24) NOT NULL, "
                    + "S_DIST_04 CHAR(24) NOT NULL, "
                    + "S_DIST_05 CHAR(24) NOT NULL, "
                    + "S_DIST_06 CHAR(24) NOT NULL, "
                    + "S_DIST_07 CHAR(24) NOT NULL, "
                    + "S_DIST_08 CHAR(24) NOT NULL, "
                    + "S_DIST_09 CHAR(24) NOT NULL, "
                    + "S_DIST_10 CHAR(24) NOT NULL, "
                    + "S_SUPPKEY INT NOT NULL,  "
                    + "PRIMARY KEY (S_W_ID,S_I_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "CREATE TABLE " + schema + ".WAREHOUSE ( "
                    + "W_ID INT NOT NULL, "
                    + "W_YTD DECIMAL(12,2) NOT NULL, "
                    + "W_TAX DECIMAL(4,4) NOT NULL, "
                    + "W_NAME VARCHAR(10) NOT NULL, "
                    + "W_STREET_1 VARCHAR(20) NOT NULL, "
                    + "W_STREET_2 VARCHAR(20) NOT NULL, "
                    + "W_CITY VARCHAR(20) NOT NULL, "
                    + "W_STATE CHAR(2) NOT NULL, "
                    + "W_ZIP CHAR(9) NOT NULL, "
                    + "W_NATIONKEY INT NOT NULL, "
                    + "PRIMARY KEY (W_ID) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "create table " + schema + ".region ( "
                    + "r_regionkey int not null, "
                    + "r_name char(55) not null, "
                    + "r_comment char(152) not null, "
                    + "PRIMARY KEY ( r_regionkey ) "
                    + ") ";
                  
                  tableSql = "create table " + schema + ".nation ( "
                    + "n_nationkey int not null, "
                    + "n_name char(25) not null, "
                    + "n_regionkey int not null, "
                    + "n_comment char(152) not null, "
                    + "PRIMARY KEY ( n_nationkey ) "
                    + ") ";
                stmt.executeUpdate(tableSql);
                  
                  tableSql = "create table " + schema + ".supplier ( "
                    + "su_suppkey int not null, "
                    + "su_name char(25) not null, "
                    + "su_address varchar(40) not null, "
                    + "su_nationkey int not null, "
                    + "su_phone char(15) not null, "
                    + "su_acctbal numeric(12,2) not null, "
                    + "su_comment char(101) not null, "
                    + "PRIMARY KEY ( su_suppkey ) "
                    + ") ";
                stmt.executeUpdate(tableSql);

                String createView = "CREATE view " + schema + ".revenue0 (supplier_no, total_revenue) AS "
                    + "SELECT supplier_no, sum(cast(ol_amount as decimal(12,2))) as total_revenue "
                    + "FROM order_line, "
                    + "(SELECT s_suppkey AS supplier_no, s_i_id, s_w_id FROM stock) stocksupp "
                    + "WHERE ol_i_id = s_i_id "
                    + "AND ol_supply_w_id = s_w_id "
                    + "AND ol_delivery_d >= '2007-01-02 00:00:00.000000' "
                    + "GROUP BY supplier_no";
                stmt.executeUpdate(createView);

                String createIndex = "CREATE INDEX CUSTOMER_IX_CUSTOMER_NAME ON CUSTOMER(C_W_ID, C_D_ID, C_LAST, C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, "
                + "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE)";
                stmt.executeUpdate(createIndex);

                createIndex = "CREATE INDEX IDX_OORDER ON OORDER (O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID, O_ENTRY_D)";
                stmt.executeUpdate(createIndex);

                createIndex = "CREATE INDEX OL_I_ID ON ORDER_LINE (OL_W_ID, OL_D_ID, OL_I_ID, OL_O_ID)";
                stmt.executeUpdate(createIndex);

                stmt.close();
                success = true;

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } 

        return success;
    }

    public boolean cleanupDatabase(String jdbcurl, String schema, String user) {
        boolean success = false;

        try{
            //For the JDBC Driver 
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        }catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return success; //exit early if we can't find the driver
        }

        try(Connection conn = DriverManager.getConnection(jdbcurl)) {
            Statement stmt = null;
            CallableStatement cs = null;
            try {

                stmt = conn.createStatement();
                stmt.executeUpdate("DROP VIEW " + schema + ".revenue0");
                stmt.executeUpdate("DROP TABLE " + schema + ".region");
                stmt.executeUpdate("DROP TABLE " + schema + ".nation");
                stmt.executeUpdate("DROP TABLE " + schema + ".supplier");
                stmt.executeUpdate("DROP TABLE " + schema + ".CUSTOMER");
                stmt.executeUpdate("DROP TABLE " + schema + ".DISTRICT");
                stmt.executeUpdate("DROP TABLE " + schema + ".HISTORY");
                stmt.executeUpdate("DROP TABLE " + schema + ".ITEM");
                stmt.executeUpdate("DROP TABLE " + schema + ".NEW_ORDER");
                stmt.executeUpdate("DROP TABLE " + schema + ".OORDER");
                stmt.executeUpdate("DROP TABLE " + schema + ".ORDER_LINE");
                stmt.executeUpdate("DROP TABLE " + schema + ". STOCK");
                stmt.executeUpdate("DROP TABLE " + schema + ". WAREHOUSE");
                stmt.executeUpdate("DROP SCHEMA" + schema + " RESTRICT");
                stmt.close();

                cs = conn.prepareCall("{call SYSCS_UTIL.VACUUM()}");
                cs.executeQuery();

                cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_DROP_USER('" + user + "')}");
                cs.executeQuery();

                cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES()}");
                cs.executeQuery();

                cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()}");
                cs.executeQuery();
                cs.close();

                success = true;
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (cs != null) {
                    cs.close();
                }
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } 

        return success;
    }

    public boolean loadData(String jdbcurl, String schema, String directory) {
        boolean success = false;

        try{
            //For the JDBC Driver 
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        }catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return success; //exit early if we can't find the driver
        }

        try(Connection conn = DriverManager.getConnection(jdbcurl)) {
            Statement stmt = null;
            CallableStatement cs = null;
            try {
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'WAREHOUSE',  null, '" + directory + "/warehouse.csv',  null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'STOCK',      null, '" + directory + "/stock.csv',      null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'CUSTOMER',   null, '" + directory + "/customer.csv',   null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'DISTRICT',   null, '" + directory + "/district.csv',   null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'HISTORY',    null, '" + directory + "/history.csv',    null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'ITEM',       null, '" + directory + "/item.csv',       null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'NEW_ORDER',  null, '" + directory + "/new_order.csv',  null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'OORDER',     null, '" + directory + "/oorder.csv',     null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'ORDER_LINE', null, '" + directory + "/order_line.csv', null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'REGION',     null, '" + directory + "/region.csv',     null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'NATION',     null, '" + directory + "/nation.csv',     null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs = conn.prepareCall("{call SYSCS_UTIL.BULK_IMPORT_HFILE (?, 'SUPPLIER',   null, '" + directory + "/supplier.csv',   null, '\"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false)");
                cs.setString(1, schema);
                cs.executeQuery();
                cs.close();
                success = true;

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (cs != null) {
                    cs.close();
                }
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } 

        return success;
    }

    public boolean createUser(String jdbcurl, String user, String password) {
        boolean success = false;

        try{
            //For the JDBC Driver 
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        }catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return success; //exit early if we can't find the driver
        }

        try(Connection conn = DriverManager.getConnection(jdbcurl)) {
            PreparedStatement ps = null;
            Statement stmt = null;
            CallableStatement cs = null;
            try {

                boolean createUser = true;
                ps = conn.prepareStatement("select USERNAME from SYS.SYSUSERS where USERNAME = ?");
                ps.setString(1, user);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    createUser = false;
                }
                rs.close();
                ps.close();

                if (createUser) {
                    cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_CREATE_USER(?,?);}");
                    cs.setString(1, user);
                    cs.setString(2, password);
                    cs.executeQuery();
                }
                cs.close();
                success = true;

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (cs != null) {
                    cs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } 

        return success;
    }


    public boolean restoreSchema(String jdbcurl, String user, String password, String schema, String sourceSchema, String directory, long backupId, boolean validate) {
        boolean success = false;

        try{
            //For the JDBC Driver 
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        }catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return success; //exit early if we can't find the driver
        }

        try(Connection conn = DriverManager.getConnection(jdbcurl)) {
            PreparedStatement ps = null;
            Statement stmt = null;
            CallableStatement cs = null;
            try {

                if (!createUser(jdbcurl, user, password)) {
                    return false;
                }

                cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER(?,?);}");
                cs.setString(1, schema);
                cs.setString(2, user);
                cs.executeQuery();

                cs = conn.prepareCall("{call SYSCS_UTIL.SYSCS_RESTORE_SCHEMA(?,?,?,?,?);}");
                cs.setString(1, schema);
                cs.setString(2, sourceSchema);
                cs.setString(3, directory);
                cs.setLong(4, backupId);
                cs.setBoolean(5, validate);
                cs.executeQuery();
                cs.close();

                String createView = "CREATE view " + schema + ".revenue0 (supplier_no, total_revenue) AS "
                    + "SELECT supplier_no, sum(cast(ol_amount as decimal(12,2))) as total_revenue "
                    + "FROM order_line, "
                    + "(SELECT s_suppkey AS supplier_no, s_i_id, s_w_id FROM stock) stocksupp "
                    + "WHERE ol_i_id = s_i_id "
                    + "AND ol_supply_w_id = s_w_id "
                    + "AND ol_delivery_d >= '2007-01-02 00:00:00.000000' "
                    + "GROUP BY supplier_no";

                stmt = conn.createStatement();
                stmt.executeUpdate(createView);

                stmt.close();
                success = true;

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (cs != null) {
                    cs.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } 

        return success;
    }
}
