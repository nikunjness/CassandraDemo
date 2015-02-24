package com.practice.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

public class CassandraWrapper {

    private Cluster cluster;
    private Session session;

    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void createSchema() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':3};");

        session.execute("USE demo;");

        session.execute(
                "CREATE TABLE IF NOT EXISTS demo.employees (" +
                        "id int PRIMARY KEY," +
                        "first_name text," +
                        "last_name text," +
                        "company_name text," +
                        "address text," +
                        "city text," +
                        "country text," +
                        "state text," +
                        "zip text," +
                        "phone1 text," +
                        "phone2 text," +
                        "email text," +
                        "web text," +
                        ");");

        //session.execute("COPY employees ( first_name, last_name, company_name, address, city, country, state, zip, phone1, phone2, email, web) FROM('us-500.csv');");
    }

    public void insertData() {
        String csvFile = "/home/nikunj/projects/CassandraDemo/src/main/resources/us-500.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        int rowid = 1;

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] employee = line.split(cvsSplitBy);
                Statement statement = QueryBuilder.insertInto("demo", "employees")
                        .value("id", rowid)
                        .value("first_name", employee[0])
                        .value("last_name", employee[1])
                        .value("company_name", employee[2])
                        .value("address", employee[3])
                        .value("city", employee[4])
                        .value("country", employee[5])
                        .value("state", employee[6])
                        .value("zip", employee[7])
                        .value("phone1", employee[8])
                        .value("phone2", employee[9])
                        .value("email", employee[10])
                        .value("web", employee[11]);
                session.execute(statement);
                rowid++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public List<Row> getEmployees(String keyspace, String table) {
        Statement statement = QueryBuilder
                .select()
                .all()
                .from(keyspace, table);
        return session.execute(statement).all();
    }

    public void getEmployee(int id) {
        Statement statement = QueryBuilder.select()
                .all()
                .from("demo", "employees")
                .where(eq("id", id));
        ResultSet results = session.execute(statement);
        for (Row row : results) {
            System.out.println("First Name: " + row.getString("first_name") + " Last Name: " + row.getString("last_name"));
        }
    }

    public void updateEmployee(int id) {
        Statement statement = QueryBuilder.update("demo", "employees")
                .with(set("last_name", "Dost"))
                .where(eq("id", id));
        session.execute(statement);
    }

    public void deleteEmployee(int id) {
        Statement statement = QueryBuilder.delete()
                .from("demo", "employees")
                .where(eq("id", 500));
        session.execute(statement);
    }

    public void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        CassandraWrapper client = new CassandraWrapper();
        client.connect("127.0.0.1");
        client.createSchema();
        client.insertData();
        //client.updateEmployee(500);
        //client.getEmployee(500);
        //client.deleteEmployee(500);
        client.getEmployee(500);
        client.close();
    }

}
