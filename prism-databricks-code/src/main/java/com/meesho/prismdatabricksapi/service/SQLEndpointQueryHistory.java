package com.meesho.prismdatabricksapi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;

import org.json.JSONException;
import org.json.JSONObject;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class SQLEndpointQueryHistory {
    private ApplicationProperties properties;

    public boolean FindSPNQueryHistory(String service_principal_id) throws ParseException, JSONException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("%1$s", properties.getValue("databricks_master_access_token"));
        String dbx_cluster_http_endpoint = String.format("%1$sapi/2.0/sql/history/queries", databricks_host);
        String sql_endpoint_cluster_id = properties.getValue("sql_endpoint_cluster_id");
        String token = String.format("Authorization: Basic %1$s ", databricks_master_access_token);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss");
        String current_time = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss").format(new Date());
        String end_time_ms = String.valueOf(LocalDateTime.parse(current_time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss"))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli());
        LocalDateTime datetime = LocalDateTime.parse(current_time, formatter);
        datetime = datetime.minusHours(5);
        String aftersubtraction = datetime.format(formatter);
        String start_time_ms = String.valueOf(LocalDateTime.parse(aftersubtraction, DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss"))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli());
        String data = String.format("{" +
                "\"filter_by\": {\n" +
                "\"query_start_time_range\": {\n" +
                "\"end_time_ms\": %4$s , \n" +
                "\"start_time_ms\": %3$s \n" +
                "},\n" +
                "\"statuses\": [\n" +
                "\"FINISHED\",\n" +
                "\"FAILED\", \n" +
                "\"QUEUED\",\n" +
                "\"CANCELED\",\n" +
                "\"RUNNING\"\n" +
                "],\n" +
                "\"user_ids\": [\n" +
                "%1$s" +
                "],\n" +
                "\"warehouse_ids\": [\n" + "\"%2$s\" " +
                "]\n" +
                "},\n" +
                "\"include_metrics\": \"false\" \n" +
                "}", service_principal_id, sql_endpoint_cluster_id, start_time_ms, end_time_ms);
        String[] command = {"curl", "--location", "--request", "GET", dbx_cluster_http_endpoint, "--header", "Content-type", ":", "raw", "/", "json", "--header", token, "--data-raw", data};
        ProcessBuilder process = new ProcessBuilder(command);
        Process p;
        boolean next_page_token = false;
        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));

            }
            String result = builder.toString();
            ObjectMapper mapper = new ObjectMapper();
            Object json_obj = mapper.readValue(result, Object.class);
            String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
            System.out.println("Query History for the Service Principal SPN ");
            System.out.print(response_output_json);
            JSONObject json_object = new JSONObject(response_output_json);
            if (json_object.has("next_page_token")) {
                System.out.println("Query are runnable since last 1 hour");
                next_page_token=true;
            } else {
                System.out.println("Query are not runnable since last 1 hour");
                next_page_token=false;
            }

        } catch (IOException e) {
            System.out.print("error");
            e.printStackTrace();
        }
        return next_page_token;
    }

    public static void main(String[] args) throws ParseException, JSONException {
        // testing the Query History API Call
        SQLEndpointQueryHistory cluster_obj= new SQLEndpointQueryHistory();
        Boolean next_page_token=cluster_obj.FindSPNQueryHistory("6089093547479495");
        System.out.println(next_page_token);


    }

}
