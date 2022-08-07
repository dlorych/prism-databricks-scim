package com.meesho.prismdatabricksapi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;


import io.trino.jdbc.$internal.okhttp3.*;

import kong.unirest.Unirest;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.data.relational.core.sql.In;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.*;

import java.net.http.HttpResponse;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;

public class SQLEndpointQueryHistory {
    private ApplicationProperties properties;

    public void SPNQueryHistory(String service_principal_id) {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("%1$s", properties.getValue("databricks_master_access_token"));
        String dbx_cluster_http_endpoint = String.format("%1$sapi/2.0/sql/history/queries", databricks_host);
        String sql_endpoint_cluster_id= properties.getValue("dbx_cluster_http_endpoint");
        String token= String.format("Authorization: Basic %1$s ",databricks_master_access_token);
        String data= String.format("{\n" +
                "    \"filter_by\": {\n" +
                "        \"statuses\": [\n" +
                "            \"FINISHED\",\n" +
                "            \"FAILED\"\n" +
                "        ],\n" +
                "        \"user_ids\": [\n" +
                "            %1$s \n" +
                "\n" +
                "        ],\n" +
                "\n" +
                "        \"warehouse_ids\": [\n" +
                "            %2$s \n" +
                "        ]\n" +
                "    },\n" +
                "    \"include_metrics\": \"false\",\n" +
                "    \"max_results\": 1\n" +
                "}",service_principal_id,sql_endpoint_cluster_id);
       String[] command={"curl","--location","--request","GET",dbx_cluster_http_endpoint,"--header","Content-type",":","raw","/","json","--header",token,"--data-raw", data};
         ProcessBuilder process = new ProcessBuilder(command);
        Process p;
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
            System.out.print(response_output_json);

        } catch (IOException e) {
            System.out.print("error");
            e.printStackTrace();
        }

    }
    public static void main(String[] args) {
        // testing the Query History API Call
        SQLEndpointQueryHistory cluster_obj= new SQLEndpointQueryHistory();
        cluster_obj.SPNQueryHistory("6457562551823152");


    }

}
