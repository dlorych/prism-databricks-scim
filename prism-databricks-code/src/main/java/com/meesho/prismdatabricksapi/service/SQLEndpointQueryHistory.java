package com.meesho.prismdatabricksapi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;

import com.meesho.prismdatabricksapi.constants.Databricks;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;
import java.util.logging.Logger;
import lombok.extern.slf4j.Slf4j;

import static com.meesho.prismdatabricksapi.constants.Databricks.num_hours;

@Slf4j
public class SQLEndpointQueryHistory {
    private ApplicationProperties properties;

    public Boolean FindAllSPNQueryHistory() throws ParseException, JSONException {
        String json_payload = "{" +
                "\"filter_by\": {\n" +
                "\"query_start_time_range\": {\n" +
                "\"end_time_ms\": %3$s , \n" +
                "\"start_time_ms\": %2$s \n" +
                "},\n" +
                "\"statuses\": [\n" +
                "\"FINISHED\",\n" +
                "\"FAILED\", \n" +
                "\"QUEUED\",\n" +
                "\"CANCELED\",\n" +
                "\"RUNNING\"\n" +
                "],\n" +
                "\"warehouse_ids\": [\n" + "\"%1$s\" " +
                "]\n" +
                "},\n" +
                "\"include_metrics\": \"false\" \n" +
                "}";
        Boolean next_page_token = null;
        try {
            next_page_token = SPNQueryHistory(json_payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return next_page_token;
    }

    public Boolean FindSPNQueryHistoryByID(String service_principal_id) throws ParseException, JSONException {

        String json_payload = "{" +
                "\"filter_by\": {\n" +
                "\"query_start_time_range\": {\n" +
                "\"end_time_ms\": %3$s , \n" +
                "\"start_time_ms\": %2$s \n" +
                "},\n" +
                "\"statuses\": [\n" +
                "\"FINISHED\",\n" +
                "\"FAILED\", \n" +
                "\"QUEUED\",\n" +
                "\"CANCELED\",\n" +
                "\"RUNNING\"\n" +
                "],\n" +
                "\"user_ids\": [\n" +
                " \n" + service_principal_id + " \n" +
                "],\n" +
                "\"warehouse_ids\": [\n" + "\"%1$s\" " +
                "]\n" +
                "},\n" +
                "\"include_metrics\": \"false\" \n" +
                "}";
        Boolean next_page_token = null;
        try {
            next_page_token = SPNQueryHistory(json_payload);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return next_page_token;
}
    class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {
        public final static String METHOD_NAME = "GET";

        @Override
        public String getMethod() {
            return METHOD_NAME;
        }
    }

    public boolean SPNQueryHistory(String json_payload) throws IOException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("%1$s", properties.getValue("databricks_master_access_token"));
        String dbx_cluster_http_endpoint = String.format("%1$sapi/2.0/sql/history/queries", databricks_host);
        String token = String.format(" Basic %1$s ", databricks_master_access_token);
        String sql_endpoint_cluster_id = properties.getValue("sql_endpoint_cluster_id");

        HttpGetWithEntity httpEntity = new HttpGetWithEntity();
        URI slots = null;
        try {
            slots = new URI(dbx_cluster_http_endpoint);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss");
        String current_time = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss").format(new Date());
        String end_time_ms = String.valueOf(LocalDateTime.parse(current_time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss"))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli());
        LocalDateTime datetime = LocalDateTime.parse(current_time, formatter);
        datetime = datetime.minusHours(num_hours);
        String aftersubtraction = datetime.format(formatter);
        String start_time_ms = String.valueOf(LocalDateTime.parse(aftersubtraction, DateTimeFormatter.ofPattern("yyyy-MM-dd HH.mm.ss"))
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli());
        String data = String.format(json_payload, sql_endpoint_cluster_id, start_time_ms, end_time_ms);

        log.info("Calling Databricks SQLEndpoint Query History API /api/2.0/sql/history/queries ");
        HttpUriRequest request = null;
        try {
            request = RequestBuilder.get(slots)
                    .setEntity(new StringEntity(data))
                    .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .setHeader(HttpHeaders.AUTHORIZATION, token).build();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        HttpClient client = HttpClientBuilder.create().build();
        HttpResponse response = null;
        try {
            response = client.execute(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("HTTP Response for Databricks SQL Endpoint Query History API /api/2.0/sql/history/queries/ ");
        log.info("HTTP Response Status Code " + response.getStatusLine().getStatusCode());
        log.info("HTTP Response Status Message " + response.getStatusLine().getReasonPhrase());

        BufferedReader rd = new BufferedReader(new InputStreamReader(response
                .getEntity().getContent()));
        String response_output;
        boolean next_page_token = false;
        String response_output_json = null;
        while ((response_output = rd.readLine()) != null) {
            if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                Object json_obj = mapper.readValue(response_output, Object.class);
                response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                log.info("Query History for the Service Principal SPN ");
                log.info(response_output_json);
                JSONObject json_object = null;
                try {
                    json_object = new JSONObject(response_output);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
                if (json_object.has("next_page_token")) {
                    log.info("Queries are runnable on SQLEndpoint Cluster since last " + num_hours + " hour for user");
                    next_page_token = true;
                } else {
                    log.info("Queries are not runnable on SQLEndpoint Cluster since last " + num_hours + " hour for user");
                    next_page_token = false;
                }

            }
        }
        return next_page_token;
    }

    public static void main(String[] args) throws ParseException, JSONException {
        // testing the Query History API Call
        SQLEndpointQueryHistory cluster_obj= new SQLEndpointQueryHistory();
        Boolean next_page_token=cluster_obj.FindSPNQueryHistoryByID("6089093547479495");
        log.info(String.valueOf(next_page_token));
        Boolean next_page_tokens = cluster_obj.FindAllSPNQueryHistory();
        log.info(String.valueOf(next_page_tokens));


    }

}
