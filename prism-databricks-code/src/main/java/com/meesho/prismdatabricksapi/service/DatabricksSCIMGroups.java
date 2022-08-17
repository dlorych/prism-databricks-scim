package com.meesho.prismdatabricksapi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.logging.Logger;

public class DatabricksSCIMGroups {
    static Logger log = Logger.getLogger(DatabricksSCIMGroups.class.getName());
    private ApplicationProperties properties;

    public String GetDatabricksGroupID() throws IOException, JSONException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("Basic %1$s", properties.getValue("databricks_master_access_token"));
        String datbricks_group_http_endpoint = String.format("%1$sapi/2.0/preview/scim/v2/Groups", databricks_host);
        String databricks_group_name = properties.getValue("databricks_group_name");
        String group_display_name = null;
        String group_id = null;
        String databricks_group_id = null;
        URL url = new URL(datbricks_group_http_endpoint);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("GET");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/scim+json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        int http_code = http.getResponseCode();
        if (http_code == 200) {
            log.info("HTTP Response for Databricks Group API Endpoint /api/2.0/preview/scim/v2/Groups");
            log.info("HTTP Response Status Code " + http.getResponseCode());
            log.info("HTTP Response Status Message " + http.getResponseMessage());
            log.info("Databricks Group detail fetched successfully");
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            HashMap<String, String> key_value = new HashMap<String, String>();
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    JSONObject json_val = null;
                    try {
                        json_val = new JSONObject(response_output);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    String resources = json_val.getString("Resources");
                    JSONArray jsonArray = new JSONArray(resources);
                    for (int i = 0; i < jsonArray.length(); ++i) {

                        JSONObject jsn = jsonArray.getJSONObject(i);

                        group_display_name = jsn.getString("displayName");
                        group_id = jsn.getString("id");
                        key_value.put(group_display_name, group_id);
                        boolean isKeyPresent = key_value.containsKey(databricks_group_name);
                        if (isKeyPresent) {
                            databricks_group_id = key_value.get(databricks_group_name);
                        } else {
                            log.info("Databricks Group " + databricks_group_name + " does not exist");
                        }
                    }

                } else {
                    log.info("No output response is generated from calling SCIM GetDatabricksGroups API 2.0");
                }
            }
        } else if (http_code == 401 || http_code == 403) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Request is unauthorised because it lacks valid authentication credentials for the requested resource Hence, not able to get group_id of databricks group " + databricks_group_name);
            System.exit(1);
        } else if (http_code == 404) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The requested resource does not exist. Hence, not able to get group_id of databricks group " + databricks_group_name);
            System.exit(1);
        } else if (http_code == 400) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The request is malformed requested by the client user. Hence, not able to get group_id of databricks group " + databricks_group_name);
            System.exit(1);
        } else if (http_code == 500) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The request is not handled correctly due to a server error. Hence, not able to get group_id of databricks group " + databricks_group_name);
            System.exit(1);
        } else {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Bad Request, Not able to call Databricks SCIM API. Hence, not able to get group_id of databricks group " + databricks_group_name);
            System.exit(1);
        }
        http.disconnect();
        return databricks_group_id;
    }

    public void GetGroupDetailByID(String group_id) throws IOException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("Basic %1$s", properties.getValue("databricks_master_access_token"));
        String databricks_group_endpoint = String.format("%1$sapi/2.0/preview/scim/v2/Groups/%2$s", databricks_host, group_id);
        URL url = new URL(databricks_group_endpoint);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("GET");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/scim+json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        int http_code = http.getResponseCode();
        if (http_code == 200) {
            log.info("HTTP Response for Databricks Group API Endpoint /api/2.0/preview/scim/v2/Groups/{id}");
            log.info("HTTP Response Status Code " + http.getResponseCode());
            log.info("HTTP Response Status Message " + http.getResponseMessage());
            log.info("Databricks Group detail fetched successfully for group id " + group_id);
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    log.info("Databricks Group Details for group id " + group_id);
                    log.info(response_output_json);
                } else {
                    log.info("No output response is generated from calling SCIM GetDatabricksGroupDetailByID API 2.0");
                }
            }
        } else if (http_code == 401 || http_code == 403) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Request is unauthorised because it lacks valid authentication credentials for the requested resource. Hence, not able to get details of databricks group for group id " + group_id);
            System.exit(1);
        } else if (http_code == 404) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The requested resource does not exist. Hence, not able to get details of databricks group for group id " + group_id);
            System.exit(1);
        } else if (http_code == 400) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The request is malformed requested by the client user. Hence, not able to get details of databricks group for group id " + group_id);
            System.exit(1);
        } else if (http_code == 500) {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The request is not handled correctly due to a server error. Hence, not able to get details of databricks group for group id " + group_id);
            System.exit(1);
        } else {
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Bad Request, Not able to call Databricks SCIM API. Hence, not able to get details of databricks group for group id " + group_id);
            System.exit(1);
        }
        http.disconnect();

    }
    public static void main(String[] args) throws IOException, JSONException {
        DatabricksSCIMGroups dbx_group= new DatabricksSCIMGroups();
        String databricks_group_id=dbx_group.GetDatabricksGroupID();
        dbx_group.GetGroupDetailByID(databricks_group_id);

    }

}
