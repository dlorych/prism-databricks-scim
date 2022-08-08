package com.meesho.prismdatabricksapi.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;
import org.json.*;

public class DatabricksServicePrincipalManagement {
    private ApplicationProperties properties;

    public void GetServicePrincipalByID(Object service_principal_id) throws IOException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("Basic %1$s", properties.getValue("databricks_master_access_token"));
        String scim_endpoint = String.format("%1$sapi/2.0/preview/scim/v2/ServicePrincipals/%2$s", databricks_host, service_principal_id);
        URL url = new URL(scim_endpoint);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("GET");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        int code = http.getResponseCode();
        if (code == 200) {
            System.out.println("HTTP Response for Databricks GetServicePrincipalByID API Endpoint /api/2.0/preview/scim/v2/ServicePrincipals/{id}");
            System.out.println("HTTP Response Status Code " + http.getResponseCode());
            System.out.println("HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Service Principal detail fetched successfully");
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    System.out.println("Service Principal Detail for Service Principal ID "+service_principal_id);
                    System.out.println(response_output_json);
                } else {
                    System.out.print("No output response is generated from calling SCIM GetServicePrincipalByID API 2.0");
                }
            }
        } else if (code == 401 || code == 403) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Request is unauthorised because it lacks valid authentication credentials for the requested resource. Hence, not able to get SPN detail for Service Principal ID "+service_principal_id);
            System.exit(1);
        } else if (code == 404) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The requested resource does not exist. Hence, not able to get SPN detail for Service Principal ID "+service_principal_id);
            System.exit(1);
        } else if (code == 400) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The request is malformed requested by the client user. Hence, not able to get SPN detail for Service Principal ID "+service_principal_id);
            System.exit(1);
        } else if (code == 500) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The request is not handled correctly due to a server error. Hence, not able to get SPN detail for Service Principal ID "+service_principal_id);
            System.exit(1);
        } else {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Bad Request, Not able to call Databricks SCIM API. Hence, not able to get SPN detail for Service Principal ID "+service_principal_id);
            System.exit(1);
        }

        http.disconnect();

    }
    public void DeleteServicePrincipalByID(Collection list_spn) throws IOException {
        this.properties = new ApplicationProperties();
        String databricks_host= properties.getValue("databricks_host");
        String databricks_master_access_token= String.format("Basic %1$s",properties.getValue("databricks_master_access_token"));
        for(Object service_principal_id:list_spn) {
            String scim_endpoint = String.format("%1$sapi/2.0/preview/scim/v2/ServicePrincipals/%2$s", databricks_host, service_principal_id);
            System.out.println(scim_endpoint);
            URL url = new URL(scim_endpoint);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestMethod("DELETE");
            http.setDoOutput(true);
            http.setRequestProperty("Content-type", "text/plain");
            http.setRequestProperty("Authorization", databricks_master_access_token);
            System.out.println("HTTP Response for Databricks GetServicePrincipals API Endpoint /api/2.0/preview/scim/v2/ServicePrincipals ");
            int http_code= http.getResponseCode();
            System.out.println("HTTP Response Status Code "+  http.getResponseCode());
            System.out.println("HTTP Response Status Message "+ http.getResponseMessage());
            if(http_code==200 || http_code==204){
                System.out.print("Service Principal ID "+service_principal_id+ " is deleted from databriciks");
            }
        }

    }
    public DatabricksSCIM GetListServicePrincipal() throws IOException,JSONException{
        this.properties = new ApplicationProperties();
        String databricks_host= properties.getValue("databricks_host");
        String databricks_master_access_token= String.format("Basic %1$s",properties.getValue("databricks_master_access_token"));
        String scim_endpoint= String.format("%1$sapi/2.0/preview/scim/v2/ServicePrincipals?excludedAttributes=applicationId,entitlements,groups,id,roles,active",databricks_host);
        URL url = new URL(scim_endpoint);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("GET");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/scim+json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        System.out.println("HTTP Response for Databricks GetServicePrincipals API Endpoint /api/2.0/preview/scim/v2/ServicePrincipals ");
        System.out.println("HTTP Response Status Code "+  http.getResponseCode());
        System.out.println("HTTP Response Status Message "+ http.getResponseMessage());
        BufferedReader br = new BufferedReader(new InputStreamReader(
                (http.getInputStream())));
        String response_output;
        ArrayList<String> display_list = new ArrayList<String>();
        ArrayList<String> spn_id_list = new ArrayList<String>();
        while ((response_output = br.readLine()) != null) {
            if (!Objects.isNull(response_output) ||response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
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

                    String spn_display = jsn.getString("displayName");
                    String spn_id = jsn.getString("id");
                    display_list.add(spn_display);
                    spn_id_list.add(spn_id);
                }

            }
            else{
                System.out.print("No output response is generated from calling SCIM ListServicePrincipal API 2.0");
            }
        }

        return new DatabricksSCIM(display_list,spn_id_list);
    }

    public DatabricksSCIM ServicePrincipalBySCIM(String display_name,String owner_email,String databricks_group_id) throws IOException, JSONException {
        this.properties = new ApplicationProperties();
        String databricks_host = properties.getValue("databricks_host");
        String databricks_master_access_token = String.format("Basic %1$s", properties.getValue("databricks_master_access_token"));
        String scim_endpoint = String.format("%1$sapi/2.0/preview/scim/v2/ServicePrincipals", databricks_host);
        String service_principal = null;
        String application_id = null;
        Boolean active = false;
        String service_principal_id = null;
        String group_name = null;
        String group_id = null;
        HashMap<Object, String> map = null;
        URL url = new URL(scim_endpoint);
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("POST");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/scim+json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        String data = String.format("{ \"displayName\":\"%1$s\", " +
                "\"groups\": " + "[{\"value\": \"%2$s\"}]," +
                "\"schemas\":" + "[\"urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal\" ]," +
                "\"active\": true}", display_name, databricks_group_id);
        byte[] out = data.getBytes(StandardCharsets.UTF_8);
        OutputStream stream = http.getOutputStream();
        stream.write(out);
        int code = http.getResponseCode();
        if (code == 200 || code == 201) {
            System.out.println("HTTP Response for Databricks SCIM API Endpoint /api/2.0/preview/scim/v2/ServicePrincipals");
            System.out.println("HTTP Response Status Code " + http.getResponseCode());
            System.out.println("HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Service Principal is created successfully");
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    System.out.println("Service Principal Details for display name "+display_name);
                    System.out.println(response_output_json);
                    JSONObject json_val = null;
                    JSONObject json_val_ = null;
                    JSONArray json_array_ = null;
                    try {
                        json_val = new JSONObject(response_output);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    service_principal = json_val.getString("displayName");
                    application_id = json_val.getString("applicationId");
                    active = json_val.getBoolean("active");
                    service_principal_id = json_val.getString("id");
                    try {
                        json_array_ = json_val.getJSONArray("groups");
                        json_val_ = json_array_.getJSONObject(0);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    if (json_val_.has("display")) {
                        group_name = json_val_.getString("display");
                    }
                    if (json_val_.has("value")) {
                        group_id = json_val_.getString("value");
                    }
                    map = new HashMap<Object, String>();
                    map.put("service_principal", service_principal);
                    map.put("application_id", application_id);
                    map.put("active", String.valueOf((active)));
                    map.put("service_principal_id", service_principal_id);
                    map.put("group_name", group_name);
                    map.put("group_id", group_id);
                    map.put("owner_email",owner_email);

                } else {
                    System.out.print("No output response is generated from calling SCIM CreateServicePrincipal API 2.0");
                }
            }
        } else if (code == 401 || code == 403) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Request is unauthorised because it lacks valid authentication credentials for the requested resource. Hence, not able to create SPN for display_name "+display_name);
            System.exit(1);
        } else if (code == 404) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The requested resource does not exist. Hence, not able to create SPN for display_name "+display_name);
            System.exit(1);
        } else if (code == 400) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The request is malformed requested by the client user. Hence, not able to create SPN for display_name "+display_name);
            System.exit(1);
        } else if (code == 500) {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("The request is not handled correctly due to a server error. Hence, not able to create SPN for display_name "+display_name);
            System.exit(1);
        } else {
            System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Bad Request, Not able to call Databricks SCIM API. Hence, not able to create SPN for display_name "+display_name);
            System.exit(1);
        }
        http.disconnect();
       return new DatabricksSCIM(service_principal, application_id, map);

    }

    public static void main(String[] args) throws IOException, JSONException {
        //Callback function for getting the user mail id from the prism UI
        String prism_owner_mail = "spn.token@meesho.com";
        String display_name = prism_owner_mail.replace("@meesho.com", "-serviceprincipal");
        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();
        DatabricksSCIMGroups dbx_group= new DatabricksSCIMGroups();
        DatabricksSCIM list_obj= scim.GetListServicePrincipal();
        Boolean b= list_obj.spn_display_list.contains(display_name);
        if(b){
            System.out.println("Databricks Service Principal already exist corresponding to the user "+prism_owner_mail + " on the AWS Databricks");
        }
        else {
            String databricks_group_id=dbx_group.GetDatabricksGroupID();
            dbx_group.GetGroupDetailByID(databricks_group_id);
           DatabricksSCIM obj =scim.ServicePrincipalBySCIM(display_name,prism_owner_mail,databricks_group_id);
           System.out.println("Your Databricks Service Principal Username is " + obj.service_principal);
            System.out.println("Your Databricks Service Principal Application ID is " + obj.application_id );
        }

        /* ======== Test ServicePrincipal DeleteServicePrincipalByID ===========
        If you want to prevent certain SPN ID to delete from workspace, pass the spn_id list

        boolean bool_test= list_obj.spn_id_list.removeIf(value -> value.contains("6457562551823152"));
        if(bool_test) {
            System.out.println("List of Service Principal ID to be deleted from workspace");
            System.out.println(list_obj.spn_id_list);
            scim.DeleteServicePrincipalByID(list_obj.spn_id_list);
        }
        */


    }
}
