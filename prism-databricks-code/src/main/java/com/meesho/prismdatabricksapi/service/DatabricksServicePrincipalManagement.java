package com.meesho.prismdatabricksapi.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;
import org.json.*;

public class DatabricksServicePrincipalManagement  {
    private ApplicationProperties properties;
    public boolean GetListServicePrincipal(String service_principal_user) throws IOException,JSONException{
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
        System.out.println("HTTP Response Status Code "+  http.getResponseCode());
        System.out.println("HTTP Response Status Message "+ http.getResponseMessage());
        BufferedReader br = new BufferedReader(new InputStreamReader(
                (http.getInputStream())));
        String response_output;
        ArrayList<String> al = new ArrayList<String>();
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

                    String keyVal = jsn.getString("displayName");
                    al.add(keyVal);
                }

            }
            else{
                System.out.print("No output response is generated from calling SCIM Token API 2.0");
            }
        }
        Boolean bool = al.contains(service_principal_user);

        return bool;
    }
    public String ServicePrincipalBySCIM(String display_name) throws IOException, JSONException {
            this.properties = new ApplicationProperties();
            String databricks_host= properties.getValue("databricks_host");
            String databricks_master_access_token= String.format("Basic %1$s",properties.getValue("databricks_master_access_token"));
            String scim_endpoint= String.format("%1$sapi/2.0/preview/scim/v2/ServicePrincipals",databricks_host);
            String service_principal = null;
            String application_id=null;
            Boolean active=false;
            String service_principal_id=null;
            String group_name=null;
            String group_id=null;
            URL url = new URL(scim_endpoint);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestMethod("POST");
            http.setDoOutput(true);
            http.setRequestProperty("Content-type", "application/scim+json");
            http.setRequestProperty("Authorization",databricks_master_access_token);
            String data = String.format("{ \"displayName\":\"%1$s\", " +
                    "\"groups\": " + "[{\"value\": \"%2$s\"}]," +
                    "\"schemas\":" + "[\"urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal\" ]," +
                    "\"active\": true}", display_name,properties.getValue("databricks_group_id"));
            byte[] out = data.getBytes(StandardCharsets.UTF_8);
            OutputStream stream = http.getOutputStream();
            stream.write(out);
            int code = http.getResponseCode();
            if(code==200 || code==201) {
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
                        System.out.println("\n" + response_output_json);
                        JSONObject json_val = null;
                        JSONObject json_val_=null;
                        try {
                            json_val = new JSONObject(response_output);
                        } catch (JSONException e) {
                            throw new RuntimeException(e);
                        }
                        service_principal = json_val.getString("displayName");
                        application_id =json_val.getString("applicationId");
                        active= json_val.getBoolean("active");
                        service_principal_id=json_val.getString("service_principal_id");
                        try {
                            json_val_ = new JSONObject( json_val.getString("groups"));
                        } catch (JSONException e) {
                            throw new RuntimeException(e);
                        }
                        if (json_val_.has("display")) {
                            group_name = json_val.getString("display");
                        }
                        if(json_val_.has("value")) {
                            group_id = json_val.getString("value");
                        }

                    } else {
                        System.out.print("No output response is generated from calling SCIM Token API 2.0");
                    }
                }
            }
            else if(code==401 || code==403){
                System.out.println("Request is unauthorised because it lacks valid authentication credentials for the requested resource");
                System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
                System.exit(1);
            }
            else if (code==404){
                System.out.println("The requested resource does not exist");
                System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
                System.exit(1);
            }
            else if(code==400){
                System.out.println("The request is malformed requested by the client user");
                System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
                System.exit(1);
            }
            else if(code==500){
                System.out.println("The request is not handled correctly due to a server error.");
                System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
                System.exit(1);
            }
            else{
                System.out.println("Bad Request, Not able to call Databricks SCIM API ");
                System.out.println("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
                System.exit(1);
            }
            http.disconnect();
            return service_principal;
        }

    public static void main(String[] args) throws IOException, JSONException {
        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();

        //Callback function for getting the user mail id from the prism UI
        String prism_owner_mail = "ankit.kalra@meesho.com";
        String display_name = prism_owner_mail.replace("@meesho.com", "-serviceprincipal");
        Boolean b= scim.GetListServicePrincipal(display_name);
        if(b){
            System.out.println("Databricks Service Principal already exist corresponding to the user "+prism_owner_mail + " on the AWS Databricks");
        }
        else {
            String service_principal = scim.ServicePrincipalBySCIM(display_name);
            System.out.println("Your Databricks Service Principal Username is " + service_principal);
        }

    }
}
