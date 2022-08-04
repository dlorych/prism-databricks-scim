package com.meesho.prismdatabricksapi.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.*;

public class DatabricksServicePrincipalManagement  {
    public boolean GetListServicePrincipal(String service_principal_user) throws IOException,JSONException{
        URL url = new URL("https://meesho-data-intelligence-prod.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals?excludedAttributes=applicationId,entitlements,groups,id,roles,active");
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        http.setRequestMethod("GET");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/scim+json");
        http.setRequestProperty("Authorization", "Basic bmlzaGNoYXkuYWdhcndhbEBtZWVzaG8uY29tOmtsbW4yODE4MzJRIw==");
        System.out.println("HTTP Response Status Code "+  http.getResponseCode());
        System.out.println("HTTP Response Status Message "+ http.getResponseMessage());
        BufferedReader br = new BufferedReader(new InputStreamReader(
                (http.getInputStream())));
        String response_output;
        ArrayList<String> al = new ArrayList<String>();
        while ((response_output = br.readLine()) != null) {
            if (response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
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

            }}
        Boolean bool = al.contains(service_principal_user);

        return bool;
    }
    public String ServicePrincipalBySCIM(String display_name) throws IOException, JSONException {
            String service_principal = null;
            URL url = new URL("https://meesho-data-intelligence-prod.cloud.databricks.com/api/2.0/preview/scim/v2/ServicePrincipals");
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestMethod("POST");
            http.setDoOutput(true);
            http.setRequestProperty("Content-type", "application/scim+json");
            http.setRequestProperty("Authorization", "Basic bmlzaGNoYXkuYWdhcndhbEBtZWVzaG8uY29tOmtsbW4yODE4MzJRIw==");
            String data = String.format("{ \"displayName\":\"%1$s\", " +
                    "\"groups\": " + "[{\"value\": \"577690862496256\"}]," +
                    "\"schemas\":" + "[\"urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal\" ]," +
                    "\"active\": true}", display_name);

            byte[] out = data.getBytes(StandardCharsets.UTF_8);

            OutputStream stream = http.getOutputStream();
            stream.write(out);
            System.out.println("HTTP Response Status Code " + http.getResponseCode());
            System.out.println("HTTP Response Status Message " + http.getResponseMessage());
            System.out.println("Service Principal is created successfully");
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    System.out.println("\n" + response_output_json);
                    JSONObject json_val = null;
                    try {
                        json_val = new JSONObject(response_output);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    service_principal = json_val.getString("displayName");
                }

            }
            http.disconnect();

            return service_principal;
        }

    public static void main(String[] args) throws IOException, JSONException {
        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();
        //Callback function for getting the user mail id from the prism UI
        String prism_owner_mail = "nikhil.srivastava@meesho.com";
        String display_name = prism_owner_mail.replace("@meesho.com", "_serviceprincipal");
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
