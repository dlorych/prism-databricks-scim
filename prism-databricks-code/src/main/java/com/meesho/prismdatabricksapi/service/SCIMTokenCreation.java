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

public class SCIMTokenCreation {
    private ApplicationProperties properties;
    public void ServicePrincipalToken(String scim_application_id, String service_principal_display_name) throws IOException,JSONException{
        this.properties = new ApplicationProperties();
        String databricks_host= properties.getValue("databricks_host");
        String databricks_master_access_token= String.format("Basic %1$s",properties.getValue("databricks_master_access_token"));
        String spn_token_endpoint= String.format("%1$sapi/2.0/token-management/on-behalf-of/tokens",databricks_host);
        URL url = new URL(spn_token_endpoint);
        HttpURLConnection http = (HttpURLConnection)url.openConnection();
        http.setRequestMethod("POST");
        http.setDoOutput(true);

        http.setRequestProperty("Content-type", "application/json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        String data = String.format("{\n  \"application_id\": \"%1$s\",\n  \"comment\":" +
                " \"This the token for the Service Principal user %2$s\",\n  " +
                "\"lifetime_seconds\":  69120000 \n}",scim_application_id, service_principal_display_name);
        byte[] out = data.getBytes(StandardCharsets.UTF_8);
        OutputStream stream = http.getOutputStream();
        stream.write(out);
        int code = http.getResponseCode();
        if(code==200 || code==201){
            System.out.println("HTTP Response Status Code " + http.getResponseCode());
            System.out.println("HTTP Response Status Message " + http.getResponseMessage());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    System.out.println("\n" + response_output_json);
                }
                else{
                    System.out.print("No output response is generated from calling SCIM Token API 2.0");
                }
            }
            http.disconnect();
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

    }
    public static void main(String[] args) throws IOException, JSONException {
        SCIMTokenCreation scim_obj = new SCIMTokenCreation();
        scim_obj.ServicePrincipalToken("7335eb25-17db-47da-8706-3611be3d0361","test-serviceprincipal");

    }

}
