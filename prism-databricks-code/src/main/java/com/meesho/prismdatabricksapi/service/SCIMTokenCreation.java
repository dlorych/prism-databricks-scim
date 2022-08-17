package com.meesho.prismdatabricksapi.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meesho.prismdatabricksapi.configs.ApplicationProperties;
import org.json.*;

public class SCIMTokenCreation {
    static Logger log = Logger.getLogger(SCIMTokenCreation.class.getName());
    private ApplicationProperties properties;
    public DatabricksSCIM ServicePrincipalToken(String scim_application_id, String service_principal_display_name) throws IOException, JSONException, ParseException {
        this.properties = new ApplicationProperties();
        String databricks_host= properties.getValue("databricks_host");
        String databricks_master_access_token= String.format("Basic %1$s",properties.getValue("databricks_master_access_token"));
        String spn_token_endpoint= String.format("%1$sapi/2.0/token-management/on-behalf-of/tokens",databricks_host);
        String token_value=null;
        Date token_creation_time=null;
        Date token_expiry_time=null;
        String token_id=null;
        String owner_id=null;
        String token_owner=null;
        HashMap<Object, Object> token_map = null;
        URL url = new URL(spn_token_endpoint);
        HttpURLConnection http = (HttpURLConnection)url.openConnection();
        http.setRequestMethod("POST");
        http.setDoOutput(true);
        http.setRequestProperty("Content-type", "application/json");
        http.setRequestProperty("Authorization", databricks_master_access_token);
        String data = String.format("{\n  \"application_id\": \"%1$s\",\n  \"comment\":" +
                " \"This the token for the Service Principal user %2$s\",\n  " +
                "\"lifetime_seconds\":  138240000 \n}",scim_application_id, service_principal_display_name);
        byte[] out = data.getBytes(StandardCharsets.UTF_8);
        OutputStream stream = http.getOutputStream();
        stream.write(out);
        int code = http.getResponseCode();
        if(code==200 || code==201){
            log.info("HTTP Response for Databricks SCIM Token API Endpoint /api/2.0/token-management/on-behalf-of/token");
            log.info("HTTP Response Status Code " + http.getResponseCode());
            log.info("HTTP Response Status Message " + http.getResponseMessage());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (http.getInputStream())));
            String response_output;
            while ((response_output = br.readLine()) != null) {
                if (!Objects.isNull(response_output) || response_output != null || !response_output.isEmpty() || !response_output.trim().isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Object json_obj = mapper.readValue(response_output, Object.class);
                    String response_output_json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json_obj);
                    log.info("Sevice Principal Token Details for Application ID "+scim_application_id);
                    log.info("\n" + response_output_json);
                    JSONObject json_object = null;
                    JSONObject json_object_ = null;
                    try {
                        json_object = new JSONObject(response_output);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    token_value = json_object.getString("token_value");
                    try {
                        json_object_ = json_object.getJSONObject("token_info");
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                    SimpleDateFormat dateFormat = new SimpleDateFormat();
                    dateFormat.setTimeZone(TimeZone.getTimeZone("IST"));
                    token_creation_time = dateFormat.parse(dateFormat.format(json_object_.getDouble("creation_time")));
                    token_id = json_object_.getString("token_id");
                    owner_id = json_object_.getString("owner_id");
                    token_owner = json_object_.getString("created_by_username");
                    token_expiry_time = dateFormat.parse(dateFormat.format(json_object_.getDouble("expiry_time")));
                    token_map = new HashMap<Object, Object>();
                    token_map.put("spn_token",token_value);
                    token_map.put("token_creation_time",token_creation_time);
                    token_map.put("token_id",token_id);
                    token_map.put("owner_id",owner_id);
                    token_map.put("token_owner",token_owner);
                    token_map.put("token_expiry_time",token_expiry_time);
                }
                else{
                    log.info("No output response is generated from calling SCIM Token API 2.0");
                }
            }
        }
        else if(code==401 || code==403){
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Request is unauthorised because it lacks valid authentication credentials for the requested resource. Hence, not able to create spn token for Application ID "+scim_application_id);
          
        }
        else if (code==404){
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The requested resource does not exist. Hence, not able to create spn token for Application ID "+scim_application_id);
          
        }
        else if(code==400){
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("The request is malformed requested by the client user. Hence, not able to create spn token for Application ID "+scim_application_id);
          
        }
        else if(code==500){
            log.info("The request is not handled correctly due to a server error. Hence, not able to create spn token for Application ID "+scim_application_id);
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
          
        }
        else{
            log.info("HTTP Response Status Code " + http.getResponseCode() + " HTTP Response Status Message " + http.getResponseMessage());
            log.info("Bad Request, Not able to call Databricks SCIM API. Hence, not able to create spn token for Application ID "+scim_application_id);
          
        }
        http.disconnect();
        return new DatabricksSCIM(scim_application_id,token_map);

    }
    public static void main(String[] args) throws IOException, JSONException, ParseException {
        // Test the API for Service Principal SCIM Token Generation
        String prism_owner_mail = "spn.token@meesho.com";
        String display_name = prism_owner_mail.replace("@meesho.com", "-serviceprincipal");
        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();
        DatabricksSCIMGroups dbx_group= new DatabricksSCIMGroups();
        SCIMTokenCreation scim_obj = new SCIMTokenCreation();
        DatabricksSCIM list_obj= scim.GetListServicePrincipal();
        Boolean b= list_obj.spn_display_list.contains(display_name);
        if(b){
            log.info("Databricks Service Principal already exist corresponding to the user "+prism_owner_mail + " on the AWS Databricks");
        }
        else {
            String databricks_group_id=dbx_group.GetDatabricksGroupID();
            dbx_group.GetGroupDetailByID(databricks_group_id);
            DatabricksSCIM obj =scim.ServicePrincipalBySCIM(display_name,prism_owner_mail,databricks_group_id);
            log.info("Your Databricks Service Principal Username is " + obj.service_principal );
            log.info("Your Databricks Service Principal Application ID is " + obj.application_id );
            DatabricksSCIM dbx_token= scim_obj.ServicePrincipalToken(obj.application_id, obj.service_principal);
            log.info("Your token corresponding to your service principal "+ obj.service_principal+" is"+ dbx_token.token_map.get("token_value"));

        }


    }

}
