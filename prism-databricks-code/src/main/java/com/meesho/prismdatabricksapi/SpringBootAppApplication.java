package com.meesho.prismdatabricksapi;

import com.meesho.prismdatabricksapi.entities.SCIMUser;
import com.meesho.prismdatabricksapi.repositories.SCIMUserRepo;
import com.meesho.prismdatabricksapi.service.*;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.io.IOException;
import java.util.*;


@SpringBootApplication
public class SpringBootAppApplication implements CommandLineRunner {
    @Autowired SCIMUserRepo object;
    public static void main(String[] args) throws JSONException, IOException {
        SpringApplication.run(SpringBootAppApplication.class, args);

    }

    public void printUsers(Collection<Optional<SCIMUser>> user, String msg) {
        for (Optional<SCIMUser> users: user) {
            System.out.println(msg);
            System.out.println(users);
        }
        System.out.println();
    }
    public void printAllUsers(Collection< SCIMUser > spn, String msg) {
        for (SCIMUser users_list: spn) {
            System.out.print(msg+"\n");
            System.out.println(users_list);
        }
        System.out.println();
    }

    @Override
    public void run(String... args) throws Exception {

        //Callback function for getting the user mail id from the prism UI
        String prism_owner_mail = "dbx_token@meesho.com";

        String display_name = prism_owner_mail.replace("@meesho.com", "-serviceprincipal");
        DatabricksSCIMGroups dbx_group= new DatabricksSCIMGroups();
        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();
        SCIMTokenCreation scim_obj = new SCIMTokenCreation();
        Boolean b = scim.GetListServicePrincipal(display_name);
        if (!b) {
            System.out.println("Databricks Service Principal already exist corresponding to the user " + prism_owner_mail + " on the AWS Databricks");
            Collection list= object.getSPNIDByDisplay(display_name);
            for(Object service_principal_id:list) {
                System.out.println("Service Principal ID for the Service Principal " + String.valueOf(service_principal_id));
                scim.GetServicePrincipalByID(String.valueOf(service_principal_id));
            }
        }
        else {
            String databricks_group_id=dbx_group.GetDatabricksGroupID();
            dbx_group.GetGroupDetailByID(databricks_group_id);
            DatabricksSCIM obj =scim.ServicePrincipalBySCIM(display_name,prism_owner_mail,databricks_group_id);
            System.out.println("Your Databricks Service Principal Username is " + obj.service_principal );
            System.out.println("Your Databricks Service Principal Application ID is " + obj.application_id );
            DatabricksSCIM dbx_token= scim_obj.ServicePrincipalToken(obj.application_id, obj.service_principal);
            System.out.print("Your token corresponding to your service principal "+ obj.service_principal+" is"+ dbx_token.token_map.get("token_value"));
            SCIMUser scimUser = new SCIMUser();
            scimUser.setApplication_id(obj.application_id);
            System.out.println("Storing the primary key application_id");
            object.save(scimUser);
            int update_records_scim = object.updateSPN((String) obj.scim_map.get("service_principal"), (String) obj.scim_map.get("owner_email"),(String) obj.scim_map.get("service_principal_id"), Boolean.parseBoolean((String) obj.scim_map.get("active")), (String) obj.scim_map.get("group_name"), (String) obj.scim_map.get("group_id"), obj.application_id);
            int update_records_token= object.updateServicePrincipalToken((String) dbx_token.token_map.get("token_id"), (String) dbx_token.token_map.get("spn_token"), (Date) dbx_token.token_map.get("token_expiry_time"), (Date) dbx_token.token_map.get("token_creation_time"), (String) dbx_token.token_map.get("token_owner"), (String) dbx_token.token_map.get("owner_id"),dbx_token.application_id);
            if (update_records_scim == 1 && update_records_token==1) {
                System.out.println("Storing the records into scim_user table for application_id " + obj.application_id);
                Optional<SCIMUser> user = object.findById(obj.application_id);
                printUsers(Arrays.asList(user), "Service Principal User Information");
                System.out.println("Records updated on scim_user table "+update_records_scim);
            } else {
                System.out.println("Failed to update the records on scim_user table in metastore database");
            }
            List<SCIMUser> users_list = object.findAll();
            printAllUsers(users_list, "Information of all SCIM Users");
        }



        /*** Put at the time whether query is running on SQL Endpoint on cluster by service principal user.
        If it is running then no need to stop or destroy container or
         if its not running in last 1 hour by particular user then destroy the container on EKS
         ***/

     // 1. API Call for last 1 hour runnable query based on service_principal_id
        SQLEndpointQueryHistory cluster_obj= new SQLEndpointQueryHistory();
        Collection list= object.getSPNIDByDisplay(display_name);
        for(Object service_principal_id:list) {

            // Input the service_principal name and we find service_principal_id
            Boolean bool=cluster_obj.FindSPNQueryHistory((String) service_principal_id);
            System.out.println(bool);
        }


    }
}