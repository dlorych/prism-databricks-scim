package com.meesho.prismdatabricksapi;

import com.meesho.prismdatabricksapi.entities.SCIMUser;

import com.meesho.prismdatabricksapi.repositories.SCIMUserRepo;
import com.meesho.prismdatabricksapi.service.DatabricksServicePrincipalManagement;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Optional;


@SpringBootApplication
public class SpringBootAppApplication
        implements CommandLineRunner {
    @Autowired SCIMUserRepo ob;
    public static void main(String[] args) throws JSONException, IOException {
//        DatabricksServicePrincipalManagement scim = new DatabricksServicePrincipalManagement();
//
//        //Callback function for getting the user mail id from the prism UI
//        String prism_owner_mail = "ankit.kalra@meesho.com";
//        String display_name = prism_owner_mail.replace("@meesho.com", "-serviceprincipal");
//        Boolean b= scim.GetListServicePrincipal(display_name);
//        if(b){
//            System.out.println("Databricks Service Principal already exist corresponding to the user "+prism_owner_mail + " on the AWS Databricks");
//        }
//        else {
//            String service_principal = scim.ServicePrincipalBySCIM(display_name);
//            System.out.println("Your Databricks Service Principal Username is " + service_principal);
//        }
        SpringApplication.run(SpringBootAppApplication.class, args);

    }
//    public void updateCustomerContacts(String application_id, String phone) {
//        Optional<SCIMUser> myCustomer = ob.findById(application_id);
//        myCustomer.phone = phone;
//        repo.save(myCustomer);
//    }


    @Override
    public void run(String... args) throws Exception
    {
        // Inserting the data in the mysql table.
        SCIMUser scimUser = new SCIMUser();
        //SCIMUser first = new SCIMUser("1","ffr","F","ff","ff",true,"d","ff","fff",new Date(),"Fff",new Date());
        //scimUser.setApplication_id("fff");
        ob.save(scimUser);
    }
}