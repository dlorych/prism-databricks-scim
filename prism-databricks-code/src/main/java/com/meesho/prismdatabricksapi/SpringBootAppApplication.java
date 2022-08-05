package com.meesho.prismdatabricksapi;

import com.meesho.prismdatabricksapi.entities.SCIMUser;

import com.meesho.prismdatabricksapi.repositories.SCIMUserRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Date;


@SpringBootApplication
public class SpringBootAppApplication
        implements CommandLineRunner {
    @Autowired SCIMUserRepo ob;
    public static void main(String[] args)
    {
        SpringApplication.run(SpringBootAppApplication.class, args);
    }
//    public void updateCustomerContacts(String application_id, String phone) {
//        Customer myCustomer = repo.findById(id);
//        myCustomer.phone = phone;
//        repo.save(myCustomer);
//    }
    @Override
    public void run(String... args) throws Exception
    {
        // Inserting the data in the mysql table.
        SCIMUser scimUser = new SCIMUser();
        //SCIMUser first = new SCIMUser("1","ffr","F","ff","ff",true,"d","ff","fff",new Date(),"Fff",new Date());
        scimUser.setApplication_id("fff");
        ob.save(scimUser);
    }
}