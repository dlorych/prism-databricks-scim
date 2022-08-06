package com.meesho.prismdatabricksapi.entities;


import javax.persistence.*;
import java.util.Date;

@Entity
@Table(schema = "prod",name = "scim_user")
public class SCIMUser {
    @Id
    @Column(name = "application_id")
    String application_id;

    @Column(name = "spn_token")
    String spn_token;
    @Column(name = "display_name")
    String display_name;

    @Column(name = "owner_email")
    String owner_email;
    @Column(name = "service_principal_id")
    String service_principal_id;
    @Column(name = "active")
    Boolean active;
    @Column(name="owner_id")
    String owner_id;

    @Column(name = "group_name")
    String group_name;
    @Column(name = "group_id")
    String group_id;
    @Column(name = "token_id")
    String token_id;
    @Column(name = "token_expiry_time")
    Date token_expiry_time;
    @Column(name = "token_owner")
    String token_owner;
    @Column(name = "token_creation_time")
    Date token_creation_time;

    public void setApplication_id(String application_id) {
        this.application_id = application_id;
    }

    public String getApplication_id() {
        return application_id;
    }
    public void setDisplay_name(String display_name) {
        this.display_name = display_name;
    }

    public String getDisplay_name() {
        return display_name;
    }
    public void setSpn_token(String spn_token) {
        this.spn_token = spn_token;
    }

    public String getSpn_token() {
        return spn_token;
    }

    public void setService_principal_id(String service_principal_id) {
        this.service_principal_id = service_principal_id;
    }

    public String getService_principal_id() {
        return service_principal_id;
    }
    public void setOwner_email(String owner_email) {
        this.owner_email = owner_email;
    }

    public String getOwner_email() {
        return owner_email;
    }
    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getActive() {
        return active;
    }
    public void setGroup_name(String group_name) {
        this.group_name = group_name;
    }

    public String getGroup_name() {
        return group_name;
    }
    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getGroup_id() {
        return group_id;
    }
    public void setToken_id(String token_id) {
        this.token_id = token_id;
    }

    public String getToken_id() {
        return token_id;
    }
    public void setToken_owner(String token_owner) {
        this.token_owner = token_owner;
    }

    public String getToken_owner() {
        return token_owner;
    }
    public void setToken_expiry_time(Date token_expiry_time) {
        this.token_expiry_time = token_expiry_time;
    }

    public Date getToken_expiry_time() {
        return token_expiry_time;
    }
    public void setToken_creation_time(Date token_creation_time) {
        this.token_creation_time = token_creation_time;
    }

    public Date getToken_creation_time() {
        return token_creation_time;
    }
    public void setOwner_id(){
        this.owner_id=owner_id;
    }
    public String getOwner_id(){
        return owner_id;
    }
    public String toString(){//overriding the toString() method
        return application_id+" "+display_name+" "+spn_token+" "+owner_email+" "+owner_id+" "+service_principal_id+" "+active+" "+group_name+" "+group_id+" "+token_owner+" "+token_creation_time+" "+token_expiry_time+" "+token_id;
    }

    public SCIMUser() {

    }
    public SCIMUser(String spn_token, String display_name, String application_id, String owner_email, String service_principal_id, Boolean active, String group_name, String group_id, String token_id, Date token_expiry_time, String token_owner,
                    Date token_creation_time)
    {

     this.display_name=display_name;
     this.active=active;
     this.application_id=application_id;
     this.token_creation_time=token_creation_time;
     this.service_principal_id=service_principal_id;
     this.group_id=group_id;
     this.group_name=group_name;
     this.owner_email=owner_email;
     this.spn_token=spn_token;
     this.token_expiry_time=token_expiry_time;
     this.token_id=token_id;
     this.token_owner=token_owner;
    }





}