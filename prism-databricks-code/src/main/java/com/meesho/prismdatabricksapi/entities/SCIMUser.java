package com.meesho.prismdatabricksapi.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(schema = "prod",name = "scim_user")
@AllArgsConstructor
@NoArgsConstructor
@Data
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

    @Column(name = "admin_id")
    String created_by_id;

    public String toString(){
        return application_id+" "+display_name+" "+spn_token+" "+owner_email+" "+owner_id+" "+service_principal_id+" "+active+" "+group_name+" "+group_id+" "+token_owner+" "+token_creation_time+" "+token_expiry_time+" "+token_id;
    }





}