package com.meesho.prismdatabricksapi.service;

import java.util.HashMap;

public class DatabricksSCIM {
    public String application_id;
    public String service_principal;
    public HashMap<Object, String>  scim_map;

    public HashMap<Object, Object>  token_map;

    public DatabricksSCIM(String service_principal, String application_id, HashMap<Object, String> scim_map) {
        this.service_principal = service_principal;
        this.application_id = application_id;
        this.scim_map = scim_map;
    }

     DatabricksSCIM(String application_id, HashMap<Object, Object>  token_map) {
        this.application_id = application_id;
        this.token_map = token_map;
    }

}
