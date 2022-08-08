package com.meesho.prismdatabricksapi.service;

import java.util.ArrayList;
import java.util.HashMap;

public class DatabricksSCIM {
    public String application_id;
    public String service_principal;
    public HashMap<Object, String>  scim_map;

    public HashMap<Object, Object>  token_map;

    public ArrayList<String> spn_display_list;

    public ArrayList<String>spn_id_list;

    public DatabricksSCIM(String service_principal, String application_id, HashMap<Object, String> scim_map) {
        this.service_principal = service_principal;
        this.application_id = application_id;
        this.scim_map = scim_map;
    }

    public DatabricksSCIM(String application_id, HashMap<Object, Object>  token_map) {
        this.application_id = application_id;
        this.token_map = token_map;
    }

    public DatabricksSCIM(ArrayList<String> spn_display_list, ArrayList<String> spn_id_list) {
        this.spn_display_list = spn_display_list;
        this.spn_id_list = spn_id_list;
    }

}
