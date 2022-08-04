package com.meesho.prismdatabricksapi.constants;

public class Databricks {
//    public static String URL = "https://metabase-temp.meesho.com";
    public static String URL = "https://metabase-presto.meesho.com";
    public static String CARD_ID = "CARD_ID";
    public static String DASHBOARD_ID = "DASHBOARD_ID";
    public static String EXECUTE_QUESTION_ENDPOINT = "/api/card/" + CARD_ID + "/query";
    public static String CREATE_SESSION_ENDPOINT = "/api/session";
    public static String EXECUTE_DASHBOARD_ENDPOINT = "/api/dashboar/" + DASHBOARD_ID + "/query";
}
