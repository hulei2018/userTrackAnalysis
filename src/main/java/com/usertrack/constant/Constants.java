package com.usertrack.constant;

public interface Constants {
    // category/product id 分割字符串
    String SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR = "|";
    String SPLIT_CATEGORY_OR_PRODUCT_ID_SEPARATOR_ESCAOE = "\\|";

    String KAFKA_METADATA_BROKER_LIST="metadata.broker.list";
    String JDBC_DRIVER="jdbc_driver";
    String JDBC_URL="jdbc_url";
    String JDBC_USER="jdbc_user";
    String JDBC_PASSWORD="jdbc_password";
    String JDBC_DATASOURCE_SIZE="jdbc_datasource_size";
    String SPARK_APP_NAME="userTrackAnalysis";
    String ISLOCAL="is_local";
    String START_DATE="startDate";
    String END_DATE="endDate";
    String SEX="sex";
    String PROFESSIONALS="professionals";
}
