package lk.ijse.dep9.orm;

import lk.ijse.dep9.orm.annotations.Id;
import lk.ijse.dep9.orm.annotations.Table;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

public class InitializeDB {

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
    public static void initialize(String host, String port,String database, String username, String password, String ...packagesToScan){
        String url="jdbc:mysql://%s:%s/%s?createDatabaseIfNotExist=true";
        url=String.format(url,host,port,database);
        Connection connection;
        try {
            connection=DriverManager.getConnection(url,username,password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<String > classNames=new ArrayList<>();

        for (String packageToScan : packagesToScan) {
            var packageName=packageToScan;
            packageToScan = packageToScan.replaceAll("[.]", "/");//lk.ijse.dep9.entity-> lk/ijse/dep9/entity
            URL packageUrl = InitializeDB.class.getResource("/" + packageToScan);

            try {
                File file = new File(packageUrl.toURI());
                classNames.addAll(Arrays.asList(file.list()).stream().map(name->packageName+"."+name.replace(".class",""))
                        .collect(Collectors.toList()));

            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }

        }

        for (String className : classNames) {
            try {
                Class<?> loadedClass = Class.forName(className);
                Table tableAnnotation = loadedClass.getDeclaredAnnotation(Table.class);
                if (tableAnnotation!=null){
                    createTable(loadedClass,connection);
                }

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

    }
    private static void createTable(Class<?> classObj, Connection connection){
        StringBuilder ddlBuilder=new StringBuilder();
        Map<Class<?>, String > dataTypes = new HashMap<>();
        dataTypes.put(String.class,"VARCHAR(256)");
        dataTypes.put(int.class,"INT");
        dataTypes.put(Integer.class,"INT");
        dataTypes.put(double.class,"DOUBLE(10,2)");
        dataTypes.put(Double.class,"DOUBLE(10,2)");
        dataTypes.put(BigDecimal.class,"DECIMAL(10,2)");
        dataTypes.put(Date.class,"Date");
        dataTypes.put(Time.class,"Time");
        dataTypes.put(Timestamp.class,"DATETIME");
        ddlBuilder.append("CREATE TABLE IF NOT EXISTS `").append(classObj.getSimpleName()).append("`(");

        Field[] declaredFields = classObj.getDeclaredFields();
        for (Field field:declaredFields){
            String name= field.getName();
            Class<?> type = field.getType();
            Id primaryKey = field.getDeclaredAnnotation(Id.class);
            if (!dataTypes.containsKey(type)) throw new RuntimeException("We do not support for the data type "+type+" yet");
            ddlBuilder.append("`").append(name).append("`").append(" ").append(dataTypes.get(type));
            if (primaryKey!=null){
                ddlBuilder.append(" PRIMARY KEY,");
            }else {
                ddlBuilder.append(",");
            }
        }
        ddlBuilder.deleteCharAt(ddlBuilder.length()-1).append(")");

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(String.valueOf(ddlBuilder));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
}
