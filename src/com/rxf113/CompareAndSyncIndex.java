package com.rxf113;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 数据库索引同步
 *
 * @author rxf113
 */
public class CompareAndSyncIndex {

    /**
     * 标注库(以这个库为准)的sql文件路径，mysqldump -d 导出的结构
     */
    private static final String STANDARD_DATABASE_SQL_PATH = "C:\\Users\\rxf113\\Desktop\\sql索引比对\\39.76\\client_backup_nodata.sql";
    /**
     * 待修改库的sql文件路径，mysqldump -d 导出的结构
     */
    private static final String MODIFY_DATABASE_SQL_PATH = "C:\\Users\\rxf113\\Desktop\\sql索引比对\\生产库\\client_backup_withoutdata.sql";
    /**
     * 区分大小写 0:不区分
     */
    private static final Integer CASE_INSENSITIVE = 0;
    /**
     * 主键
     */
    private static final String PK = "PRIMARY KEY";
    /**
     * 唯一索引
     */
    private static final String UK = "UNIQUE KEY";

    public static void main(String[] args) {
        Map<String, List<KeyModel>> standardTableToKeyMap = getTableToKeysMap(STANDARD_DATABASE_SQL_PATH);
        Map<String, List<KeyModel>> modifyTableToKeyMap = getTableToKeysMap(MODIFY_DATABASE_SQL_PATH);

        List<KeyModel> needCreate = new ArrayList<>();
        List<KeyModel> needDrop = new ArrayList<>();


        standardTableToKeyMap.forEach((tableName, mainKeyModels) -> {
            List<KeyModel> existsKeyModels = modifyTableToKeyMap.get(tableName);
            if (existsKeyModels == null) {
                //需要新增
                needCreate.addAll(mainKeyModels);
            } else {
                try {
                    Map<String, KeyModel> existsFieldsToKeyModel = existsKeyModels.stream().collect(Collectors.toMap(KeyModel::getFields, i -> i));
                    Map<String, KeyModel> mainFieldsToKeyModel = mainKeyModels.stream().collect(Collectors.toMap(KeyModel::getFields, i -> i));
                    //需要创建和删除的
                    List<KeyModel> create = mainKeyModels.stream().filter(i -> !existsFieldsToKeyModel.containsKey(i.getFields())).collect(Collectors.toList());
                    List<KeyModel> drop = existsKeyModels.stream().filter(i -> !mainFieldsToKeyModel.containsKey(i.getFields())).collect(Collectors.toList());
                    needCreate.addAll(create);
                    needDrop.addAll(drop);
                } catch (Exception e) {
                    e.printStackTrace();
                    //一般问题，同一个字段建了多个索引
                    System.err.printf("%n 你这个表有问题啊: %s", tableName);
                }

            }
        });

        StringBuilder res = new StringBuilder(256);
        //将KeyModel 转换为具体语句

        for (KeyModel keyModel : needCreate) {
            String sql = convertKeyModelToSql(keyModel, 1);
            res.append(sql).append("\n");
        }
        for (KeyModel keyModel : needDrop) {
            String sql = convertKeyModelToSql(keyModel, 2);
            res.append(sql).append("\n");
        }
        System.out.println(res);
    }

    static final Pattern TABLE_PATTERN = Pattern.compile(".*CREATE TABLE\\s+(.*)\\s+.*");

    /**
     * @param filePath mysqldump出的表结构sql
     * @return table->keys
     */
    public static Map<String, List<KeyModel>> getTableToKeysMap(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            StringBuilder bbb = new StringBuilder(1024 * 1024);
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.matches("^CREATE TABLE.*")) {
                    Matcher matcher = TABLE_PATTERN.matcher(line);
                    if (matcher.find()) {
                        bbb.append("##").append(matcher.group(1)).append("\n");
                    }
                }
                if (line.matches("^\\s+PRIMARY KEY.*") || line.matches("^\\s+KEY.*") || line.matches("^\\s+UNIQUE KEY.*")) {
                    bbb.append(line).append("\n");
                }
            }
            String[] tableKeysArray = bbb.substring(2).split("##");
            Map<String, List<KeyModel>> tableName2Keys = new HashMap<>(tableKeysArray.length, 1);
            for (String tableKeys : tableKeysArray) {
                if (tableKeys.matches("CREATE TABLE\\s+.*\\s+\\(\\s+")) {
                    continue;
                }
                String[] lines = tableKeys.split("\n");
                List<KeyModel> keys = Arrays.stream(lines).skip(1).map(i -> {
                    KeyModel keyModel = convertLineToKeyModel(i);
                    keyModel.setTable(lines[0]);
                    return keyModel;
                }).sorted(Comparator.comparing(KeyModel::getFields)).collect(Collectors.toList());
                if (!keys.isEmpty()) {
                    tableName2Keys.put(lines[0], keys);
                }

            }
            return tableName2Keys;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static final Pattern KEY_TYPE_PATTERN = Pattern.compile("^(.*KEY)");
    static final Pattern KEY_NAME_PATTERN = Pattern.compile("KEY\\s(.*?)\\s");
    static final Pattern KEY_FIELDS_PATTERN = Pattern.compile("\\((.*)\\)");
    //static final Pattern KEY_STRUCT_PATTERN = Pattern.compile("USING\\s(.*)$"); 暂时都默认btree


    private static KeyModel convertLineToKeyModel(String line) {
        String type = getValByPattern(KEY_TYPE_PATTERN, line);
        String name = null;
        if (!PK.equals(type)) {
            name = getValByPattern(KEY_NAME_PATTERN, line);
            if (CASE_INSENSITIVE == 0) {
                name = name.toLowerCase(Locale.ROOT);
            }
        }

        String fields = getValByPattern(KEY_FIELDS_PATTERN, line);
        if (CASE_INSENSITIVE == 0) {
            fields = fields.toLowerCase(Locale.ROOT);
        }
        //String struct = getValByPattern(KEY_STRUCT_PATTERN, line);
        return new KeyModel(type, name, null, fields, null);
    }

    private static String getValByPattern(Pattern pattern, String str) {
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        System.err.printf("%n pattern: %s", pattern);
        System.err.printf("%n str: %s", str);
        throw new NoSuchElementException("提取值失败!");
    }

    /**
     * @param keyModel m
     * @param model    1 create,2 drop
     * @return sql
     */
    private static String convertKeyModelToSql(KeyModel keyModel, int model) {
        String type = keyModel.getType();
        String table = keyModel.getTable();
        String fields = keyModel.getFields();
        String name = keyModel.getName();
        if (model == 1) {
            return convertKeyModelToCreateSql(type, table, fields, name);
        }
        return convertKeyModelToDropSql(type, table, fields, name);
    }

    private static String convertKeyModelToCreateSql(String type, String table, String fields, String name) {
        if (PK.equals(type)) {
            return String.format("alter table %s add constraint %s_pk primary key (%s);", table, table.replace("`", ""), fields);
        } else if (UK.equals(type)) {
            return String.format("create unique index %s on %s (%s);", name, table, fields);
        } else {
            return String.format("create index %s on %s (%s);", name, table, fields);
        }
    }

    private static String convertKeyModelToDropSql(String type, String table, String fields, String name) {
        if (PK.equals(type)) {
            return String.format("alter table %s drop primary key;", table);
        } else {
            return String.format("drop index %s on %s;", name, table);
        }
    }
}

class KeyModel {

    public KeyModel(String type, String name, String table, String fields, String struct) {
        this.type = type;
        this.name = name;
        this.table = table;
        this.fields = fields;
        this.struct = struct;
    }

    /**
     * 索引类型: key / primary key / unique key
     */
    private String type;
    /**
     * 索引名称
     */
    private String name;
    /**
     * 表
     */
    private String table;
    /**
     * 索引字段
     */
    private String fields;
    /**
     * 索引结构: btree等
     */
    private String struct;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyModel keyModel = (KeyModel) o;
        //根据 type 和 字段 区分索引是否相同
        return keyModel.getType().equals(this.type) && keyModel.getFields().equals(this.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, fields);
    }
}