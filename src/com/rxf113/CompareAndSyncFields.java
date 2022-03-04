import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 对比同步字段
 *
 * @author rxf113
 */
public class CompareAndSyncFields {
    /**
     * 标注库(以这个库为准)的sql文件路径，mysqldump -d 导出的结构
     */
    private static final String STANDARD_DATABASE_SQL_PATH = "C:\\Users\\rxf113\\Desktop\\sql索引比对\\205\\client_backup_nodata.sql";
    /**
     * 待修改库的sql文件路径，mysqldump -d 导出的结构
     */
    private static final String MODIFY_DATABASE_SQL_PATH = "C:\\Users\\rxf113\\Desktop\\sql索引比对\\生产库\\client_backup_withoutdata.sql";
    /**
     * 区分大小写 0:不区分
     */
    static final Pattern TABLE_PATTERN = Pattern.compile(".*CREATE TABLE\\s+(.*)\\s+.*");


    public static void main(String[] args) {
        Map<String, Set<TableFieldsModel>> standardTableToKeyMap = getTableToFieldsMap(STANDARD_DATABASE_SQL_PATH);

        Map<String, Set<TableFieldsModel>> modifyTableToKeyMap = getTableToFieldsMap(MODIFY_DATABASE_SQL_PATH);

        List<TableFieldsModel> needAdd = new ArrayList<>();
        List<TableFieldsModel> needDrop = new ArrayList<>();

        modifyTableToKeyMap.forEach(((tableName, tableFieldsModels) -> {
            Set<TableFieldsModel> models = standardTableToKeyMap.get(tableName);
            if (models == null) {
                if (!tableName.contains("temp") && !tableName.contains("tmp")) {
                    System.out.printf("这个表: [%s] 要删除%n", tableName);
                }
            }
        }));


        standardTableToKeyMap.forEach((tableName, standardTableFields) -> {
            Set<TableFieldsModel> modifyTableFields = modifyTableToKeyMap.get(tableName);
            if (modifyTableFields == null) {
                //这个表没有
                System.out.printf("%n 这个表: [%s] 需要新增%n", tableName);
            } else {
                //对比字段
                //删除
                List<TableFieldsModel> drop = modifyTableFields.stream().filter(i -> !standardTableFields.contains(i)).collect(Collectors.toList());
                //新增
                List<TableFieldsModel> add = standardTableFields.stream().filter(i -> !modifyTableFields.contains(i)).collect(Collectors.toList());
                //更新：存在于新增和删除中 字段名一样 但类型不一样
                Set<String> dropFields = drop.stream().map(TableFieldsModel::getField).collect(Collectors.toSet());
                List<TableFieldsModel> update = add.stream().filter(i -> dropFields.contains(i.getField())).collect(Collectors.toList());
                if (!update.isEmpty()) {
                    System.err.println("需要更新的表-字段");
                    System.err.println(update);
                }
                needAdd.addAll(add);
                needDrop.addAll(drop);
            }
        });

        StringBuilder res = new StringBuilder(256);

        for (TableFieldsModel tableFieldsModel : needAdd) {
            String sql = convertTableFieldsModelToSql(tableFieldsModel, 1);
            res.append(sql).append("\n");
        }
        for (TableFieldsModel tableFieldsModel : needDrop) {
            String sql = convertTableFieldsModelToSql(tableFieldsModel, 2);
            res.append(sql).append("\n");
        }
        System.out.println(res);

    }

    /**
     * ？？？
     *
     * @param tableFieldsModel f
     * @param num              1:新增, 2:删除
     * @return
     */
    private static String convertTableFieldsModelToSql(TableFieldsModel tableFieldsModel, int num) {
        if (num == 1) {
            return String.format("alter table %s add %s %s %s;", tableFieldsModel.getTable(), tableFieldsModel.getField(), tableFieldsModel.getType(), tableFieldsModel.getNullOrNotNull());
        }
        return String.format("alter table %s drop column %s;", tableFieldsModel.getTable(), tableFieldsModel.getField());
    }


    public static Map<String, Set<TableFieldsModel>> getTableToFieldsMap(String filePath) {
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
                if (line.matches("^\\s+`\\w+`.*")) {
                    bbb.append(line).append("\n");
                }
            }
            String[] tableFieldsArray = bbb.substring(2).split("##");
            Map<String, Set<TableFieldsModel>> tableName2Keys = new HashMap<>(tableFieldsArray.length, 1);
            for (String tableFields : tableFieldsArray) {
                if (tableFields.matches("CREATE TABLE\\s+.*\\s+\\(\\s+")) {
                    continue;
                }
                String[] lines = tableFields.split("\n");

                Set<TableFieldsModel> tableFieldsModels = Arrays.stream(lines).skip(1).map(i -> {
                    TableFieldsModel tableFieldsModel = convertLineToTableFieldsModel(i);
                    tableFieldsModel.setTable(lines[0]);
                    return tableFieldsModel;
                }).collect(Collectors.toSet());
                tableName2Keys.put(lines[0], tableFieldsModels);

            }
            return tableName2Keys;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    static final Pattern FIELD_PATTERN = Pattern.compile("^\\s+`(\\w*)`.*");
    static final Pattern TYPE_PATTERN = Pattern.compile("^\\s+`\\w+`\\s+(.*?)\\s+.*");
    static final Pattern NULL_NOT_PATTERN = Pattern.compile(".*\\s+((\\w*)\\s+NULL)");
    static final Pattern DEFAULT_PATTERN = Pattern.compile(".*\\s+(DEFAULT\\s+(.*?)\\s+).*");

    private static TableFieldsModel convertLineToTableFieldsModel(String line) {
        String field = getValByPattern(FIELD_PATTERN, line);
        String type = getValByPattern(TYPE_PATTERN, line);

        String nullOrNotNull = getNullOrNotNullOrDefault(line);
        return new TableFieldsModel(field, type, nullOrNotNull);
    }

    private static String getNullOrNotNullOrDefault(String str) {
        Matcher matcher = NULL_NOT_PATTERN.matcher(str);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        //查找默认值
        Matcher deMatch = DEFAULT_PATTERN.matcher(str);
        if (deMatch.find()) {
            return deMatch.group(1).trim();
        }
        return null;
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

}

class TableFieldsModel {

    public TableFieldsModel(String field, String type, String nullOrNotNull) {
        this.field = field.toLowerCase(Locale.ROOT);
        this.type = type.toLowerCase(Locale.ROOT);
        this.nullOrNotNull = nullOrNotNull;
    }

    public void setTable(String table) {
        this.table = table.toLowerCase(Locale.ROOT);
    }

    public String getTable() {
        return table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getNullOrNotNull() {
        return nullOrNotNull;
    }

    public void setNullOrNotNull(String nullOrNotNull) {
        this.nullOrNotNull = nullOrNotNull;
    }

    private String table;

    private String field;

    private String type;

    private String nullOrNotNull;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableFieldsModel that = (TableFieldsModel) o;
        return Objects.equals(table, that.table) && Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, field);
    }
}
