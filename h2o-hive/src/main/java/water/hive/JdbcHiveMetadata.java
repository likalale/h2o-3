package water.hive;


import com.sun.javafx.binding.StringFormatter;

import java.sql.*;
import java.util.*;

public class JdbcHiveMetadata implements HiveMetaData {

    private static final String SQL_DESCRIBE_TABLE = "DESCRIBE FORMATTED %s";
    private static final String SQL_DESCRIBE_PARTITION = "DESCRIBE FORMATTED %s PARTITION (%s)";
    private static final String SQL_SHOW_PARTS = "SHOW PARTITIONS %s";

    private final String url;

    public JdbcHiveMetadata(String url) {
        this.url = url;
    }

    static class StorableMetadata {
        String location;
        String serializationLib;
        String inputFormat;
        Map<String, String> serDeParams = Collections.emptyMap();
    }

    static class JdbcStorable implements Storable {

        private final String location;
        private final String serializationLib;
        private final String inputFormat;
        private final Map<String, String> serDeParams;

        JdbcStorable(StorableMetadata data) {
            this.location = data.location;
            this.serializationLib = data.serializationLib;
            this.inputFormat = data.inputFormat;
            this.serDeParams = data.serDeParams;
        }

        @Override
        public Map<String, String> getSerDeParams() {
            return serDeParams;
        }

        @Override
        public String getLocation() {
            return location;
        }

        @Override
        public String getSerializationLib() {
            return serializationLib;
        }

        @Override
        public String getInputFormat() {
            return inputFormat;
        }
    }

    static class JdbcPartition extends JdbcStorable implements Partition {

        private final List<String> values;

        JdbcPartition(StorableMetadata meta, List<String> values) {
            super(meta);
            this.values = values;
        }

        @Override
        public List<String> getValues() {
            return values;
        }
    }

    static class JdbcColumn implements Column {

        private final String name;
        private final String type;

        JdbcColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return type;
        }
    }

    static class JdbcTable extends JdbcStorable implements Table {

        private final String name;
        private final List<Partition> partitions;
        private final List<Column> columns;
        private final List<Column> partitionKeys;

        public JdbcTable(
            String name,
            StorableMetadata meta,
            List<Column> columns,
            List<Partition> partitions,
            List<Column> partitionKeys
        ) {
            super(meta);
            this.name = name;
            this.partitions = partitions;
            this.columns = columns;
            this.partitionKeys = partitionKeys;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean hasPartitions() {
            return !partitionKeys.isEmpty();
        }

        @Override
        public List<Partition> getPartitions() {
            return partitions;
        }

        @Override
        public List<Column> getColumns() {
            return columns;
        }

        @Override
        public List<Column> getPartitionKeys() {
            return partitionKeys;
        }
    }
    
    @Override
    public Table getTable(String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            return getTable(conn, tableName);
        }
    }

    private Table getTable(Connection conn, String name) throws SQLException {
        String query = String.format(SQL_DESCRIBE_TABLE, name);
        try (PreparedStatement describeStmt = conn.prepareStatement(query)) {
            describeStmt.setString(1, name);
            try (ResultSet rs = describeStmt.executeQuery()) {
                rs.next(); // go to first row
                rs.next(); // skip col_name header row
                List<Column> columns = readColumns(rs);
                List<Column> partitionKeys = readPartitionKeys(rs);
                List<Partition> partitions = readPartitions(conn, name, partitionKeys);
                StorableMetadata storableData = readStorableMetadata(rs);
                return new JdbcTable(name, storableData, columns, partitions, partitionKeys);
            }
        }
    }

    private StorableMetadata readStorableMetadata(ResultSet rs) throws SQLException {
        StorableMetadata res = new StorableMetadata();
        while (rs.next()) {
            String id = rs.getString(1);
            String value = rs.getString(2);
            if ("Location:".equals(id)) {
                res.location = value;
            } else if ("InputFormat:".equals(id)) {
                res.inputFormat = value;
            } else if ("SerDe Library:".equals(id)) {
                res.serializationLib = value;
            } else if ("Storage Desc Params:".equals(id)) {
                res.serDeParams = readSerDeParams(rs);
            }
        }
        return res;
    }

    private Map<String, String> readSerDeParams(ResultSet rs) throws SQLException {
        Map<String, String> serDeParams = new HashMap<>();
        while (rs.next()) {
            serDeParams.put(rs.getString(2), rs.getString(3));
        }
        return serDeParams;
    }

    private List<Partition> readPartitions(
        Connection conn,
        String tableName,
        List<Column> partitionKeys
    ) throws SQLException {
        if (partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }
        try (Statement partitionListStmt = conn.createStatement()) {
            List<Partition> parts = new ArrayList<>();
            String query = String.format(SQL_SHOW_PARTS, tableName);
            try (ResultSet rs = partitionListStmt.executeQuery(query)) {
                while (rs.next()) {
                    List<String> values = parsePartitionValues(rs.getString(1));
                    StorableMetadata data = readPartitionMetadata(conn, tableName, partitionKeys, values);
                    parts.add(new JdbcPartition(data, values));
                }
            }
            return parts;
        }
    }

    private StorableMetadata readPartitionMetadata(
        Connection conn,
        String tableName,
        List<Column> partitionKeys,
        List<String> values
    ) throws SQLException {
        String query = String.format(SQL_DESCRIBE_PARTITION, tableName, toPartitionIdentifier(partitionKeys, values));
        try (Statement describeStmt = conn.createStatement()) {
            try (ResultSet rs = describeStmt.executeQuery(query)) {
                rs.next(); // go to first row
                while (!rs.getString(1).contains("Detailed Partition Information")) {
                    rs.next(); // skip column infos                    
                }
                return readStorableMetadata(rs);
            }
        }
    }

    private String toPartitionIdentifier(List<Column> partitionKeys, List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionKeys.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(partitionKeys.get(i)).append(("=")).append(values.get(i));
        }
        return sb.toString();
    }

    private List<String> parsePartitionValues(String valuesStr) {
        List<String> values = new ArrayList<>();
        String[] colValuePairs = valuesStr.split("/");
        for (String pair : colValuePairs) {
            String[] colValue = pair.split("=");
            values.add(colValue[1]);
        }
        return values;
    }

    private List<Column> readPartitionKeys(ResultSet rs) throws SQLException {
        String header = rs.getString(1);
        if (!header.contains("Partition Information")) {
            return Collections.emptyList();
        } else {
            rs.next(); // skip another header row
            rs.next(); // skip blank separator row
            rs.next(); // go to first partition column row
            return readColumns(rs);
        }
    }

    private List<Column> readColumns(ResultSet rs) throws SQLException {
        String type;
        List<Column> columns = new ArrayList<>();
        while (!(type = rs.getString(2)).equals("NULL")) {
            String name = rs.getString(1);
            columns.add(new JdbcColumn(name, type));
            rs.next();
        }
        return columns;
    }

}
