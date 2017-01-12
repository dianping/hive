package org.apache.hadoop.hive.metastore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by sunyerui on 16/12/21.
 */
public class ObjectStoreWithBIMapping extends ObjectStore {
    private static final Log LOG = LogFactory.getLog(ObjectStoreWithBIMapping.class);

    private static class MappingName {
        String db;
        String table;

        public MappingName(String db, String table) {
            this.db = db;
            this.table = table;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MappingName)) {
                return false;
            }
            MappingName o = (MappingName)other;
            return this.db.equals(o.db) && this.table.equals(o.table);
        }

        @Override
        public int hashCode() {
            return (31 + 31 * db.hashCode()) * 31 + table.hashCode();
        }

        @Override
        public String toString() {
            return db + "." + table;
        }
    }

    public static final String MAPPING_FILE = "bi.mapping.properties";
    private Map<MappingName, MappingName> mapping;
    private static final Pattern MAPPING_PATTERN = Pattern.compile("^(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)\\s*$");

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        mapping = new HashMap<MappingName, MappingName>();
        loadMappingConfig();
    }

    private void loadMappingConfig() {
        String line;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(Configuration.class.getClassLoader().getResourceAsStream(MAPPING_FILE)));
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }
                Matcher matcher = MAPPING_PATTERN.matcher(line);
                if (matcher.find()) {
                    mapping.put(new MappingName(matcher.group(1), matcher.group(2)), new MappingName(matcher.group(3), matcher.group(4)));
                } else {
                    throw new IllegalArgumentException("Wrong format of BI mapping: " + line);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Load BI mapping file failed", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {}
            }
        }
    }

    private Table mapping(Table tbl) throws MetaException {
        if (tbl == null) {
            return tbl;
        }
        Map<MappingName, MappingName> usedMapping = mapping;
        MappingName name = new MappingName(tbl.getDbName(), tbl.getTableName());
        MappingName mappingName = usedMapping.get(name);
        // alter/create table with mapping name make the location with mapping path, which not exists
        // that's why forbid alter/create table with mapping name for now
        if (mappingName != null) {
//            tbl.getSd().getLocation().replace(tbl.getDbName() + ".db/" + tbl.getTableName(), mappingName.db + ".db/" + mappingName.table);
//            tbl.setDbName(mappingName.db);
//            tbl.setTableName(mappingName.table);
            throw new MetaException("Table " + tbl.getDbName() + "." + tbl.getTableName() + " mapping to " + mappingName);
        }
        LOG.info("Mapping Table from " + name + " to " + mappingName);
        return tbl;
    }

    private MappingName mapping(String db, String table) {
        Map<MappingName, MappingName> usedMapping = mapping;
        MappingName name = new MappingName(db, table);
        MappingName mappingName = usedMapping.get(name);
        LOG.info("Mapping db and table from " + name + " to " + mappingName);
        if (mappingName != null) {
            return mappingName;
        } else {
            return new MappingName(db, table);
        }
    }

    private Partition mapping(Partition part) {
        if (part == null) {
            return null;
        }
        Map<MappingName, MappingName> usedMapping = mapping;
        MappingName name = new MappingName(part.getDbName(), part.getTableName());
        MappingName mappingName = usedMapping.get(name);
        if (mappingName != null) {
            part.setDbName(mappingName.db);
            part.setTableName(mappingName.table);
        }
        LOG.info("Mapping Partition from " + name + " to " + mappingName);
        return part;
    }

    private Table setMapping(Table table, String dbName, String tableName) {
        if (table == null) {
            return null;
        }
        LOG.info("Set Mapping Table from " + table + " to " + dbName + "." + tableName);
        table.setDbName(dbName);
        table.setTableName(tableName);
        return table;
    }

    private Partition setMapping(Partition part, String dbName, String tableName) {
        if (part == null) {
            return null;
        }
        LOG.info("Set Mapping Partition from " + part + " to " + dbName + "." + tableName);
        part.setDbName(dbName);
        part.setTableName(tableName);
        return part;
    }

    @Override
    public void createTable(Table tbl) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping createTable " + tbl);
        super.createTable(mapping(tbl));
    }

    @Override
    public boolean dropTable(String dbName, String tableName) throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
        LOG.info("ObjectStoreWithBIMapping dropTable " + dbName + "." + tableName);
//        MappingName mappingName = mapping(dbName, tableName);
//        return super.dropTable(mappingName.db, mappingName.table);
        return super.dropTable(dbName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping getTable " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return setMapping(super.getTable(mappingName.db, mappingName.table), dbName, tableName);
    }

    @Override
    public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping addPartition " + part);
        return super.addPartition(mapping(part));
    }

    @Override
    public boolean addPartitions(String dbName, String tblName, List<Partition> parts) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping addPartitions " + dbName + "." + tblName);
        for (Partition part : parts) {
            mapping(part);
        }
        MappingName mappingName = mapping(dbName, tblName);
        return super.addPartitions(mappingName.db, mappingName.table, parts);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartition " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return setMapping(super.getPartition(mappingName.db, mappingName.table, part_vals), dbName, tableName);
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping doesPartitionExist " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return super.doesPartitionExist(mappingName.db, mappingName.table, part_vals);
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        LOG.info("ObjectStoreWithBIMapping dropPartition " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return super.dropPartition(mappingName.db, mappingName.table, part_vals);
    }

    @Override
    public void dropPartitions(String dbName, String tableName, List<String> partNames) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping dropPartitions " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        super.dropPartitions(mappingName.db, mappingName.table, partNames);
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, int max) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartitions " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        List<Partition> parts = super.getPartitions(mappingName.db, mappingName.table, max);
        for (Partition part : parts) {
            setMapping(part, dbName, tableName);
        }
        return parts;
    }

    @Override
    public void alterTable(String dbName, String tableName, Table newTable) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping alterTable " + dbName + "." + tableName + " " + newTable);
        MappingName mappingName = mapping(dbName, tableName);
        super.alterTable(mappingName.db, mappingName.table, mapping(newTable));
    }

    // filter with pattern ignore mapped tables
    @Override
    public List<String> getTables(String dbName, String pattern) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping getTables " + dbName + " " + pattern);
        return super.getTables(dbName, pattern);
    }

    @Override
    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames) throws MetaException, UnknownDBException {
        LOG.info("ObjectStoreWithBIMapping getTableObjectsByName " + dbName);
        Map<String, List<String>> mappingNames = new HashMap<String, List<String>>();
        for (String table : tableNames) {
            MappingName mappingName = mapping(dbName, table);
            List<String> newTableNames = mappingNames.get(mappingName.db);
            if (newTableNames == null) {
                newTableNames = new ArrayList<String>();
                mappingNames.put(mappingName.db, newTableNames);
            }
            newTableNames.add(mappingName.table);
        }
        List<Table> tables = new ArrayList<Table>(tableNames.size());
        for (String db : mappingNames.keySet()) {
            for (String table : mappingNames.get(db)) {
                tables.add(setMapping(super.getTable(db, table), db, table));
            }
        }
        return tables;
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping getAllTables " + dbName);
        return super.getAllTables(dbName);
    }

    // filter list ignore mapped tables
    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping listTableNamesByFilter " + dbName + " " + filter);
        return super.listTableNamesByFilter(dbName, filter, max_tables);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, short max_parts) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping listPartitionNames " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return super.listPartitionNames(mappingName.db, mappingName.table, max_parts);
    }

    @Override
    public List<String> listPartitionNamesByFilter(String dbName, String tableName, String filter, short max_parts) throws MetaException {
        LOG.info("ObjectStoreWithBIMapping listPartitionNamesByFilter " + dbName + "." + tableName + " " + filter);
        MappingName mappingName = mapping(dbName, tableName);
        return super.listPartitionNamesByFilter(mappingName.db, mappingName.table, filter, max_parts);
    }

    @Override
    public List<String> listPartitionNamesPs(String dbName, String tableName, List<String> part_vals, short max_parts) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping listPartitionNamesPs " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return super.listPartitionNamesPs(mappingName.db, mappingName.table, part_vals, max_parts);
    }

    @Override
    public void alterPartition(String dbName, String tableName, List<String> part_vals, Partition new_part) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping alterPartition " + dbName + "." + tableName + " " + new_part);
        MappingName mappingName = mapping(dbName, tableName);
        super.alterPartition(mappingName.db, mappingName.table, part_vals, mapping(new_part));
    }

    @Override
    public void alterPartitions(String dbName, String tableName, List<List<String>> part_vals_list, List<Partition> new_parts) throws InvalidObjectException, MetaException {
        LOG.info("ObjectStoreWithBIMapping alterPartitions " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        for (Partition part : new_parts) {
            mapping(part);
        }
        super.alterPartitions(mappingName.db, mappingName.table, part_vals_list, new_parts);
    }

    @Override
    public List<Partition> getPartitionsByFilter(String dbName, String tableName, String filter, short maxParts) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartitionsByFilter " + dbName + "." + tableName + " " + filter);
        MappingName mappingName = mapping(dbName, tableName);
        List<Partition> parts = super.getPartitionsByFilter(mappingName.db, mappingName.table, filter, maxParts);
        for (Partition part : parts) {
            setMapping(part, dbName, tableName);
        }
        return parts;
    }

    @Override
    public boolean getPartitionsByExpr(String dbName, String tableName, byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
        LOG.info("ObjectStoreWithBIMapping getPartitionsByExpr " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        boolean ret = super.getPartitionsByExpr(mappingName.db, mappingName.table, expr, defaultPartitionName, maxParts, result);
        for (Partition part : result) {
            setMapping(part, dbName, tableName);
        }
        return ret;
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> partNames) throws MetaException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartitionsByNames " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        List<Partition> parts = super.getPartitionsByNames(mappingName.db, mappingName.table, partNames);
        for (Partition part : parts) {
            setMapping(part, dbName, tableName);
        }
        return parts;
    }

    @Override
    public Partition getPartitionWithAuth(String dbName, String tableName, List<String> partVals, String user_name, List<String> group_names) throws MetaException, NoSuchObjectException, InvalidObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartitionWithAuth " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        return setMapping(super.getPartitionWithAuth(mappingName.db, mappingName.table, partVals, user_name, group_names), dbName, tableName);
    }

    @Override
    public List<Partition> getPartitionsWithAuth(String dbName, String tableName, short maxParts, String userName, List<String> groupNames) throws MetaException, NoSuchObjectException, InvalidObjectException {
        LOG.info("ObjectStoreWithBIMapping getPartitionsWithAuth " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        List<Partition> partitions = super.getPartitionsWithAuth(mappingName.db, mappingName.table, maxParts, userName, groupNames);
        for (Partition part : partitions) {
            setMapping(part, dbName, tableName);
        }
        return partitions;
    }

    @Override
    public List<Partition> listPartitionsPsWithAuth(String dbName, String tableName, List<String> part_vals, short max_parts, String userName, List<String> groupNames) throws MetaException, InvalidObjectException, NoSuchObjectException {
        LOG.info("ObjectStoreWithBIMapping listPartitionsPsWithAuth " + dbName + "." + tableName);
        MappingName mappingName = mapping(dbName, tableName);
        List<Partition> partitions = super.listPartitionsPsWithAuth(mappingName.db, mappingName.table, part_vals, max_parts, userName, groupNames);
        for (Partition part : partitions) {
            setMapping(part, dbName, tableName);
        }
        return partitions;
    }
}

