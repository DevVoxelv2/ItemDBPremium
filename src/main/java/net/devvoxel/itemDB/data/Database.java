package net.devvoxel.itemDB.data;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.devvoxel.itemDB.ItemDB;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.inventory.ItemStack;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Database {
    private final ItemDB plugin;
    private HikariDataSource dataSource;
    private DatabaseType type;
    private String table;
    private final java.util.concurrent.atomic.AtomicLong lastTimestamp = new java.util.concurrent.atomic.AtomicLong();

    public Database(ItemDB plugin) {
        this.plugin = plugin;
    }

    public void connect() throws SQLException {
        ConfigurationSection cfg = plugin.getConfig().getConfigurationSection("Database");
        if (cfg == null) {
            throw new SQLException("Database section missing in config.yml");
        }

        this.type = DatabaseType.fromConfig(cfg.getString("Type"));
        this.table = cfg.getString("Table", "itemdb_items");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setMaximumPoolSize(cfg.getInt("PoolSize", 10));
        hikariConfig.setMinimumIdle(Math.max(2, hikariConfig.getMaximumPoolSize() / 2));
        hikariConfig.setPoolName("ItemDBPool");
        hikariConfig.setConnectionTestQuery("SELECT 1");

        if (type == DatabaseType.MYSQL) {
            String host = cfg.getString("Host", "127.0.0.1");
            int port = cfg.getInt("Port", 3306);
            String database = cfg.getString("Database", "itemdb");
            String user = cfg.getString("User", "root");
            String pass = cfg.getString("Password", "");

            String url = "jdbc:mysql://" + host + ":" + port + "/" + database +
                    "?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=utf8&serverTimezone=UTC";
            hikariConfig.setJdbcUrl(url);
            hikariConfig.setUsername(user);
            hikariConfig.setPassword(pass);
            hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        } else {
            plugin.getDataFolder().mkdirs();
            String file = plugin.getDataFolder().toPath().resolve(cfg.getString("File", "itemdb.sqlite")).toString();
            hikariConfig.setJdbcUrl("jdbc:sqlite:" + file);
            hikariConfig.setDriverClassName("org.sqlite.JDBC");
            hikariConfig.addDataSourceProperty("foreign_keys", "true");
        }

        this.dataSource = new HikariDataSource(hikariConfig);

        plugin.getLogger().info("Connected to " + type.name().toLowerCase(Locale.ROOT) + " database.");

        try (Connection connection = getConnection()) {
            initTable(connection);
        }
    }

    private void initTable(Connection connection) throws SQLException {
        String itemColumnType = type == DatabaseType.MYSQL ? "LONGTEXT" : "TEXT";
        String textColumnType = type == DatabaseType.MYSQL ? "TEXT" : "TEXT";

        String sql = "CREATE TABLE IF NOT EXISTS `" + table + "` (" +
                "`name` VARCHAR(64) PRIMARY KEY," +
                "`item` " + itemColumnType + " NOT NULL," +
                "`display_name` VARCHAR(255)," +
                "`lore` " + textColumnType + "," +
                "`custom_model_data` INTEGER," +
                "`enchantments` " + textColumnType + "," +
                "`updated_at` BIGINT NOT NULL," +
                "`is_deleted` BOOLEAN NOT NULL DEFAULT FALSE" +
                ");";
        connection.createStatement().executeUpdate(sql);

        ensureColumnExists(connection, "updated_at", type == DatabaseType.MYSQL ? "BIGINT NOT NULL DEFAULT 0" : "INTEGER NOT NULL DEFAULT 0");
        ensureColumnExists(connection, "is_deleted", "BOOLEAN NOT NULL DEFAULT FALSE");

        String indexSql = "CREATE INDEX IF NOT EXISTS `idx_" + table + "_updated` ON `" + table + "` (`updated_at`);";
        connection.createStatement().executeUpdate(indexSql);
    }

    private void ensureColumnExists(Connection connection, String column, String definition) throws SQLException {
        if (columnExists(connection, column)) {
            return;
        }

        String sql = "ALTER TABLE `" + table + "` ADD COLUMN `" + column + "` " + definition + ";";
        connection.createStatement().executeUpdate(sql);

        if ("updated_at".equals(column)) {
            long now = Instant.now().toEpochMilli();
            String updateSql = "UPDATE `" + table + "` SET `updated_at` = ? WHERE `updated_at` = 0 OR `updated_at` IS NULL";
            try (PreparedStatement ps = connection.prepareStatement(updateSql)) {
                ps.setLong(1, now);
                ps.executeUpdate();
            }
        }
    }

    private boolean columnExists(Connection connection, String column) throws SQLException {
        if (hasColumn(connection, table, column)) {
            return true;
        }
        if (hasColumn(connection, table.toUpperCase(Locale.ROOT), column)) {
            return true;
        }
        return hasColumn(connection, table.toLowerCase(Locale.ROOT), column);
    }

    private boolean hasColumn(Connection connection, String tableName, String column) throws SQLException {
        try (ResultSet rs = connection.getMetaData().getColumns(connection.getCatalog(), null, tableName, column)) {
            return rs.next();
        }
    }

    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            throw new SQLException("Database not initialised");
        }
        return dataSource.getConnection();
    }

    public void saveItem(ItemRecord record) throws SQLException {
        String sql = "REPLACE INTO `" + table + "` (name,item,display_name,lore,custom_model_data,enchantments,updated_at,is_deleted) " +
                "VALUES (?,?,?,?,?,?,?,?)";
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, record.key());
            try {
                ps.setString(2, ItemSerializer.serialize(record.item()));
            } catch (Exception ex) {
                throw new SQLException("Unable to serialize item", ex);
            }
            ps.setString(3, record.displayName());
            ps.setString(4, loreToColumn(record.lore()));
            if (record.customModelData() == null) {
                ps.setNull(5, Types.INTEGER);
            } else {
                ps.setInt(5, record.customModelData());
            }
            ps.setString(6, enchantmentsToColumn(record.enchantments()));
            ps.setLong(7, record.updatedAt());
            ps.setBoolean(8, record.deleted());
            ps.executeUpdate();
        }
    }

    public boolean markDeleted(String key, long timestamp) throws SQLException {
        String sql = "UPDATE `" + table + "` SET is_deleted = TRUE, updated_at = ? WHERE name = ?";
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, timestamp);
            ps.setString(2, key);
            return ps.executeUpdate() > 0;
        }
    }

    public List<ItemRecord> loadAllItems() throws SQLException {
        String sql = "SELECT name,item,display_name,lore,custom_model_data,enchantments,updated_at,is_deleted FROM `" + table + "` WHERE is_deleted = FALSE";
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<ItemRecord> records = new ArrayList<>();
            while (rs.next()) {
                records.add(mapRecord(rs));
            }
            return records;
        }
    }

    public List<ItemRecord> fetchChanges(long since) throws SQLException {
        String sql = "SELECT name,item,display_name,lore,custom_model_data,enchantments,updated_at,is_deleted FROM `" + table + "` WHERE updated_at > ?";
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, since);
            try (ResultSet rs = ps.executeQuery()) {
                List<ItemRecord> changes = new ArrayList<>();
                while (rs.next()) {
                    changes.add(mapRecord(rs));
                }
                return changes;
            }
        }
    }

    public List<ItemRecord> search(String query, Integer customModelData, int limit) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT name,item,display_name,lore,custom_model_data,enchantments,updated_at,is_deleted FROM `" + table + "` WHERE is_deleted = FALSE AND (LOWER(name) LIKE ? OR LOWER(display_name) LIKE ? OR LOWER(lore) LIKE ?)");
        if (customModelData != null) {
            sql.append(" AND custom_model_data = ?");
        }
        sql.append(" ORDER BY updated_at DESC");
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }

        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            String like = "%" + query.toLowerCase(Locale.ROOT) + "%";
            ps.setString(1, like);
            ps.setString(2, like);
            ps.setString(3, like);
            if (customModelData != null) {
                ps.setInt(4, customModelData);
            }

            try (ResultSet rs = ps.executeQuery()) {
                List<ItemRecord> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapRecord(rs));
                }
                return results;
            }
        }
    }

    private ItemRecord mapRecord(ResultSet rs) throws SQLException {
        String key = rs.getString("name");
        String itemData = rs.getString("item");
        ItemStack item;
        try {
            item = ItemSerializer.deserialize(itemData);
        } catch (Exception ex) {
            throw new SQLException("Failed to deserialize item for key " + key, ex);
        }

        String display = rs.getString("display_name");
        String loreColumn = rs.getString("lore");
        List<String> lore = loreColumn == null || loreColumn.isEmpty()
                ? List.of()
                : Arrays.asList(loreColumn.split("\n"));

        Integer cmd = null;
        Object cmdObject = rs.getObject("custom_model_data");
        if (cmdObject != null) {
            cmd = rs.getInt("custom_model_data");
        }

        String enchantColumn = rs.getString("enchantments");
        Map<String, Integer> enchantments = new HashMap<>();
        if (enchantColumn != null && !enchantColumn.isEmpty()) {
            for (String part : enchantColumn.split("\n")) {
                String[] split = part.split(":");
                if (split.length == 3) {
                    String namespace = split[0];
                    String keyPart = split[1];
                    try {
                        int level = Integer.parseInt(split[2]);
                        enchantments.put(namespace + ":" + keyPart, level);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }
        }

        long updatedAt = rs.getLong("updated_at");
        boolean deleted = rs.getBoolean("is_deleted");

        return new ItemRecord(key, item, display, lore, cmd, enchantments, updatedAt, deleted);
    }

    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    public String table() {
        return table;
    }

    public DatabaseType type() {
        return type;
    }

    private String loreToColumn(List<String> lore) {
        if (lore == null || lore.isEmpty()) {
            return null;
        }
        return String.join("\n", lore);
    }

    private String enchantmentsToColumn(Map<String, Integer> enchantments) {
        if (enchantments == null || enchantments.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Integer> entry : enchantments.entrySet()) {
            String key = entry.getKey();
            int level = entry.getValue();
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(key).append(':').append(level);
        }
        return builder.toString();
    }

    public long now() {
        long current = Instant.now().toEpochMilli();
        while (true) {
            long prev = lastTimestamp.get();
            long next = Math.max(current, prev + 1);
            if (lastTimestamp.compareAndSet(prev, next)) {
                return next;
            }
        }
    }
}
