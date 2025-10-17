package net.devvoxel.itemDB.data;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.devvoxel.itemDB.ItemDB;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.inventory.ItemStack;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
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
import java.util.Optional;
import java.util.regex.Pattern;

public class Database {
    private final ItemDB plugin;
    private HikariDataSource dataSource;
    private DatabaseType type;
    private String table;
    private final String versionsTable = "item_versions";
    private final String auditTable = "item_audit";
    private final java.util.concurrent.atomic.AtomicLong lastTimestamp = new java.util.concurrent.atomic.AtomicLong();

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoItems;
    private MongoCollection<Document> mongoVersions;
    private MongoCollection<Document> mongoAudit;
    private String mongoCollectionPrefix;

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

        if (type == DatabaseType.MONGODB) {
            connectMongo(cfg);
            plugin.getLogger().info("Connected to mongodb database.");
            return;
        }

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
            initHistoryTables(connection);
        }
    }

    private void connectMongo(ConfigurationSection cfg) throws SQLException {
        this.mongoCollectionPrefix = cfg.getString("MongoCollectionPrefix", "itemdb_");
        String uri = cfg.getString("MongoConnectionUri", "mongodb://127.0.0.1:27017/itemdb");
        String databaseName = cfg.getString("MongoDatabase", cfg.getString("Database", "itemdb"));
        try {
            this.mongoClient = MongoClients.create(uri);
            this.mongoDatabase = mongoClient.getDatabase(databaseName);
            this.mongoItems = mongoDatabase.getCollection(mongoCollectionPrefix + "items");
            this.mongoVersions = mongoDatabase.getCollection(mongoCollectionPrefix + "versions");
            this.mongoAudit = mongoDatabase.getCollection(mongoCollectionPrefix + "audit");
            ensureMongoIndexes();
        } catch (MongoException ex) {
            throw new SQLException("Unable to connect to MongoDB", ex);
        }
    }

    private void ensureMongoIndexes() {
        try {
            mongoItems.createIndex(Indexes.ascending("name"), new IndexOptions().unique(true));
            mongoItems.createIndex(Indexes.descending("updated_at"));
            mongoVersions.createIndex(Indexes.descending("item_name"));
            mongoVersions.createIndex(Indexes.compoundIndex(Indexes.ascending("item_name"), Indexes.descending("version")),
                    new IndexOptions().unique(true));
            mongoAudit.createIndex(Indexes.descending("created_at"));
        } catch (MongoException ex) {
            plugin.getLogger().warning("Failed to ensure MongoDB indexes: " + ex.getMessage());
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

    private void initHistoryTables(Connection connection) throws SQLException {
        String itemColumnType = type == DatabaseType.MYSQL ? "LONGTEXT" : "TEXT";
        String textColumnType = type == DatabaseType.MYSQL ? "TEXT" : "TEXT";
        String idDefinition = type == DatabaseType.MYSQL ? "BIGINT AUTO_INCREMENT PRIMARY KEY" : "INTEGER PRIMARY KEY AUTOINCREMENT";

        String versionsSql = "CREATE TABLE IF NOT EXISTS `" + versionsTable + "` (" +
                "`id` " + idDefinition + "," +
                "`item_name` VARCHAR(128) NOT NULL," +
                "`version` INTEGER NOT NULL," +
                "`editor` VARCHAR(64)," +
                "`nbt` " + itemColumnType + " NOT NULL," +
                "`created_at` BIGINT NOT NULL," +
                "`comment` " + textColumnType + "," +
                "`is_deleted` BOOLEAN NOT NULL DEFAULT FALSE" +
                ");";
        connection.createStatement().executeUpdate(versionsSql);

        String versionsIndex = "CREATE INDEX IF NOT EXISTS `idx_" + versionsTable + "_item` ON `" + versionsTable + "` (`item_name`);";
        connection.createStatement().executeUpdate(versionsIndex);

        String versionsUnique = "CREATE UNIQUE INDEX IF NOT EXISTS `idx_" + versionsTable + "_uniq` ON `" + versionsTable + "` (`item_name`, `version`);";
        connection.createStatement().executeUpdate(versionsUnique);

        String auditSql = "CREATE TABLE IF NOT EXISTS `" + auditTable + "` (" +
                "`id` " + idDefinition + "," +
                "`action` VARCHAR(64) NOT NULL," +
                "`item_name` VARCHAR(128)," +
                "`actor` VARCHAR(64)," +
                "`details` " + textColumnType + "," +
                "`created_at` BIGINT NOT NULL" +
                ");";
        connection.createStatement().executeUpdate(auditSql);

        String auditIndex = "CREATE INDEX IF NOT EXISTS `idx_" + auditTable + "_created` ON `" + auditTable + "` (`created_at`);";
        connection.createStatement().executeUpdate(auditIndex);
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

    public void saveItem(ItemRecord record, String editor, String comment) throws SQLException {
        String serialized;
        try {
            serialized = ItemSerializer.serialize(record.item());
        } catch (IOException ex) {
            throw new SQLException("Unable to serialize item", ex);
        }

        if (type == DatabaseType.MONGODB) {
            saveItemMongo(record, serialized, editor, comment);
            return;
        }

        String sql = "REPLACE INTO `" + table + "` (name,item,display_name,lore,custom_model_data,enchantments,updated_at,is_deleted) " +
                "VALUES (?,?,?,?,?,?,?,?)";

        try (Connection connection = getConnection()) {
            boolean previous = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, record.key());
                ps.setString(2, serialized);
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

                insertVersion(connection, record, serialized, editor, comment);
                recordAudit(connection, "save", record.key(), editor, comment, record.updatedAt());
                connection.commit();
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(previous);
            }
        }
    }

    private void saveItemMongo(ItemRecord record, String serialized, String editor, String comment) throws SQLException {
        try {
            Document document = buildMongoItemDocument(record, serialized);
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            mongoItems.replaceOne(Filters.eq("_id", record.key()), document, options);

            int version = nextMongoVersion(record.key());
            mongoVersions.insertOne(buildMongoVersionDocument(record.key(), version, serialized, editor, comment, record.updatedAt(), record.deleted()));
            mongoAudit.insertOne(buildMongoAuditDocument("save", record.key(), editor, comment, record.updatedAt()));
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    public boolean markDeleted(ItemRecord record, long timestamp, String editor, String comment) throws SQLException {
        String serialized;
        try {
            serialized = ItemSerializer.serialize(record.item());
        } catch (IOException ex) {
            throw new SQLException("Unable to serialize item", ex);
        }

        if (type == DatabaseType.MONGODB) {
            return markDeletedMongo(record, timestamp, serialized, editor, comment);
        }

        String sql = "UPDATE `" + table + "` SET is_deleted = TRUE, updated_at = ? WHERE name = ?";

        try (Connection connection = getConnection()) {
            boolean previous = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setLong(1, timestamp);
                ps.setString(2, record.key());
                int updated = ps.executeUpdate();
                if (updated > 0) {
                    ItemRecord deleted = record.markDeleted(timestamp);
                    insertVersion(connection, deleted, serialized, editor, comment);
                    recordAudit(connection, "delete", record.key(), editor, comment, timestamp);
                }
                connection.commit();
                return updated > 0;
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(previous);
            }
        }
    }

    private boolean markDeletedMongo(ItemRecord record, long timestamp, String serialized, String editor, String comment) throws SQLException {
        try {
            ItemRecord deleted = record.markDeleted(timestamp);
            ReplaceOptions options = new ReplaceOptions().upsert(false);
            UpdateResult result = mongoItems.replaceOne(Filters.eq("_id", record.key()),
                    buildMongoItemDocument(deleted, serialized), options);
            if (result.getMatchedCount() == 0) {
                return false;
            }
            int version = nextMongoVersion(record.key());
            mongoVersions.insertOne(buildMongoVersionDocument(record.key(), version, serialized, editor, comment, timestamp, true));
            mongoAudit.insertOne(buildMongoAuditDocument("delete", record.key(), editor, comment, timestamp));
            return true;
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    private List<ItemRecord> loadAllMongo() throws SQLException {
        try {
            List<ItemRecord> out = new ArrayList<>();
            for (Document doc : mongoItems.find(Filters.eq("is_deleted", false))) {
                out.add(mapRecord(doc));
            }
            return out;
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    private List<ItemRecord> fetchMongoChanges(long since) throws SQLException {
        try {
            List<ItemRecord> out = new ArrayList<>();
            for (Document doc : mongoItems.find(Filters.gt("updated_at", since))) {
                out.add(mapRecord(doc));
            }
            return out;
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    private List<ItemVersion> fetchMongoHistory(String key, int limit) throws SQLException {
        try {
            List<ItemVersion> versions = new ArrayList<>();
            var iterable = mongoVersions.find(Filters.eq("item_name", key)).sort(Sorts.descending("version"));
            if (limit > 0) {
                iterable = iterable.limit(limit);
            }
            for (Document doc : iterable) {
                versions.add(mapVersion(doc));
            }
            return versions;
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    private Optional<ItemVersion> fetchMongoVersion(String key, int version) throws SQLException {
        try {
            Document doc = mongoVersions.find(Filters.and(Filters.eq("item_name", key), Filters.eq("version", version))).first();
            if (doc == null) {
                return Optional.empty();
            }
            return Optional.of(mapVersion(doc));
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    private List<ItemRecord> searchMongo(String query, Integer customModelData, int limit) throws SQLException {
        try {
            Pattern pattern = Pattern.compile(Pattern.quote(query), Pattern.CASE_INSENSITIVE);
            List<Bson> predicates = new ArrayList<>();
            predicates.add(Filters.regex("name", pattern));
            predicates.add(Filters.regex("display_name", pattern));
            predicates.add(Filters.regex("lore_text", pattern));

            Bson filter = Filters.and(Filters.eq("is_deleted", false), Filters.or(predicates));
            if (customModelData != null) {
                filter = Filters.and(filter, Filters.eq("custom_model_data", customModelData));
            }

            var iterable = mongoItems.find(filter).sort(Sorts.descending("updated_at"));
            if (limit > 0) {
                iterable = iterable.limit(limit);
            }
            List<ItemRecord> results = new ArrayList<>();
            for (Document doc : iterable) {
                results.add(mapRecord(doc));
            }
            return results;
        } catch (MongoException ex) {
            throw new SQLException("MongoDB operation failed", ex);
        }
    }

    public List<ItemRecord> loadAllItems() throws SQLException {
        if (type == DatabaseType.MONGODB) {
            return loadAllMongo();
        }
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
        if (type == DatabaseType.MONGODB) {
            return fetchMongoChanges(since);
        }
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

    public List<ItemVersion> fetchHistory(String key, int limit) throws SQLException {
        if (type == DatabaseType.MONGODB) {
            return fetchMongoHistory(key, limit);
        }
        StringBuilder sql = new StringBuilder("SELECT id,item_name,version,editor,nbt,created_at,comment,is_deleted FROM `").append(versionsTable).append("` WHERE item_name = ? ORDER BY version DESC");
        if (limit > 0) {
            sql.append(" LIMIT ?");
        }

        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            ps.setString(1, key);
            if (limit > 0) {
                ps.setInt(2, limit);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<ItemVersion> versions = new ArrayList<>();
                while (rs.next()) {
                    versions.add(mapVersion(rs));
                }
                return versions;
            }
        }
    }

    public Optional<ItemVersion> fetchVersion(String key, int version) throws SQLException {
        if (type == DatabaseType.MONGODB) {
            return fetchMongoVersion(key, version);
        }
        String sql = "SELECT id,item_name,version,editor,nbt,created_at,comment,is_deleted FROM `" + versionsTable + "` WHERE item_name = ? AND version = ?";
        try (Connection connection = getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setInt(2, version);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapVersion(rs));
                }
                return Optional.empty();
            }
        }
    }

    public List<ItemRecord> search(String query, Integer customModelData, int limit) throws SQLException {
        if (type == DatabaseType.MONGODB) {
            return searchMongo(query, customModelData, limit);
        }
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

    private void insertVersion(Connection connection, ItemRecord record, String serialized, String editor, String comment) throws SQLException {
        int nextVersion = 1;
        String select = "SELECT COALESCE(MAX(version), 0) FROM `" + versionsTable + "` WHERE item_name = ?";
        try (PreparedStatement ps = connection.prepareStatement(select)) {
            ps.setString(1, record.key());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    nextVersion = rs.getInt(1) + 1;
                }
            }
        }

        String insert = "INSERT INTO `" + versionsTable + "` (item_name,version,editor,nbt,created_at,comment,is_deleted) VALUES (?,?,?,?,?,?,?)";
        try (PreparedStatement ps = connection.prepareStatement(insert)) {
            ps.setString(1, record.key());
            ps.setInt(2, nextVersion);
            if (editor == null) {
                ps.setNull(3, Types.VARCHAR);
            } else {
                ps.setString(3, editor);
            }
            ps.setString(4, serialized);
            ps.setLong(5, record.updatedAt());
            if (comment == null) {
                ps.setNull(6, Types.VARCHAR);
            } else {
                ps.setString(6, comment);
            }
            ps.setBoolean(7, record.deleted());
            ps.executeUpdate();
        }
    }

    private void recordAudit(Connection connection, String action, String itemName, String editor, String details, long timestamp) throws SQLException {
        if (action == null || action.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO `" + auditTable + "` (action,item_name,actor,details,created_at) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, action);
            if (itemName == null) {
                ps.setNull(2, Types.VARCHAR);
            } else {
                ps.setString(2, itemName);
            }
            if (editor == null) {
                ps.setNull(3, Types.VARCHAR);
            } else {
                ps.setString(3, editor);
            }
            if (details == null) {
                ps.setNull(4, Types.VARCHAR);
            } else {
                ps.setString(4, details);
            }
            ps.setLong(5, timestamp);
            ps.executeUpdate();
        }
    }

    public void recordAudit(String action, String itemName, String editor, String details, long timestamp) throws SQLException {
        if (type == DatabaseType.MONGODB) {
            try {
                mongoAudit.insertOne(buildMongoAuditDocument(action, itemName, editor, details, timestamp));
            } catch (MongoException ex) {
                throw new SQLException("MongoDB operation failed", ex);
            }
            return;
        }
        try (Connection connection = getConnection()) {
            recordAudit(connection, action, itemName, editor, details, timestamp);
        }
    }

    private ItemVersion mapVersion(ResultSet rs) throws SQLException {
        long id = rs.getLong("id");
        String itemName = rs.getString("item_name");
        int version = rs.getInt("version");
        String editor = rs.getString("editor");
        String nbt = rs.getString("nbt");
        long createdAt = rs.getLong("created_at");
        String comment = rs.getString("comment");
        boolean deleted = rs.getBoolean("is_deleted");
        return new ItemVersion(id, itemName, version, editor, nbt, createdAt, comment, deleted);
    }

    private ItemVersion mapVersion(Document doc) {
        Number idNumber = (Number) doc.getOrDefault("id", 0L);
        long id = idNumber == null ? 0L : idNumber.longValue();
        String itemName = doc.getString("item_name");
        int version = doc.getInteger("version", 0);
        String editor = doc.getString("editor");
        String nbt = doc.getString("nbt");
        Number createdNumber = (Number) doc.getOrDefault("created_at", 0L);
        long createdAt = createdNumber == null ? 0L : createdNumber.longValue();
        String comment = doc.getString("comment");
        boolean deleted = Boolean.TRUE.equals(doc.getBoolean("is_deleted"));
        return new ItemVersion(id, itemName, version, editor, nbt, createdAt, comment, deleted);
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

    private ItemRecord mapRecord(Document doc) throws SQLException {
        String key = doc.getString("name");
        String itemData = doc.getString("item");
        if (itemData == null) {
            throw new SQLException("Missing serialized item data for key " + key);
        }
        ItemStack item;
        try {
            item = ItemSerializer.deserialize(itemData);
        } catch (Exception ex) {
            throw new SQLException("Failed to deserialize item for key " + key, ex);
        }
        String display = doc.getString("display_name");
        List<String> lore = doc.getList("lore", String.class);
        if (lore == null) {
            lore = List.of();
        }
        Integer cmd = null;
        if (doc.containsKey("custom_model_data")) {
            Number number = (Number) doc.get("custom_model_data");
            if (number != null) {
                cmd = number.intValue();
            }
        }
        Map<String, Integer> enchantments = documentToEnchantments(doc.get("enchantments", Document.class));
        Number updated = (Number) doc.getOrDefault("updated_at", 0L);
        long updatedAt = updated == null ? 0L : updated.longValue();
        boolean deleted = Boolean.TRUE.equals(doc.getBoolean("is_deleted"));
        return new ItemRecord(key, item, display, lore, cmd, enchantments, updatedAt, deleted);
    }

    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (mongoClient != null) {
            mongoClient.close();
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

    private Document buildMongoItemDocument(ItemRecord record, String serialized) {
        Document doc = new Document("_id", record.key())
                .append("name", record.key())
                .append("item", serialized)
                .append("display_name", record.displayName())
                .append("lore", record.lore())
                .append("lore_text", loreToColumn(record.lore()))
                .append("custom_model_data", record.customModelData())
                .append("enchantments", enchantmentsToDocument(record.enchantments()))
                .append("updated_at", record.updatedAt())
                .append("is_deleted", record.deleted());
        return doc;
    }

    private Document buildMongoVersionDocument(String key, int version, String serialized, String editor, String comment, long createdAt, boolean deleted) {
        return new Document("item_name", key)
                .append("version", version)
                .append("editor", editor)
                .append("nbt", serialized)
                .append("created_at", createdAt)
                .append("comment", comment)
                .append("is_deleted", deleted)
                .append("id", createdAt);
    }

    private Document buildMongoAuditDocument(String action, String itemName, String editor, String details, long timestamp) {
        return new Document("action", action)
                .append("item_name", itemName)
                .append("actor", editor)
                .append("details", details)
                .append("created_at", timestamp);
    }

    private int nextMongoVersion(String key) {
        Document doc = mongoVersions.find(Filters.eq("item_name", key)).sort(Sorts.descending("version")).first();
        if (doc == null) {
            return 1;
        }
        return doc.getInteger("version", 0) + 1;
    }

    private Document enchantmentsToDocument(Map<String, Integer> enchantments) {
        if (enchantments == null || enchantments.isEmpty()) {
            return null;
        }
        Document doc = new Document();
        enchantments.forEach(doc::append);
        return doc;
    }

    private Map<String, Integer> documentToEnchantments(Document doc) {
        if (doc == null || doc.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> map = new HashMap<>();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Number number) {
                map.put(entry.getKey(), number.intValue());
            }
        }
        return map;
    }
}
