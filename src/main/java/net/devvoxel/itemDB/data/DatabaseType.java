package net.devvoxel.itemDB.data;

public enum DatabaseType {
    MYSQL,
    SQLITE,
    MONGODB;

    public static DatabaseType fromConfig(String raw) {
        if (raw == null) {
            return MYSQL;
        }
        return switch (raw.toLowerCase()) {
            case "sqlite", "sqlite3" -> SQLITE;
            case "mongo", "mongodb", "mongo-db" -> MONGODB;
            default -> MYSQL;
        };
    }
}
