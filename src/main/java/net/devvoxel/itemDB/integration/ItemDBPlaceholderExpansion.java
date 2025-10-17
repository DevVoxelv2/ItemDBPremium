package net.devvoxel.itemDB.integration;

import me.clip.placeholderapi.expansion.PlaceholderExpansion;
import net.devvoxel.itemDB.ItemDB;
import org.bukkit.OfflinePlayer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class ItemDBPlaceholderExpansion extends PlaceholderExpansion {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
            .withZone(ZoneId.systemDefault());

    private final ItemDB plugin;

    public ItemDBPlaceholderExpansion(ItemDB plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getIdentifier() {
        return "itemdb";
    }

    @Override
    public String getAuthor() {
        return String.join(", ", plugin.getDescription().getAuthors());
    }

    @Override
    public String getVersion() {
        return plugin.getDescription().getVersion();
    }

    @Override
    public boolean persist() {
        return true;
    }

    @Override
    public boolean canRegister() {
        return true;
    }

    @Override
    public String onRequest(OfflinePlayer player, String params) {
        if (params == null) {
            return "";
        }
        String identifier = params.toLowerCase(Locale.ROOT);
        return switch (identifier) {
            case "total", "count", "items" -> String.valueOf(plugin.items().size());
            case "database_type" -> plugin.db().type().name().toLowerCase(Locale.ROOT);
            case "last_sync" -> formatLastSync();
            default -> {
                if (identifier.startsWith("exists_")) {
                    String item = identifier.substring("exists_".length());
                    yield plugin.items().record(item).isPresent() ? "true" : "false";
                }
                yield "";
            }
        };
    }

    private String formatLastSync() {
        long lastSync = plugin.items().lastSync();
        if (lastSync <= 0) {
            return "-";
        }
        return FORMATTER.format(Instant.ofEpochMilli(lastSync));
    }
}
