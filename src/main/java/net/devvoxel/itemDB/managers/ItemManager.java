package net.devvoxel.itemDB.managers;

import net.devvoxel.itemDB.ItemDB;
import net.devvoxel.itemDB.data.Database;
import net.devvoxel.itemDB.data.ItemRecord;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class ItemManager {
    private final ItemDB plugin;
    private final Database db;
    private final ConcurrentMap<String, ItemRecord> cache = new ConcurrentHashMap<>();
    private volatile long lastSync = 0L;

    public ItemManager(ItemDB plugin, Database db) {
        this.plugin = plugin;
        this.db = db;
        load(true);
    }

    public void load(boolean logResult) {
        try {
            List<ItemRecord> all = db.loadAllItems();
            cache.clear();
            long maxTimestamp = 0;
            for (ItemRecord record : all) {
                cache.put(record.key(), record);
                maxTimestamp = Math.max(maxTimestamp, record.updatedAt());
            }
            lastSync = maxTimestamp;
            if (logResult) {
                plugin.getLogger().info("Geladene Items aus DB: " + cache.size());
            }
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Laden der Items: " + ex.getMessage());
        }
    }

    public void sync() {
        long since = lastSync;
        try {
            List<ItemRecord> changes = db.fetchChanges(since);
            long maxTimestamp = since;
            for (ItemRecord change : changes) {
                maxTimestamp = Math.max(maxTimestamp, change.updatedAt());
                if (change.deleted()) {
                    cache.remove(change.key());
                } else {
                    cache.put(change.key(), change);
                }
            }
            lastSync = maxTimestamp;
        } catch (SQLException ex) {
            plugin.getLogger().warning("Konnte Änderungen nicht synchronisieren: " + ex.getMessage());
        }
    }

    public boolean add(String name, ItemStack stack) {
        String key = normalize(name);
        if (cache.containsKey(key)) {
            return false;
        }

        ItemRecord record = buildRecord(key, stack, db.now());
        try {
            db.saveItem(record);
            cache.put(key, record);
            lastSync = Math.max(lastSync, record.updatedAt());
            return true;
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Speichern des Items '" + name + "': " + ex.getMessage());
            return false;
        }
    }

    public boolean remove(String name) {
        String key = normalize(name);
        if (!cache.containsKey(key)) {
            return false;
        }

        long timestamp = db.now();
        try {
            if (db.markDeleted(key, timestamp)) {
                cache.remove(key);
                lastSync = Math.max(lastSync, timestamp);
                return true;
            }
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Löschen des Items '" + name + "': " + ex.getMessage());
        }
        return false;
    }

    public ItemStack get(String name) {
        ItemRecord record = cache.get(normalize(name));
        if (record == null) {
            return null;
        }
        return record.item().clone();
    }

    public Optional<ItemRecord> record(String name) {
        return Optional.ofNullable(cache.get(normalize(name)));
    }

    public List<String> keys() {
        List<String> keys = new ArrayList<>(cache.keySet());
        keys.sort(String.CASE_INSENSITIVE_ORDER);
        return keys;
    }

    public int size() {
        return cache.size();
    }

    public boolean exists(String name) {
        return cache.containsKey(normalize(name));
    }

    public boolean updateItem(String name, Function<ItemStack, ItemStack> mutator) {
        String key = normalize(name);
        ItemRecord current = cache.get(key);
        if (current == null) {
            return false;
        }

        ItemStack base = current.item().clone();
        ItemStack mutated = mutator.apply(base);
        if (mutated == null) {
            return false;
        }

        ItemRecord updated = buildRecord(key, mutated, db.now());
        try {
            db.saveItem(updated);
            cache.put(key, updated);
            lastSync = Math.max(lastSync, updated.updatedAt());
            return true;
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Aktualisieren des Items '" + name + "': " + ex.getMessage());
            return false;
        }
    }

    public boolean updateMeta(String name, Consumer<ItemMeta> consumer) {
        return updateItem(name, stack -> {
            ItemMeta meta = stack.getItemMeta();
            if (meta == null) {
                meta = Bukkit.getItemFactory().getItemMeta(stack.getType());
            }
            if (meta == null) {
                return null;
            }
            consumer.accept(meta);
            stack.setItemMeta(meta);
            return stack;
        });
    }

    public List<ItemRecord> search(String query, Integer customModelData, int limit) {
        try {
            return db.search(query, customModelData, limit);
        } catch (SQLException ex) {
            plugin.getLogger().warning("Fehler bei der Suche: " + ex.getMessage());
            return Collections.emptyList();
        }
    }

    public org.bukkit.scheduler.BukkitTask applySyncTask(long intervalTicks) {
        return Bukkit.getScheduler().runTaskTimerAsynchronously(plugin, this::sync, intervalTicks, intervalTicks);
    }

    public static String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private ItemRecord buildRecord(String key, ItemStack stack, long timestamp) {
        ItemStack clone = stack.clone();
        ItemMeta meta = clone.getItemMeta();
        String displayName = meta != null && meta.hasDisplayName() ? meta.getDisplayName() : null;
        List<String> lore = meta != null && meta.hasLore() ? meta.getLore() : List.of();
        Integer customModelData = meta != null && meta.hasCustomModelData() ? meta.getCustomModelData() : null;
        Map<String, Integer> enchantments = meta != null && !meta.getEnchants().isEmpty()
                ? meta.getEnchants().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(e -> {
                    var namespacedKey = e.getKey().getKey();
                    return namespacedKey.getNamespace() + ":" + namespacedKey.getKey();
                }, Map.Entry::getValue))
                : Map.of();

        return new ItemRecord(key, clone, displayName, lore, customModelData, enchantments, timestamp, false);
    }

    public boolean setCustomModelData(String name, Integer value) {
        return updateMeta(name, meta -> {
            if (value == null) {
                meta.setCustomModelData(null);
            } else {
                meta.setCustomModelData(value);
            }
        });
    }

    public boolean setDisplayName(String name, String displayName) {
        return updateMeta(name, meta -> meta.setDisplayName(ChatColor.translateAlternateColorCodes('&', displayName)));
    }

    public boolean clearDisplayName(String name) {
        return updateMeta(name, meta -> meta.setDisplayName(null));
    }

    public boolean addLoreLine(String name, String line) {
        return updateMeta(name, meta -> {
            List<String> lore = meta.hasLore() ? new ArrayList<>(meta.getLore()) : new ArrayList<>();
            lore.add(ChatColor.translateAlternateColorCodes('&', line));
            meta.setLore(lore);
        });
    }

    public boolean setLoreLine(String name, int index, String line) {
        return updateMeta(name, meta -> {
            List<String> lore = meta.hasLore() ? new ArrayList<>(meta.getLore()) : new ArrayList<>();
            while (lore.size() <= index) {
                lore.add("");
            }
            lore.set(index, ChatColor.translateAlternateColorCodes('&', line));
            meta.setLore(lore);
        });
    }

    public boolean clearLore(String name) {
        return updateMeta(name, meta -> meta.setLore(null));
    }

    public Optional<ItemRecord> info(String name) {
        return record(name);
    }
}
