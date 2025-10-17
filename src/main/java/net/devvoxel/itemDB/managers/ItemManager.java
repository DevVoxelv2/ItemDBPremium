package net.devvoxel.itemDB.managers;

import net.devvoxel.itemDB.ItemDB;
import net.devvoxel.itemDB.data.Database;
import net.devvoxel.itemDB.data.ItemRecord;
import net.devvoxel.itemDB.data.ItemSerializer;
import net.devvoxel.itemDB.data.ItemVersion;
import net.devvoxel.itemDB.integration.ExternalItemProvider;
import net.devvoxel.itemDB.webhook.WebhookNotifier;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ItemManager {
    private final ItemDB plugin;
    private final Database db;
    private final WebhookNotifier webhooks;
    private final ExternalItemProvider externalItems;
    private final ConcurrentMap<String, ItemRecord> cache = new ConcurrentHashMap<>();
    private volatile long lastSync = 0L;

    public ItemManager(ItemDB plugin, Database db, WebhookNotifier webhooks, ExternalItemProvider externalItems) {
        this.plugin = plugin;
        this.db = db;
        this.webhooks = webhooks;
        this.externalItems = externalItems;
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
            webhooks.notifyError("load", "Fehler beim Laden der Items", ex);
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
            webhooks.notifyError("sync", "Konnte Änderungen nicht synchronisieren", ex);
        }
    }

    public boolean add(String name, ItemStack stack, String editor) {
        return add(name, stack, editor, null);
    }

    public boolean add(String name, ItemStack stack, String editor, String comment) {
        String key = normalize(name);
        if (cache.containsKey(key)) {
            return false;
        }
        return replaceInternal(key, stack, editor, comment != null ? comment : "Added item");
    }

    public boolean replace(String name, ItemStack stack, String editor, String comment) {
        return replaceInternal(normalize(name), stack, editor, comment);
    }

    private boolean replaceInternal(String key, ItemStack stack, String editor, String comment) {
        ItemRecord record = ItemRecord.fromStack(key, stack, db.now(), false);
        String appliedComment = comment != null ? comment : "Updated item";
        try {
            db.saveItem(record, editor, appliedComment);
            cache.put(key, record);
            lastSync = Math.max(lastSync, record.updatedAt());
            webhooks.notifyChange("save", key, editor, appliedComment);
            return true;
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Speichern des Items '" + key + "': " + ex.getMessage());
            webhooks.notifyError("save", "Fehler beim Speichern des Items '" + key + "'", ex);
            return false;
        }
    }

    public boolean remove(String name, String editor) {
        String key = normalize(name);
        ItemRecord current = cache.get(key);
        if (current == null) {
            return false;
        }

        long timestamp = db.now();
        try {
            if (db.markDeleted(current, timestamp, editor, "Deleted item")) {
                cache.remove(key);
                lastSync = Math.max(lastSync, timestamp);
                webhooks.notifyChange("delete", key, editor, "Deleted item");
                return true;
            }
        } catch (SQLException ex) {
            plugin.getLogger().severe("Fehler beim Löschen des Items '" + name + "': " + ex.getMessage());
            webhooks.notifyError("delete", "Fehler beim Löschen des Items '" + name + "'", ex);
        }
        return false;
    }

    public ItemStack get(String name) {
        ItemRecord record = cache.get(normalize(name));
        if (record == null) {
            return externalItems.resolve(name).orElse(null);
        }
        return record.item().clone();
    }

    public Optional<ItemRecord> record(String name) {
        return Optional.ofNullable(cache.get(normalize(name)));
    }

    public Optional<ItemRecord> info(String name) {
        return record(name);
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

    public long lastSync() {
        return lastSync;
    }

    public boolean updateItem(String name, Function<ItemStack, ItemStack> mutator, String editor, String comment) {
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

        return replaceInternal(key, mutated, editor, comment);
    }

    public boolean updateMeta(String name, Consumer<ItemMeta> consumer, String editor, String comment) {
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
        }, editor, comment);
    }

    public boolean setCustomModelData(String name, Integer value, String editor) {
        String comment = value == null ? "Cleared CustomModelData" : "Set CustomModelData to " + value;
        return updateMeta(name, meta -> meta.setCustomModelData(value), editor, comment);
    }

    public boolean setDisplayName(String name, String displayName, String editor) {
        String comment = "Updated display name";
        return updateMeta(name, meta -> meta.setDisplayName(ChatColor.translateAlternateColorCodes('&', displayName)), editor, comment);
    }

    public boolean clearDisplayName(String name, String editor) {
        return updateMeta(name, meta -> meta.setDisplayName(null), editor, "Cleared display name");
    }

    public boolean addLoreLine(String name, String line, String editor) {
        return updateMeta(name, meta -> {
            List<String> lore = meta.hasLore() ? new ArrayList<>(meta.getLore()) : new ArrayList<>();
            lore.add(ChatColor.translateAlternateColorCodes('&', line));
            meta.setLore(lore);
        }, editor, "Added lore line");
    }

    public boolean setLoreLine(String name, int index, String line, String editor) {
        return updateMeta(name, meta -> {
            List<String> lore = meta.hasLore() ? new ArrayList<>(meta.getLore()) : new ArrayList<>();
            while (lore.size() <= index) {
                lore.add("");
            }
            lore.set(index, ChatColor.translateAlternateColorCodes('&', line));
            meta.setLore(lore);
        }, editor, "Updated lore line " + (index + 1));
    }

    public boolean clearLore(String name, String editor) {
        return updateMeta(name, meta -> meta.setLore(null), editor, "Cleared lore");
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

    public List<ItemVersion> history(String name, int limit) {
        try {
            return db.fetchHistory(normalize(name), limit);
        } catch (SQLException ex) {
            plugin.getLogger().warning("Konnte History nicht laden: " + ex.getMessage());
            return List.of();
        }
    }

    public Optional<ItemVersion> version(String name, int version) {
        try {
            return db.fetchVersion(normalize(name), version);
        } catch (SQLException ex) {
            plugin.getLogger().warning("Konnte Version nicht laden: " + ex.getMessage());
            return Optional.empty();
        }
    }

    public List<String> diff(String name, int versionA, int versionB) {
        String key = normalize(name);
        try {
            Optional<ItemVersion> first = db.fetchVersion(key, versionA);
            Optional<ItemVersion> second = db.fetchVersion(key, versionB);
            if (first.isEmpty() || second.isEmpty()) {
                return List.of();
            }
            ItemStack left = ItemSerializer.deserialize(first.get().nbt());
            ItemStack right = ItemSerializer.deserialize(second.get().nbt());
            Map<String, String> leftMap = flattenItem(left);
            Map<String, String> rightMap = flattenItem(right);

            Set<String> keys = new HashSet<>();
            keys.addAll(leftMap.keySet());
            keys.addAll(rightMap.keySet());
            List<String> sortedKeys = new ArrayList<>(keys);
            sortedKeys.sort(String::compareToIgnoreCase);

            List<String> diff = new ArrayList<>();
            for (String entry : sortedKeys) {
                String leftValue = leftMap.get(entry);
                String rightValue = rightMap.get(entry);
                if (leftValue == null) {
                    diff.add("+ " + entry + ": " + rightValue);
                } else if (rightValue == null) {
                    diff.add("- " + entry + ": " + leftValue);
                } else if (!leftValue.equals(rightValue)) {
                    diff.add("~ " + entry + ": " + leftValue + " -> " + rightValue);
                }
            }
            return diff;
        } catch (SQLException | IOException | ClassNotFoundException ex) {
            plugin.getLogger().warning("Fehler beim Erstellen des Diffs: " + ex.getMessage());
            webhooks.notifyError("diff", "Fehler beim Erstellen des Diffs", ex);
            return List.of();
        }
    }

    public boolean rollback(String name, int version, String editor) {
        String key = normalize(name);
        try {
            Optional<ItemVersion> target = db.fetchVersion(key, version);
            if (target.isEmpty()) {
                return false;
            }
            ItemStack stack = ItemSerializer.deserialize(target.get().nbt());
            return replaceInternal(key, stack, editor, "Rollback to version " + version);
        } catch (SQLException | IOException | ClassNotFoundException ex) {
            plugin.getLogger().severe("Rollback fehlgeschlagen: " + ex.getMessage());
            webhooks.notifyError("rollback", "Rollback fehlgeschlagen für '" + name + "'", ex);
            return false;
        }
    }

    public ImportReport importFromZip(Path file, String namespace, boolean dryRun, String editor) {
        int total = 0;
        int created = 0;
        int updated = 0;
        List<String> errors = new ArrayList<>();

        if (!Files.exists(file)) {
            errors.add("File not found: " + file);
            return new ImportReport(total, created, updated, errors, dryRun);
        }

        try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(file))) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    zip.closeEntry();
                    continue;
                }
                if (!entry.getName().startsWith("items/") || !entry.getName().endsWith(".nbt")) {
                    zip.closeEntry();
                    continue;
                }
                total++;
                byte[] data = zip.readAllBytes();
                String serialized = new String(data, StandardCharsets.UTF_8);
                String derivedName = deriveNameFromEntry(entry.getName());
                String baseName = derivedName.contains(":") ? derivedName.substring(derivedName.indexOf(':') + 1) : derivedName;
                String finalName = namespace != null && !namespace.isEmpty() ? namespace + ":" + baseName : derivedName;
                String key = normalize(finalName);
                boolean exists = cache.containsKey(key);
                if (dryRun) {
                    if (exists) {
                        updated++;
                    } else {
                        created++;
                    }
                    zip.closeEntry();
                    continue;
                }
                try {
                    ItemStack stack = ItemSerializer.deserialize(serialized);
                    replaceInternal(key, stack, editor, "Imported from " + file.getFileName());
                    if (exists) {
                        updated++;
                    } else {
                        created++;
                    }
                } catch (IOException | ClassNotFoundException ex) {
                    errors.add("Failed to import " + finalName + ": " + ex.getMessage());
                }
                zip.closeEntry();
            }
            if (!dryRun) {
                db.recordAudit("import", null, editor, "Imported " + total + " items from " + file, db.now());
            }
        } catch (IOException | SQLException ex) {
            errors.add(ex.getMessage());
            webhooks.notifyError("import", "Import fehlgeschlagen", ex);
        }

        return new ImportReport(total, created, updated, List.copyOf(errors), dryRun);
    }

    public ExportReport exportToZip(Path file, String namespace, String editor) {
        int exported = 0;
        List<String> errors = new ArrayList<>();
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(file))) {
                List<ItemRecord> records = new ArrayList<>(cache.values());
                records.sort(Comparator.comparing(ItemRecord::key));
                for (ItemRecord record : records) {
                    if (namespace != null && !namespace.isEmpty()) {
                        String prefix = namespace.toLowerCase(Locale.ROOT) + ":";
                        if (!record.key().startsWith(prefix)) {
                            continue;
                        }
                    }
                    String entryName = buildEntryName(record.key());
                    zip.putNextEntry(new ZipEntry("items/" + entryName + ".nbt"));
                    try {
                        String serialized = ItemSerializer.serialize(record.item());
                        zip.write(serialized.getBytes(StandardCharsets.UTF_8));
                        exported++;
                    } catch (IOException ex) {
                        errors.add("Failed to export " + record.key() + ": " + ex.getMessage());
                    } finally {
                        zip.closeEntry();
                    }
                }
            }
            db.recordAudit("export", null, editor, "Exported " + exported + " items to " + file, db.now());
        } catch (IOException | SQLException ex) {
            errors.add(ex.getMessage());
            webhooks.notifyError("export", "Export fehlgeschlagen", ex);
        }
        return new ExportReport(exported, List.copyOf(errors));
    }

    private Map<String, String> flattenItem(ItemStack stack) {
        Map<String, Object> serialized = stack.serialize();
        Map<String, String> out = new HashMap<>();
        serialized.forEach((key, value) -> flatten("root." + key, value, out));
        return out;
    }

    private void flatten(String path, Object value, Map<String, String> out) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                flatten(path + "." + key, entry.getValue(), out);
            }
            return;
        }
        if (value instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                flatten(path + "[" + i + "]", list.get(i), out);
            }
            return;
        }
        out.put(path, String.valueOf(value));
    }

    private String deriveNameFromEntry(String entry) {
        String trimmed = entry.substring("items/".length(), entry.length() - 4);
        return trimmed.replace('/', ':');
    }

    private String buildEntryName(String key) {
        int colon = key.indexOf(':');
        if (colon == -1) {
            return key.replace(':', '_');
        }
        String namespace = key.substring(0, colon);
        String name = key.substring(colon + 1);
        return namespace + "/" + name.replace(':', '_');
    }

    public record ImportReport(int total, int created, int updated, List<String> errors, boolean dryRun) {
        public boolean hasErrors() {
            return !errors.isEmpty();
        }
    }

    public record ExportReport(int exported, List<String> errors) {
        public boolean hasErrors() {
            return !errors.isEmpty();
        }
    }
}
