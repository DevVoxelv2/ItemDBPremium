package net.devvoxel.itemDB.data;

import org.bukkit.NamespacedKey;
import org.bukkit.enchantments.Enchantment;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public record ItemRecord(
        String key,
        ItemStack item,
        String displayName,
        List<String> lore,
        Integer customModelData,
        Map<String, Integer> enchantments,
        long updatedAt,
        boolean deleted
) {

    public ItemRecord {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(item, "item");
        key = key.toLowerCase(Locale.ROOT);
        item = item.clone();
        lore = lore == null ? List.of() : List.copyOf(lore);
        enchantments = enchantments == null ? Map.of() : Map.copyOf(enchantments);
    }

    public ItemRecord withItem(ItemStack newItem, long timestamp) {
        ItemMeta meta = newItem.getItemMeta();
        String newDisplay = meta != null && meta.hasDisplayName() ? meta.getDisplayName() : null;
        List<String> newLore = meta != null && meta.hasLore() ? meta.getLore() : List.of();
        Integer newCmd = meta != null && meta.hasCustomModelData() ? meta.getCustomModelData() : null;
        Map<String, Integer> newEnchants = meta != null && !meta.getEnchants().isEmpty()
                ? meta.getEnchants().entrySet().stream()
                .collect(Collectors.toMap(e -> namespacedKey(e.getKey()), Map.Entry::getValue))
                : Map.of();
        return new ItemRecord(key, newItem, newDisplay, newLore, newCmd, newEnchants, timestamp, false);
    }

    private static String namespacedKey(Enchantment enchantment) {
        NamespacedKey key = enchantment.getKey();
        return key.namespace() + ":" + key.getKey();
    }

    public ItemRecord markDeleted(long timestamp) {
        return new ItemRecord(key, item, displayName, lore, customModelData, enchantments, timestamp, true);
    }

    public ItemMeta meta() {
        ItemMeta meta = item.getItemMeta();
        if (meta == null) {
            return null;
        }
        return meta.clone();
    }

    public List<String> loreUnmodifiable() {
        return Collections.unmodifiableList(lore);
    }

    public Map<String, Integer> enchantmentsUnmodifiable() {
        return Collections.unmodifiableMap(enchantments);
    }

    public static ItemRecord fromStack(String key, ItemStack stack, long timestamp, boolean deleted) {
        ItemStack clone = stack.clone();
        ItemMeta meta = clone.getItemMeta();

        String display = null;
        List<String> lore = List.of();
        Integer customModelData = null;
        Map<String, Integer> enchantments = Map.of();

        if (meta != null) {
            if (meta.hasDisplayName()) {
                display = meta.getDisplayName();
            }
            if (meta.hasLore()) {
                lore = new ArrayList<>(meta.getLore());
            }
            if (meta.hasCustomModelData()) {
                customModelData = meta.getCustomModelData();
            }
            if (!meta.getEnchants().isEmpty()) {
                Map<String, Integer> map = new HashMap<>();
                meta.getEnchants().forEach((enchantment, level) -> map.put(namespacedKey(enchantment), level));
                enchantments = map;
            }
        }

        return new ItemRecord(key, clone, display, lore, customModelData, enchantments, timestamp, deleted);
    }
}
