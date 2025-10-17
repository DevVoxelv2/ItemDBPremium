package net.devvoxel.itemDB.ui;

import net.devvoxel.itemDB.ItemDB;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.Sound;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.inventory.meta.ItemMeta;
import org.bukkit.inventory.meta.SkullMeta;

import java.util.List;

public class ItemsGui implements Listener {
    private static final int ROWS = 6;
    private static final int SIZE = ROWS * 9;
    private static final int ITEMS_PER_ROW = 7;
    private static final int BACK_SLOT = 45;
    private static final int CLOSE_SLOT = 49;
    private static final int NEXT_SLOT = 53;

    private final ItemDB plugin;

    public ItemsGui(ItemDB plugin) {
        this.plugin = plugin;
    }

    /**
     * Opens the items database GUI for a player.
     */
    public void open(Player player) {
        open(player, 0);
    }

    public void open(Player player, int page) {
        List<String> names = plugin.items().keys();
        int itemsPerPage = ITEMS_PER_ROW * (ROWS - 2);
        int totalPages = Math.max(1, (int) Math.ceil(names.size() / (double) itemsPerPage));
        int currentPage = Math.max(0, Math.min(page, totalPages - 1));

        GuiHolder holder = new GuiHolder(currentPage, totalPages);
        Inventory inv = Bukkit.createInventory(holder, SIZE, formatTitle(currentPage, totalPages));
        holder.setInventory(inv);

        fillStaticLayout(inv, player);

        int startIndex = currentPage * itemsPerPage;
        for (int index = 0; index < itemsPerPage; index++) {
            int nameIndex = startIndex + index;
            if (nameIndex >= names.size()) {
                break;
            }

            String name = names.get(nameIndex);
            ItemStack item = plugin.items().get(name);
            if (item == null) {
                continue;
            }

            ItemStack display = item.clone();
            ItemMeta meta = display.getItemMeta();
            if (meta != null) {
                meta.setDisplayName("§f" + name);
                meta.setLore(plugin.messages().getList("gui-lore")
                        .stream().map(s -> s.replace("{name}", name)).toList());
                display.setItemMeta(meta);
            }

            int slot = nextInnerSlot(index);
            if (slot >= SIZE) {
                break;
            }
            inv.setItem(slot, display);
        }

        if (currentPage > 0) {
            inv.setItem(BACK_SLOT, createNavigationItem(Material.ARROW, "gui-back", "§aBack"));
        }

        inv.setItem(CLOSE_SLOT, createNavigationItem(Material.BARRIER, "gui-close", "§cClose"));

        if (currentPage < totalPages - 1) {
            inv.setItem(NEXT_SLOT, createNavigationItem(Material.ARROW, "gui-next", "§aNext"));
        }

        player.openInventory(inv);
    }

    private void fillStaticLayout(Inventory inv, Player player) {
        ItemStack glass = new ItemStack(Material.GRAY_STAINED_GLASS_PANE);
        ItemMeta gm = glass.getItemMeta();
        if (gm != null) {
            gm.setDisplayName(" ");
            glass.setItemMeta(gm);
        }

        for (int slot = 0; slot < SIZE; slot++) {
            int row = slot / 9;
            int col = slot % 9;

            if (slot == 0) {
                continue; // player head gets placed later
            }

            if (row == 0 || row == ROWS - 1 || col == 0 || col == 8) {
                inv.setItem(slot, glass.clone());
            }
        }

        inv.setItem(0, createPlayerHead(player));
    }

    private ItemStack createPlayerHead(Player player) {
        ItemStack head = new ItemStack(Material.PLAYER_HEAD);
        ItemMeta meta = head.getItemMeta();
        if (meta instanceof SkullMeta skullMeta) {
            skullMeta.setOwningPlayer(player);
            skullMeta.setDisplayName("§e" + player.getName());
            head.setItemMeta(skullMeta);
        }
        return head;
    }

    private ItemStack createNavigationItem(Material material, String messageKey, String fallback) {
        ItemStack item = new ItemStack(material);
        ItemMeta meta = item.getItemMeta();
        if (meta != null) {
            meta.setDisplayName(messageOrFallback(messageKey, fallback));
            item.setItemMeta(meta);
        }
        return item;
    }

    private String messageOrFallback(String key, String fallback) {
        String value = plugin.messages().get(key);
        if (value.equals(key)) {
            return fallback;
        }
        return value;
    }

    private String formatTitle(int page, int totalPages) {
        String title = plugin.messages().guiTitle();
        if (totalPages > 1) {
            title += " §8(§7Page " + (page + 1) + "§8/§7" + totalPages + "§8)";
        }
        return title;
    }

    private int nextInnerSlot(int index) {
        int innerCols = ITEMS_PER_ROW;
        int row = index / innerCols;
        int col = index % innerCols;
        return (row + 1) * 9 + (col + 1);
    }

    private boolean isItemSlot(int slot) {
        int row = slot / 9;
        int col = slot % 9;
        return row > 0 && row < ROWS - 1 && col > 0 && col < 8;
    }

    @EventHandler
    public void onClick(InventoryClickEvent e) {
        Inventory top = e.getView().getTopInventory();
        if (!(top.getHolder() instanceof GuiHolder holder)) return;

        e.setCancelled(true);

        if (!(e.getWhoClicked() instanceof Player p)) return;
        if (e.getClickedInventory() == null || !e.getClickedInventory().equals(top)) return;

        int slot = e.getSlot();

        if (slot == BACK_SLOT && holder.page > 0) {
            open(p, holder.page - 1);
            return;
        }

        if (slot == CLOSE_SLOT) {
            p.closeInventory();
            return;
        }

        if (slot == NEXT_SLOT && holder.page < holder.totalPages - 1) {
            open(p, holder.page + 1);
            return;
        }

        if (!isItemSlot(slot)) return;

        ItemStack clicked = e.getCurrentItem();
        if (clicked == null || clicked.getType() == Material.AIR) return;

        ItemMeta meta = clicked.getItemMeta();
        String name = meta != null ? meta.getDisplayName() : null;
        if (name == null || name.isBlank()) return;
        name = name.replace("§f", "").trim();

        ItemStack dbItem = plugin.items().get(name);
        if (dbItem == null) {
            p.sendMessage(plugin.messages().get("item-not-found").replace("{name}", name));
            return;
        }

        p.getInventory().addItem(dbItem.clone());
        p.playSound(p.getLocation(), Sound.ENTITY_ITEM_PICKUP, 0.8f, 1.2f);
        p.sendMessage(plugin.messages().get("item-given-self").replace("{name}", name));
    }

    private static class GuiHolder implements InventoryHolder {
        private final int page;
        private final int totalPages;
        private Inventory inventory;

        GuiHolder(int page, int totalPages) {
            this.page = page;
            this.totalPages = totalPages;
        }

        @Override
        public Inventory getInventory() {
            return inventory;
        }

        void setInventory(Inventory inventory) {
            this.inventory = inventory;
        }
    }
}
