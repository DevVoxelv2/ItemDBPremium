package net.devvoxel.itemDB.i18n;

import net.devvoxel.itemDB.ItemDB;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.File;
import java.util.List;

public class MessageManager {
    private final ItemDB plugin;
    private final File file;
    private FileConfiguration cfg;
    private String prefix;
    private String guiTitle;

    public MessageManager(ItemDB plugin) {
        this.plugin = plugin;
        this.file = new File(plugin.getDataFolder(), "messages.yml");
        reload();
    }

    public void reload() {
        cfg = YamlConfiguration.loadConfiguration(file);
        prefix = plugin.getConfig().getString("Prefix", "&8[&cItemDB&8]&7 ");
        guiTitle = plugin.getConfig().getString("Gui.Title", "§cOPSucht §7» §fMarktplatz");
        // inject dynamic values into messages.yml fields if referenced
        cfg.set("gui-title", cfg.getString("gui-title", "{gui_title}")
                .replace("{gui_title}", guiTitle));
    }

    public String get(String key) {
        String s = cfg.getString(key, key);
        return color(applyPrefix(s));
    }

    public List<String> getList(String key) {
        List<String> list = cfg.getStringList(key);
        return list.stream().map(this::applyPrefix).map(MessageManager::color).toList();
    }

    private String applyPrefix(String s) {
        return s.replace("{prefix}", prefix);
    }

    public String guiTitle() { return guiTitle; }

    public static String color(String s) {
        return s.replace('&', '§');
    }
}
