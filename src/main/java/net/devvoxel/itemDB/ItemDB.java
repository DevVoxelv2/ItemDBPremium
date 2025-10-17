package net.devvoxel.itemDB;

import net.devvoxel.itemDB.command.DbCommand;
import net.devvoxel.itemDB.data.Database;
import net.devvoxel.itemDB.integration.ExternalItemProvider;
import net.devvoxel.itemDB.integration.ItemDBPlaceholderExpansion;
import net.devvoxel.itemDB.managers.ItemManager;
import net.devvoxel.itemDB.i18n.MessageManager;
import net.devvoxel.itemDB.ui.ItemsGui;
import net.devvoxel.itemDB.webhook.WebhookNotifier;
import org.bstats.bukkit.Metrics;
import org.bstats.charts.SingleLineChart;
import org.bstats.charts.SimplePie;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

public class ItemDB extends JavaPlugin {

    private static ItemDB instance;

    private Database database;
    private ItemManager itemManager;
    private MessageManager messageManager;
    private ItemsGui itemsGui;
    private WebhookNotifier webhookNotifier;
    private ExternalItemProvider externalItemProvider;
    private ItemDBPlaceholderExpansion placeholderExpansion;
    private BukkitTask syncTask;

    public static ItemDB get() {
        return instance;
    }

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        saveResource("messages.yml", false);

        try {
            // MySQL-Verbindung herstellen
            this.database = new Database(this);
            database.connect();

            // Manager laden
            this.messageManager = new MessageManager(this);
            this.webhookNotifier = new WebhookNotifier(this);
            this.externalItemProvider = new ExternalItemProvider(this);
            this.itemManager = new ItemManager(this, database, webhookNotifier, externalItemProvider);
            this.itemsGui = new ItemsGui(this);

            if (externalItemProvider.hasAnyIntegration()) {
                getLogger().info("Aktive Item-Integrationen: " + externalItemProvider.describeSources());
            }

            if (Bukkit.getPluginManager().isPluginEnabled("PlaceholderAPI")) {
                this.placeholderExpansion = new ItemDBPlaceholderExpansion(this);
                this.placeholderExpansion.register();
                getLogger().info("PlaceholderAPI-Unterstützung aktiviert.");
            }

            // Command registrieren
            var dbCmd = new DbCommand(this);
            getCommand("db").setExecutor(dbCmd);
            getCommand("db").setTabCompleter(dbCmd);

            // Listener registrieren
            Bukkit.getPluginManager().registerEvents(itemsGui, this);

            // === bStats Metrics ===
            int pluginId = 27408; // deine Plugin-ID von bStats
            Metrics metrics = new Metrics(this, pluginId);

            // Custom Chart: Anzahl gespeicherter Items
            metrics.addCustomChart(new SingleLineChart("stored_items", () -> itemManager.size()));

            // Custom Chart: DB-Typ (mysql/sqlite)
            metrics.addCustomChart(new SimplePie("database_type", () ->
                    getConfig().getString("Database.Type", "mysql").toLowerCase())
            );

            long interval = Math.max(20L, getConfig().getLong("Database.SyncIntervalTicks", 100L));
            this.syncTask = itemManager.applySyncTask(interval);

            getLogger().info("ItemDB erfolgreich aktiviert.");
            getLogger().info("Geladene Items aus Datenbank: " + itemManager.size());
        } catch (Exception e) {
            getLogger().severe("Konnte nicht mit der Datenbank verbinden: " + e.getMessage());
            getServer().getPluginManager().disablePlugin(this);
        }
    }

    @Override
    public void onDisable() {
        if (syncTask != null) {
            syncTask.cancel();
            syncTask = null;
        }
        if (placeholderExpansion != null) {
            placeholderExpansion.unregister();
            placeholderExpansion = null;
        }
        if (database != null) {
            database.close();
        }
        getLogger().info("ItemDB deaktiviert.");
    }

    // === Getter ===
    public ItemManager items() {
        return itemManager;
    }

    public MessageManager messages() {
        return messageManager;
    }

    public ItemsGui gui() {
        return itemsGui;
    }

    public Database db() {
        return database;
    }

    public WebhookNotifier webhooks() {
        return webhookNotifier;
    }

    public ExternalItemProvider externalItems() {
        return externalItemProvider;
    }
}
