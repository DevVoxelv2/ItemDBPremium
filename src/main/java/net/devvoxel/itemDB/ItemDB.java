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

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.regex.Pattern;

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

    private static final Pattern LICENSE_VALID_PATTERN = Pattern.compile("\"valid\"\\s*:\\s*(true|false)", Pattern.CASE_INSENSITIVE);
    private static final int PRODUCT_ID = 4792;

    public static ItemDB get() {
        return instance;
    }

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        saveResource("messages.yml", false);

        if (!verifyLicense()) {
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        try {
            this.database = new Database(this);
            database.connect();

            this.messageManager = new MessageManager(this);
            this.webhookNotifier = new WebhookNotifier(this);
            this.externalItemProvider = new ExternalItemProvider(this);
            this.itemManager = new ItemManager(this, database, webhookNotifier, externalItemProvider);
            this.itemsGui = new ItemsGui(this);

            if (externalItemProvider.hasAnyIntegration()) {
                getLogger().info("Active item integrations: " + externalItemProvider.describeSources());
            }

            if (Bukkit.getPluginManager().isPluginEnabled("PlaceholderAPI")) {
                this.placeholderExpansion = new ItemDBPlaceholderExpansion(this);
                this.placeholderExpansion.register();
                getLogger().info("PlaceholderAPI support enabled.");
            }

            var dbCmd = new DbCommand(this);
            getCommand("db").setExecutor(dbCmd);
            getCommand("db").setTabCompleter(dbCmd);

            Bukkit.getPluginManager().registerEvents(itemsGui, this);

            // === bStats Metrics ===
            int pluginId = 27408; // bStats plugin id
            Metrics metrics = new Metrics(this, pluginId);

            // Custom Chart: Anzahl gespeicherter Items
            metrics.addCustomChart(new SingleLineChart("stored_items", () -> itemManager.size()));

            // Custom Chart: DB-Typ (mysql/sqlite)
            metrics.addCustomChart(new SimplePie("database_type", () ->
                    getConfig().getString("Database.Type", "mysql").toLowerCase())
            );

            long interval = Math.max(20L, getConfig().getLong("Database.SyncIntervalTicks", 100L));
            this.syncTask = itemManager.applySyncTask(interval);

            getLogger().info("ItemDBPremium has been enabled. Thank you for your support!");
            getLogger().info("Loaded items from the database: " + itemManager.size());
        } catch (Exception e) {
            getLogger().severe("Unable to connect to the database: " + e.getMessage());
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
        getLogger().info("ItemDBPremium has been disabled.");
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

    private boolean verifyLicense() {
        String licenseKey = getConfig().getString("License.Key", "").trim();

        if (licenseKey.isEmpty()) {
            getLogger().severe("License check failed: license key is missing in the configuration.");
            return false;
        }

        try {
            String query = "pid=" + PRODUCT_ID + "&key=" + URLEncoder.encode(licenseKey, StandardCharsets.UTF_8);
            URI uri = URI.create("https://api.chunkfactory.com?" + query);

            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                getLogger().severe("License check failed: unexpected response code " + response.statusCode());
                return false;
            }

            String body = response.body();
            var matcher = LICENSE_VALID_PATTERN.matcher(body);
            if (matcher.find()) {
                boolean valid = Boolean.parseBoolean(matcher.group(1));
                if (valid) {
                    getLogger().info("License validated successfully.");
                    return true;
                }
                getLogger().severe("License check failed: the configured license key is invalid.");
                return false;
            }

            getLogger().severe("License check failed: could not parse response.");
        } catch (Exception exception) {
            getLogger().severe("License check failed: " + exception.getMessage());
        }

        return false;
    }
}
