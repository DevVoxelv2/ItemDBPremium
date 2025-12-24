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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

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

    private static final String API_BASE_URL = "https://www.craftingstudiopro.de";
    private static final String LICENSE_VALIDATE_ENDPOINT = "/api/license/validate";
    private static final String PLUGIN_ID = "itemdbpremium";
    private static final Gson GSON = new Gson();

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
            // Erstelle JSON Request Body
            JsonObject requestBody = new JsonObject();
            requestBody.addProperty("licenseKey", licenseKey);
            requestBody.addProperty("pluginId", PLUGIN_ID);
            String jsonBody = GSON.toJson(requestBody);

            URI uri = URI.create(API_BASE_URL + LICENSE_VALIDATE_ENDPOINT);

            HttpRequest request = HttpRequest.newBuilder(uri)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
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
            JsonObject jsonResponse = GSON.fromJson(body, JsonObject.class);

            if (jsonResponse == null) {
                getLogger().severe("License check failed: could not parse response.");
                return false;
            }

            boolean valid = jsonResponse.has("valid") && jsonResponse.get("valid").getAsBoolean();

            if (valid) {
                getLogger().info("License validated successfully.");
                if (jsonResponse.has("purchase")) {
                    JsonObject purchase = jsonResponse.getAsJsonObject("purchase");
                    getLogger().info("Purchase ID: " + purchase.get("id").getAsString());
                }
                return true;
            } else {
                String message = jsonResponse.has("message") 
                    ? jsonResponse.get("message").getAsString() 
                    : "Ungültiger Lizenz-Key";
                getLogger().severe("License check failed: " + message);
                return false;
            }
        } catch (Exception exception) {
            getLogger().severe("License check failed: " + exception.getMessage());
            if (getLogger().isLoggable(java.util.logging.Level.FINE)) {
                exception.printStackTrace();
            }
        }

        return false;
    }
}
