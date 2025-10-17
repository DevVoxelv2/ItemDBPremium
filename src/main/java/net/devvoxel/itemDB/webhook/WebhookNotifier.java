package net.devvoxel.itemDB.webhook;

import net.devvoxel.itemDB.ItemDB;
import org.bukkit.configuration.ConfigurationSection;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class WebhookNotifier {

    private final ItemDB plugin;
    private final String changeUrl;
    private final String errorUrl;

    public WebhookNotifier(ItemDB plugin) {
        this.plugin = plugin;
        ConfigurationSection discord = plugin.getConfig().getConfigurationSection("Webhooks.Discord");
        this.changeUrl = sanitize(discord != null ? discord.getString("OnChange") : null);
        this.errorUrl = sanitize(discord != null ? discord.getString("OnError") : null);
    }

    private String sanitize(String raw) {
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public void notifyChange(String action, String itemName, String editor, String comment) {
        if (changeUrl == null) {
            return;
        }
        StringBuilder message = new StringBuilder();
        message.append("**[")
                .append(action == null ? "update" : action.toLowerCase())
                .append("]** ");
        if (itemName != null) {
            message.append('`').append(itemName).append('`').append(' ');
        }
        if (editor != null && !editor.isBlank()) {
            message.append("by ").append(editor).append(' ');
        }
        if (comment != null && !comment.isBlank()) {
            message.append("- ").append(comment);
        }
        sendAsync(changeUrl, toJson(message.toString()));
    }

    public void notifyError(String context, String message, Throwable throwable) {
        if (errorUrl == null && changeUrl == null) {
            return;
        }
        StringBuilder payload = new StringBuilder();
        payload.append("**[error]** ");
        if (context != null && !context.isBlank()) {
            payload.append(context).append(':').append(' ');
        }
        if (message != null && !message.isBlank()) {
            payload.append(message);
        }
        if (throwable != null) {
            payload.append(" (")
                    .append(throwable.getClass().getSimpleName())
                    .append(':').append(' ')
                    .append(Objects.requireNonNullElse(throwable.getMessage(), "no message"))
                    .append(')');
        }
        String json = toJson(payload.toString());
        if (errorUrl != null) {
            sendAsync(errorUrl, json);
        } else if (changeUrl != null) {
            sendAsync(changeUrl, json);
        }
    }

    private String toJson(String content) {
        String escaped = content
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n");
        return "{\"content\":\"" + escaped + "\"}";
    }

    private void sendAsync(String url, String payload) {
        CompletableFuture.runAsync(() -> {
            try {
                post(url, payload);
            } catch (IOException ex) {
                plugin.getLogger().log(Level.WARNING, "Failed to send webhook to " + url + ": " + ex.getMessage());
            }
        });
    }

    private void post(String target, String payload) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(target).openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        connection.setRequestProperty("Content-Length", String.valueOf(data.length));

        try (OutputStream os = connection.getOutputStream()) {
            os.write(data);
        }

        int response = connection.getResponseCode();
        if (response < 200 || response >= 300) {
            plugin.getLogger().log(Level.WARNING, "Webhook responded with status " + response + " for URL " + target);
        }
        connection.disconnect();
    }
}
