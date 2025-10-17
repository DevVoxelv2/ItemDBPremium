package net.devvoxel.itemDB.command;

import net.devvoxel.itemDB.ItemDB;
import net.devvoxel.itemDB.data.ItemRecord;
import net.devvoxel.itemDB.data.ItemVersion;
import net.devvoxel.itemDB.i18n.MessageManager;
import net.devvoxel.itemDB.managers.ItemManager;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class DbCommand implements CommandExecutor, TabCompleter {

    private static final DateTimeFormatter HISTORY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final ItemDB plugin;

    public DbCommand(ItemDB plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        MessageManager msg = plugin.messages();

        if (args.length == 0) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }

        String sub = args[0].toLowerCase(Locale.ROOT);
        switch (sub) {
            case "show":
                return handleShow(sender, args, msg);
            case "search":
                return handleSearch(sender, args, msg);
            case "info":
                return handleInfo(sender, args, msg);
            case "edit":
                return handleEdit(sender, args, msg);
            case "add":
                return handleAdd(sender, args, msg);
            case "remove":
                return handleRemove(sender, args, msg);
            case "giveitem":
                return handleGiveItem(sender, args, msg);
            case "history":
                return handleHistory(sender, args, msg);
            case "diff":
                return handleDiff(sender, args, msg);
            case "rollback":
                return handleRollback(sender, args, msg);
            case "import":
                return handleImport(sender, args, msg);
            case "export":
                return handleExport(sender, args, msg);
            default:
                return handleDefault(sender, args, msg);
        }
    }

    private boolean handleShow(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 1) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }
        if (!(sender instanceof Player player)) {
            sender.sendMessage(msg.get("only-players"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.show")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        plugin.gui().open(player);
        return true;
    }

    private boolean handleSearch(CommandSender sender, String[] args, MessageManager msg) {
        if (!sender.hasPermission("itemdb.premium.search")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        if (args.length < 2) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }

        String query = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
        int limit = plugin.getConfig().getInt("Search.DefaultLimit", 10);
        try {
            int parsed = Integer.parseInt(args[args.length - 1]);
            query = String.join(" ", Arrays.copyOfRange(args, 1, args.length - 1));
            limit = Math.max(1, Math.min(50, parsed));
        } catch (NumberFormatException ignored) {
            // not a limit
        }

        List<ItemRecord> results = plugin.items().search(query, null, limit);
        if (results.isEmpty()) {
            sender.sendMessage(msg.get("search-empty").replace("{query}", query));
            return true;
        }

        sender.sendMessage(msg.get("search-header")
                .replace("{query}", query)
                .replace("{size}", String.valueOf(results.size())));
        for (ItemRecord record : results) {
            String display = record.displayName() != null ? record.displayName() : "-";
            sender.sendMessage(msg.get("search-line")
                    .replace("{name}", record.key())
                    .replace("{display}", display)
                    .replace("{customModelData}", record.customModelData() == null ? "-" : record.customModelData().toString()));
        }
        return true;
    }

    private boolean handleInfo(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 2) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.info")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }

        Optional<ItemRecord> record = plugin.items().info(args[1]);
        if (record.isEmpty()) {
            sender.sendMessage(msg.get("item-not-found").replace("{name}", args[1]));
            return true;
        }

        ItemRecord itemRecord = record.get();
        sender.sendMessage(msg.get("info-header").replace("{name}", itemRecord.key()));
        sender.sendMessage(msg.get("info-display").replace("{display}", itemRecord.displayName() == null ? "-" : itemRecord.displayName()));
        sender.sendMessage(msg.get("info-lore")
                .replace("{lore}", itemRecord.lore().isEmpty() ? msg.get("info-lore-empty") : String.join(msg.get("info-lore-separator"), itemRecord.lore())));
        sender.sendMessage(msg.get("info-cmd")
                .replace("{customModelData}", itemRecord.customModelData() == null ? "-" : itemRecord.customModelData().toString()));
        sender.sendMessage(msg.get("info-enchants")
                .replace("{enchantments}", itemRecord.enchantments().isEmpty() ? msg.get("info-enchants-empty") : formatEnchantments(itemRecord)));
        return true;
    }

    private boolean handleEdit(CommandSender sender, String[] args, MessageManager msg) {
        if (!sender.hasPermission("itemdb.premium.edit")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        if (args.length < 3) {
            sender.sendMessage(msg.get("usage-edit"));
            return true;
        }

        String name = args[1];
        String action = args[2].toLowerCase(Locale.ROOT);
        String editor = senderName(sender);

        switch (action) {
            case "display", "displayname" -> {
                if (args.length == 3) {
                    if (plugin.items().clearDisplayName(name, editor)) {
                        sender.sendMessage(msg.get("item-display-cleared").replace("{name}", name));
                    } else {
                        sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                    }
                    return true;
                }
                String displayName = String.join(" ", Arrays.copyOfRange(args, 3, args.length));
                if (plugin.items().setDisplayName(name, displayName, editor)) {
                    sender.sendMessage(msg.get("item-display-updated").replace("{name}", name));
                } else {
                    sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                }
                return true;
            }
            case "lore" -> {
                if (args.length < 4) {
                    sender.sendMessage(msg.get("usage-edit-lore"));
                    return true;
                }
                String loreAction = args[3].toLowerCase(Locale.ROOT);
                if (loreAction.equals("clear")) {
                    if (plugin.items().clearLore(name, editor)) {
                        sender.sendMessage(msg.get("item-lore-cleared").replace("{name}", name));
                    } else {
                        sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                    }
                    return true;
                }

                if (loreAction.equals("add")) {
                    if (args.length < 5) {
                        sender.sendMessage(msg.get("usage-edit-lore"));
                        return true;
                    }
                    String line = String.join(" ", Arrays.copyOfRange(args, 4, args.length));
                    if (plugin.items().addLoreLine(name, line, editor)) {
                        sender.sendMessage(msg.get("item-lore-added").replace("{name}", name));
                    } else {
                        sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                    }
                    return true;
                }

                if (loreAction.equals("set")) {
                    if (args.length < 6) {
                        sender.sendMessage(msg.get("usage-edit-lore"));
                        return true;
                    }
                    int index;
                    try {
                        index = Math.max(0, Integer.parseInt(args[4]) - 1);
                    } catch (NumberFormatException ex) {
                        sender.sendMessage(msg.get("usage-edit-lore"));
                        return true;
                    }
                    String line = String.join(" ", Arrays.copyOfRange(args, 5, args.length));
                    if (plugin.items().setLoreLine(name, index, line, editor)) {
                        sender.sendMessage(msg.get("item-lore-set").replace("{name}", name).replace("{line}", String.valueOf(index + 1)));
                    } else {
                        sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                    }
                    return true;
                }

                sender.sendMessage(msg.get("usage-edit-lore"));
                return true;
            }
            case "custommodel" -> {
                if (args.length == 3 || (args.length == 4 && args[3].equalsIgnoreCase("clear"))) {
                    if (plugin.items().setCustomModelData(name, null, editor)) {
                        sender.sendMessage(msg.get("item-custommodel-cleared").replace("{name}", name));
                    } else {
                        sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                    }
                    return true;
                }
                if (args.length >= 4) {
                    try {
                        int value = Integer.parseInt(args[3]);
                        if (plugin.items().setCustomModelData(name, value, editor)) {
                            sender.sendMessage(msg.get("item-custommodel-updated").replace("{name}", name).replace("{value}", String.valueOf(value)));
                        } else {
                            sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                        }
                    } catch (NumberFormatException ex) {
                        sender.sendMessage(msg.get("usage-edit-custommodel"));
                    }
                    return true;
                }
                sender.sendMessage(msg.get("usage-edit-custommodel"));
                return true;
            }
            default -> {
                sender.sendMessage(msg.get("usage-edit"));
                return true;
            }
        }
    }

    private boolean handleAdd(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 2) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.add")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        if (!(sender instanceof Player player)) {
            sender.sendMessage(msg.get("only-players"));
            return true;
        }
        String name = args[1].toLowerCase(Locale.ROOT);
        if (plugin.items().exists(name)) {
            player.sendMessage(msg.get("item-exists").replace("{name}", name));
            return true;
        }

        ItemStack hand = player.getInventory().getItemInMainHand();
        if (hand == null || hand.getType().isAir()) {
            player.sendMessage(msg.get("no-hand"));
            return true;
        }

        plugin.items().add(name, hand, senderName(sender));
        player.sendMessage(msg.get("item-added").replace("{name}", name));
        return true;
    }

    private boolean handleRemove(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 2) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.remove")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        String name = args[1];
        if (plugin.items().remove(name, senderName(sender))) {
            sender.sendMessage(msg.get("item-removed").replace("{name}", name));
        } else {
            sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
        }
        return true;
    }

    private boolean handleGiveItem(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 3) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.giveitem")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        String name = args[1];
        Player target = Bukkit.getPlayerExact(args[2]);
        if (target == null) {
            sender.sendMessage(msg.get("player-not-found").replace("{player}", args[2]));
            return true;
        }
        ItemStack item = plugin.items().get(name);
        if (item == null) {
            sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
            return true;
        }
        target.getInventory().addItem(item.clone());
        sender.sendMessage(msg.get("item-given-other")
                .replace("{name}", name)
                .replace("{player}", target.getName()));
        target.sendMessage(msg.get("item-given-self").replace("{name}", name));
        return true;
    }

    private boolean handleHistory(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 2) {
            sender.sendMessage(msg.get("usage-history"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.history")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }

        String name = args[1];
        int limit = Math.max(1, plugin.getConfig().getInt("History.DefaultLimit", 20));
        List<ItemVersion> versions = plugin.items().history(name, limit);
        if (versions.isEmpty()) {
            sender.sendMessage(msg.get("history-empty").replace("{name}", name));
            return true;
        }
        sender.sendMessage(msg.get("history-header").replace("{name}", name));
        for (ItemVersion version : versions) {
            String editor = version.editor() == null || version.editor().isBlank() ? msg.get("history-line-no-editor") : version.editor();
            String comment = version.comment() == null || version.comment().isBlank() ? msg.get("history-line-no-comment") : version.comment();
            String deletedSuffix = version.deleted() ? msg.get("history-line-deleted") : "";
            String line = msg.get("history-line")
                    .replace("{version}", String.valueOf(version.version()))
                    .replace("{timestamp}", formatTimestamp(version.createdAt()))
                    .replace("{editor}", editor)
                    .replace("{comment}", comment)
                    .replace("{deleted}", deletedSuffix);
            sender.sendMessage(line);
        }
        return true;
    }

    private boolean handleDiff(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 4) {
            sender.sendMessage(msg.get("usage-diff"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.diff")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        String name = args[1];
        int versionA;
        int versionB;
        try {
            versionA = Integer.parseInt(args[2]);
            versionB = Integer.parseInt(args[3]);
        } catch (NumberFormatException ex) {
            sender.sendMessage(msg.get("usage-diff"));
            return true;
        }

        Optional<ItemVersion> first = plugin.items().version(name, versionA);
        if (first.isEmpty()) {
            sender.sendMessage(msg.get("diff-version-missing").replace("{name}", name).replace("{version}", String.valueOf(versionA)));
            return true;
        }
        Optional<ItemVersion> second = plugin.items().version(name, versionB);
        if (second.isEmpty()) {
            sender.sendMessage(msg.get("diff-version-missing").replace("{name}", name).replace("{version}", String.valueOf(versionB)));
            return true;
        }

        List<String> diff = plugin.items().diff(name, versionA, versionB);
        if (diff.isEmpty()) {
            sender.sendMessage(msg.get("diff-no-change"));
            return true;
        }

        sender.sendMessage(msg.get("diff-header")
                .replace("{name}", name)
                .replace("{from}", String.valueOf(versionA))
                .replace("{to}", String.valueOf(versionB)));
        for (String entry : diff) {
            if (entry.length() < 3) {
                continue;
            }
            char type = entry.charAt(0);
            String body = entry.substring(2);
            String template = switch (type) {
                case '+' -> msg.get("diff-line-added");
                case '-' -> msg.get("diff-line-removed");
                case '~' -> msg.get("diff-line-changed");
                default -> msg.get("diff-line-changed");
            };
            sender.sendMessage(template.replace("{line}", body));
        }
        return true;
    }

    private boolean handleRollback(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length != 3) {
            sender.sendMessage(msg.get("usage-rollback"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.rollback")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        String name = args[1];
        int version;
        try {
            version = Integer.parseInt(args[2]);
        } catch (NumberFormatException ex) {
            sender.sendMessage(msg.get("usage-rollback"));
            return true;
        }

        Optional<ItemVersion> target = plugin.items().version(name, version);
        if (target.isEmpty()) {
            sender.sendMessage(msg.get("rollback-version-missing").replace("{name}", name).replace("{version}", String.valueOf(version)));
            return true;
        }

        if (plugin.items().rollback(name, version, senderName(sender))) {
            sender.sendMessage(msg.get("rollback-success").replace("{name}", name).replace("{version}", String.valueOf(version)));
        } else {
            sender.sendMessage(msg.get("rollback-failed").replace("{name}", name));
        }
        return true;
    }

    private boolean handleImport(CommandSender sender, String[] args, MessageManager msg) {
        if (!sender.hasPermission("itemdb.premium.import")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        if (args.length < 3 || !args[1].equalsIgnoreCase("file")) {
            sender.sendMessage(msg.get("usage-import"));
            return true;
        }

        String rawPath = args[2];
        String namespace = null;
        boolean dryRun = false;
        for (int i = 3; i < args.length; i++) {
            String option = args[i];
            if (option.equalsIgnoreCase("--dry-run")) {
                dryRun = true;
            } else if (option.startsWith("--namespace=")) {
                namespace = option.substring("--namespace=".length());
            } else {
                sender.sendMessage(msg.get("import-unknown-option").replace("{option}", option));
                return true;
            }
        }

        Path path;
        try {
            path = Path.of(rawPath);
        } catch (InvalidPathException ex) {
            sender.sendMessage(msg.get("import-invalid-path").replace("{file}", rawPath));
            return true;
        }

        ItemManager.ImportReport report = plugin.items().importFromZip(path, namespace, dryRun, senderName(sender));
        if (report.total() == 0 && report.errors().isEmpty()) {
            sender.sendMessage(msg.get("import-empty").replace("{file}", path.toString()));
        } else {
            String template = report.dryRun() ? msg.get("import-dry-run") : msg.get("import-success");
            sender.sendMessage(template
                    .replace("{file}", path.toString())
                    .replace("{total}", String.valueOf(report.total()))
                    .replace("{created}", String.valueOf(report.created()))
                    .replace("{updated}", String.valueOf(report.updated()))
                    .replace("{namespace}", namespace == null ? "-" : namespace));
        }
        if (!report.errors().isEmpty()) {
            sender.sendMessage(msg.get("import-errors-header"));
            for (String error : report.errors()) {
                sender.sendMessage(msg.get("import-error-line").replace("{error}", error));
            }
        }
        return true;
    }

    private boolean handleExport(CommandSender sender, String[] args, MessageManager msg) {
        if (!sender.hasPermission("itemdb.premium.export")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        if (args.length < 3 || !args[1].equalsIgnoreCase("file")) {
            sender.sendMessage(msg.get("usage-export"));
            return true;
        }

        String rawPath = args[2];
        String namespace = null;
        for (int i = 3; i < args.length; i++) {
            String option = args[i];
            if (option.startsWith("--namespace=")) {
                namespace = option.substring("--namespace=".length());
            } else {
                sender.sendMessage(msg.get("export-unknown-option").replace("{option}", option));
                return true;
            }
        }

        Path path;
        try {
            path = Path.of(rawPath);
        } catch (InvalidPathException ex) {
            sender.sendMessage(msg.get("export-invalid-path").replace("{file}", rawPath));
            return true;
        }

        ItemManager.ExportReport report = plugin.items().exportToZip(path, namespace, senderName(sender));
        if (!report.hasErrors()) {
            if (report.exported() == 0) {
                sender.sendMessage(msg.get("export-empty").replace("{file}", path.toString()));
            } else {
                sender.sendMessage(msg.get("export-success")
                        .replace("{file}", path.toString())
                        .replace("{count}", String.valueOf(report.exported()))
                        .replace("{namespace}", namespace == null ? "-" : namespace));
            }
        } else {
            sender.sendMessage(msg.get("export-partial")
                    .replace("{file}", path.toString())
                    .replace("{count}", String.valueOf(report.exported()))
                    .replace("{namespace}", namespace == null ? "-" : namespace));
            sender.sendMessage(msg.get("export-errors-header"));
            for (String error : report.errors()) {
                sender.sendMessage(msg.get("export-error-line").replace("{error}", error));
            }
        }
        return true;
    }

    private boolean handleDefault(CommandSender sender, String[] args, MessageManager msg) {
        if (args.length == 1) {
            return giveSelf(sender, args[0], msg);
        }
        sender.sendMessage(msg.get("usage"));
        return true;
    }

    private boolean giveSelf(CommandSender sender, String name, MessageManager msg) {
        if (!(sender instanceof Player player)) {
            sender.sendMessage(msg.get("only-players"));
            return true;
        }
        if (!sender.hasPermission("itemdb.premium.use")) {
            sender.sendMessage(msg.get("no-permission"));
            return true;
        }
        ItemStack item = plugin.items().get(name);
        if (item == null) {
            player.sendMessage(msg.get("item-not-found").replace("{name}", name));
            return true;
        }
        player.getInventory().addItem(item.clone());
        player.sendMessage(msg.get("item-given-self").replace("{name}", name));
        return true;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        List<String> out = new ArrayList<>();

        if (args.length == 1) {
            if (sender.hasPermission("itemdb.premium.show")) out.add("show");
            if (sender.hasPermission("itemdb.premium.search")) out.add("search");
            if (sender.hasPermission("itemdb.premium.info")) out.add("info");
            if (sender.hasPermission("itemdb.premium.edit")) out.add("edit");
            if (sender.hasPermission("itemdb.premium.add")) out.add("add");
            if (sender.hasPermission("itemdb.premium.remove")) out.add("remove");
            if (sender.hasPermission("itemdb.premium.giveitem")) out.add("giveitem");
            if (sender.hasPermission("itemdb.premium.history")) out.add("history");
            if (sender.hasPermission("itemdb.premium.diff")) out.add("diff");
            if (sender.hasPermission("itemdb.premium.rollback")) out.add("rollback");
            if (sender.hasPermission("itemdb.premium.import")) out.add("import");
            if (sender.hasPermission("itemdb.premium.export")) out.add("export");
            out.addAll(plugin.items().keys());
            return filter(out, args[0]);
        }

        if (args.length == 2) {
            String sub = args[0].toLowerCase(Locale.ROOT);
            switch (sub) {
                case "info", "edit", "remove", "giveitem", "history", "diff", "rollback" ->
                        out.addAll(plugin.items().keys());
                case "import", "export" -> out.add("file");
                default -> {
                }
            }
            return filter(out, args[1]);
        }

        if (args.length == 3) {
            String sub = args[0].toLowerCase(Locale.ROOT);
            if (sub.equals("giveitem")) {
                for (Player player : Bukkit.getOnlinePlayers()) {
                    out.add(player.getName());
                }
                return filter(out, args[2]);
            }
            if ((sub.equals("import") || sub.equals("export")) && args[1].equalsIgnoreCase("file")) {
                return out;
            }
        }

        if (args.length >= 3) {
            String sub = args[0].toLowerCase(Locale.ROOT);
            if ((sub.equals("import") || sub.equals("export")) && args[1].equalsIgnoreCase("file")) {
                out.add("--namespace=");
                if (sub.equals("import")) {
                    out.add("--dry-run");
                }
                return filter(out, args[args.length - 1]);
            }
        }

        return out;
    }

    private List<String> filter(List<String> list, String start) {
        String s = start.toLowerCase(Locale.ROOT);
        return list.stream().filter(x -> x.toLowerCase(Locale.ROOT).startsWith(s)).toList();
    }

    private String formatEnchantments(ItemRecord record) {
        if (record.enchantments().isEmpty()) {
            return "-";
        }
        List<String> parts = new ArrayList<>();
        record.enchantments().forEach((key, level) -> parts.add(key + " " + level));
        return String.join(", ", parts);
    }

    private String formatTimestamp(long epoch) {
        return HISTORY_FORMATTER.format(Instant.ofEpochMilli(epoch));
    }

    private String senderName(CommandSender sender) {
        return sender.getName();
    }
}
