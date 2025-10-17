package net.devvoxel.itemDB.command;

import net.devvoxel.itemDB.ItemDB;
import net.devvoxel.itemDB.data.ItemRecord;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public class DbCommand implements CommandExecutor, TabCompleter {

    private final ItemDB plugin;

    public DbCommand(ItemDB plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        var msg = plugin.messages();

        // /db → Hilfe
        if (args.length == 0) {
            sender.sendMessage(msg.get("usage"));
            return true;
        }

        // /db show → GUI öffnen
        if (args.length == 1 && args[0].equalsIgnoreCase("show")) {
            if (!(sender instanceof Player p)) {
                sender.sendMessage(msg.get("only-players"));
                return true;
            }
            if (!sender.hasPermission("net.devvoxel.itemdb.show")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }
            plugin.gui().open(p);
            return true;
        }

        // /db search <query> [limit]
        if (args.length >= 2 && args[0].equalsIgnoreCase("search")) {
            if (!sender.hasPermission("net.devvoxel.itemdb.search")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }

            String query = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
            int limit = plugin.getConfig().getInt("Search.DefaultLimit", 10);
            try {
                int parsed = Integer.parseInt(args[args.length - 1]);
                query = String.join(" ", Arrays.copyOfRange(args, 1, args.length - 1));
                limit = Math.max(1, Math.min(50, parsed));
            } catch (NumberFormatException ignored) {
                // last argument not an integer, treat as part of query
            }

            List<ItemRecord> results = plugin.items().search(query, null, limit);
            if (results.isEmpty()) {
                sender.sendMessage(msg.get("search-empty").replace("{query}", query));
                return true;
            }

            sender.sendMessage(msg.get("search-header").replace("{query}", query).replace("{size}", String.valueOf(results.size())));
            for (ItemRecord record : results) {
                String display = record.displayName() != null ? record.displayName() : "-";
                sender.sendMessage(msg.get("search-line")
                        .replace("{name}", record.key())
                        .replace("{display}", display)
                        .replace("{customModelData}", record.customModelData() == null ? "-" : record.customModelData().toString()));
            }
            return true;
        }

        // /db info <name>
        if (args.length == 2 && args[0].equalsIgnoreCase("info")) {
            if (!sender.hasPermission("net.devvoxel.itemdb.info")) {
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

        // /db <name> → Item holen
        if (args.length == 1) {
            if (!(sender instanceof Player p)) {
                sender.sendMessage(msg.get("only-players"));
                return true;
            }
            if (!sender.hasPermission("net.devvoxel.itemdb.use")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }
            String name = args[0];
            ItemStack item = plugin.items().get(name);
            if (item == null) {
                p.sendMessage(msg.get("item-not-found").replace("{name}", name));
                return true;
            }
            p.getInventory().addItem(item.clone());
            p.sendMessage(msg.get("item-given-self").replace("{name}", name));
            return true;
        }

        // /db edit <name> ...
        if (args.length >= 3 && args[0].equalsIgnoreCase("edit")) {
            if (!sender.hasPermission("net.devvoxel.itemdb.edit")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }

            String name = args[1];
            String action = args[2].toLowerCase(Locale.ROOT);

            switch (action) {
                case "display", "displayname" -> {
                    if (args.length == 3) {
                        if (plugin.items().clearDisplayName(name)) {
                            sender.sendMessage(msg.get("item-display-cleared").replace("{name}", name));
                        } else {
                            sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                        }
                        return true;
                    }
                    String displayName = String.join(" ", Arrays.copyOfRange(args, 3, args.length));
                    if (plugin.items().setDisplayName(name, displayName)) {
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
                        if (plugin.items().clearLore(name)) {
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
                        if (plugin.items().addLoreLine(name, line)) {
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
                        if (plugin.items().setLoreLine(name, index, line)) {
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
                        if (plugin.items().setCustomModelData(name, null)) {
                            sender.sendMessage(msg.get("item-custommodel-cleared").replace("{name}", name));
                        } else {
                            sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
                        }
                        return true;
                    }
                    if (args.length >= 4) {
                        try {
                            int value = Integer.parseInt(args[3]);
                            if (plugin.items().setCustomModelData(name, value)) {
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

        // /db add <name>
        if (args.length == 2 && args[0].equalsIgnoreCase("add")) {
            if (!(sender instanceof Player p)) {
                sender.sendMessage(msg.get("only-players"));
                return true;
            }
            if (!sender.hasPermission("net.devvoxel.itemdb.add")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }
            String name = args[1].toLowerCase(Locale.ROOT);
            if (plugin.items().exists(name)) {
                p.sendMessage(msg.get("item-exists").replace("{name}", name));
                return true;
            }

            ItemStack hand = p.getInventory().getItemInMainHand();
            if (hand == null || hand.getType().isAir()) {
                p.sendMessage(msg.get("no-hand"));
                return true;
            }

            plugin.items().add(name, hand);
            p.sendMessage(msg.get("item-added").replace("{name}", name));
            return true;
        }

        // /db remove <name>
        if (args.length == 2 && args[0].equalsIgnoreCase("remove")) {
            if (!sender.hasPermission("net.devvoxel.itemdb.remove")) {
                sender.sendMessage(msg.get("no-permission"));
                return true;
            }
            String name = args[1];
            if (plugin.items().remove(name)) {
                sender.sendMessage(msg.get("item-removed").replace("{name}", name));
            } else {
                sender.sendMessage(msg.get("item-not-found").replace("{name}", name));
            }
            return true;
        }

        // /db giveitem <name> <player>
        if (args.length == 3 && args[0].equalsIgnoreCase("giveitem")) {
            if (!sender.hasPermission("net.devvoxel.itemdb.giveitem")) {
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

        sender.sendMessage(msg.get("usage"));
        return true;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        List<String> out = new ArrayList<>();

        // 1. Argument
        if (args.length == 1) {
            if (sender.hasPermission("net.devvoxel.itemdb.show")) out.add("show");
            if (sender.hasPermission("net.devvoxel.itemdb.search")) out.add("search");
            if (sender.hasPermission("net.devvoxel.itemdb.info")) out.add("info");
            if (sender.hasPermission("net.devvoxel.itemdb.edit")) out.add("edit");
            if (sender.hasPermission("net.devvoxel.itemdb.add")) out.add("add");
            if (sender.hasPermission("net.devvoxel.itemdb.remove")) out.add("remove");
            if (sender.hasPermission("net.devvoxel.itemdb.giveitem")) out.add("giveitem");
            out.addAll(plugin.items().keys()); // Item-Namen
            return filter(out, args[0]);
        }

        // 2. Argument
        if (args.length == 2) {
            if (args[0].equalsIgnoreCase("info") || args[0].equalsIgnoreCase("edit")) {
                return filter(plugin.items().keys(), args[1]);
            }
            if (args[0].equalsIgnoreCase("remove") || args[0].equalsIgnoreCase("giveitem")) {
                return filter(plugin.items().keys(), args[1]);
            }
        }

        if (args.length == 3 && args[0].equalsIgnoreCase("edit")) {
            List<String> editOptions = List.of("display", "displayname", "lore", "custommodel");
            return filter(editOptions, args[2]);
        }

        if (args.length == 4 && args[0].equalsIgnoreCase("edit") && args[2].equalsIgnoreCase("lore")) {
            return filter(List.of("add", "set", "clear"), args[3]);
        }

        // 3. Argument (bei giveitem → Spieler)
        if (args.length == 3 && args[0].equalsIgnoreCase("giveitem")) {
            for (Player p : Bukkit.getOnlinePlayers()) {
                out.add(p.getName());
            }
            return filter(out, args[2]);
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
}
