package net.devvoxel.itemDB.integration;

import net.devvoxel.itemDB.ItemDB;
import org.bukkit.inventory.ItemStack;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.logging.Level;

public class ExternalItemProvider {

    private final ItemDB plugin;
    private final boolean itemsAdderAvailable;
    private final Method itemsAdderGetInstance;
    private final Method itemsAdderGetItemStack;

    private final boolean oraxenAvailable;
    private final Method oraxenGetItemById;
    private final Method oraxenBuild;

    public ExternalItemProvider(ItemDB plugin) {
        this.plugin = plugin;
        this.itemsAdderGetInstance = resolveMethod("dev.lone.itemsadder.api.CustomStack", "getInstance", String.class);
        this.itemsAdderGetItemStack = resolveMethod("dev.lone.itemsadder.api.CustomStack", "getItemStack");
        this.itemsAdderAvailable = plugin.getServer().getPluginManager().isPluginEnabled("ItemsAdder")
                && this.itemsAdderGetInstance != null && this.itemsAdderGetItemStack != null;

        this.oraxenGetItemById = resolveMethod("io.th0rgal.oraxen.api.OraxenItems", "getItemById", String.class);
        this.oraxenBuild = resolveMethod("io.th0rgal.oraxen.items.OraxenItem", "build");
        this.oraxenAvailable = plugin.getServer().getPluginManager().isPluginEnabled("Oraxen")
                && this.oraxenGetItemById != null && this.oraxenBuild != null;

        if (itemsAdderAvailable) {
            plugin.getLogger().info("ItemsAdder support enabled.");
        }
        if (oraxenAvailable) {
            plugin.getLogger().info("Oraxen support enabled.");
        }
    }

    private Method resolveMethod(String className, String methodName, Class<?>... parameterTypes) {
        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method;
        } catch (ClassNotFoundException ignored) {
            return null;
        } catch (NoSuchMethodException ex) {
            plugin.getLogger().log(Level.WARNING, "Missing method " + methodName + " on " + className + ": " + ex.getMessage());
            return null;
        }
    }

    public Optional<ItemStack> resolve(String rawId) {
        if (rawId == null || rawId.isBlank()) {
            return Optional.empty();
        }
        String id = rawId.trim();
        Optional<ItemStack> itemsAdder = resolveItemsAdder(id);
        if (itemsAdder.isPresent()) {
            return itemsAdder.map(ItemStack::clone);
        }
        Optional<ItemStack> oraxen = resolveOraxen(id);
        return oraxen.map(ItemStack::clone);
    }

    private Optional<ItemStack> resolveItemsAdder(String id) {
        if (!itemsAdderAvailable) {
            return Optional.empty();
        }
        try {
            Object customStack = itemsAdderGetInstance.invoke(null, id);
            if (customStack == null) {
                return Optional.empty();
            }
            Object stack = itemsAdderGetItemStack.invoke(customStack);
            if (stack instanceof ItemStack itemStack) {
                return Optional.of(itemStack);
            }
        } catch (ReflectiveOperationException ex) {
            plugin.getLogger().log(Level.WARNING, "Failed to resolve ItemsAdder item " + id + ": " + ex.getMessage());
        }
        return Optional.empty();
    }

    private Optional<ItemStack> resolveOraxen(String id) {
        if (!oraxenAvailable) {
            return Optional.empty();
        }
        try {
            Object item = oraxenGetItemById.invoke(null, id);
            if (item == null) {
                return Optional.empty();
            }
            Object built = oraxenBuild.invoke(item);
            if (built instanceof ItemStack stack) {
                return Optional.of(stack);
            }
        } catch (ReflectiveOperationException ex) {
            plugin.getLogger().log(Level.WARNING, "Failed to resolve Oraxen item " + id + ": " + ex.getMessage());
        }
        return Optional.empty();
    }

    public String describeSources() {
        if (itemsAdderAvailable && oraxenAvailable) {
            return "ItemsAdder, Oraxen";
        }
        if (itemsAdderAvailable) {
            return "ItemsAdder";
        }
        if (oraxenAvailable) {
            return "Oraxen";
        }
        return "None";
    }

    public boolean hasAnyIntegration() {
        return itemsAdderAvailable || oraxenAvailable;
    }
}
