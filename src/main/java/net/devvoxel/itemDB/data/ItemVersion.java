package net.devvoxel.itemDB.data;

public record ItemVersion(
        long id,
        String itemName,
        int version,
        String editor,
        String nbt,
        long createdAt,
        String comment,
        boolean deleted
) {
}
