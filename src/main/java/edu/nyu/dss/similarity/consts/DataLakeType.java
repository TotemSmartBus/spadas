package edu.nyu.dss.similarity.consts;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

public enum DataLakeType {
    PURE_LOCATION(13, true, (file) -> file.getName().contains("ywz") || file.getParent().contains("ywz")),
    ARGOVERSE(13, true, (file)->file.getAbsolutePath().contains("argo")),
    // if a folder contains shapefile(.shp), we extract it with SHAPEFILE_READER
    SHAPE_FILE(12, true, (file) -> Objects.requireNonNull(file.list((f, name) -> name.matches(".*[.]shp"))).length > 0),
    OPEN_NYC(11, true, (file) -> file.getName().contains("nyc") && file.getName().contains("open")),
    POI(10, false, (file) -> file.getName().equals("poi")), // BAIDU POI
    // MOVE BANK
    MOVE_BANK(9, false, (file) -> file.getName().contains("movebank")), // BUS LINES
    BUS_LINE(8, false, "Bus_lines"::equals), // USA Datasets
    USA(8, true, (file) -> !file.getName().isEmpty() && Character.isUpperCase(file.getName().charAt(0))), // POIs

    BAIDU_POI(7, true, (file) -> true),
    ;

    public final int id;
    public final boolean active;
    public final Function<File, Boolean> func;

    private static final DataLakeType DEFAULT = BAIDU_POI;

    DataLakeType(int id, boolean active, Function<File, Boolean> func) {
        this.id = id;
        this.active = active;
        this.func = func;
    }


    public static DataLakeType matchType(File folder) {
        Object[] types = Arrays.stream(DataLakeType.values()).sorted(Comparator.comparingInt(a -> a.id)).toArray();
        for (int i = types.length - 1; i >= 0; i--) {
            if ((((DataLakeType) types[i]).func.apply(folder))) {
                System.out.println(String.format("we match the file %s with type %s", folder.getName(), ((DataLakeType) types[i]).name()));
                return (DataLakeType) types[i];
            }
        }
        return DEFAULT;
    }
}
