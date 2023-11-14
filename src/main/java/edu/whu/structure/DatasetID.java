package edu.whu.structure;

import java.util.Objects;

public class DatasetID {
    private final int value;

    public DatasetID(int i) {
        this.value = i;
    }

    public int get() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            if (o.getClass() == Integer.class) {
                return (int) o == this.value;
            } else {
                return false;
            }
        }
        DatasetID datasetID = (DatasetID) o;
        return value == datasetID.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
