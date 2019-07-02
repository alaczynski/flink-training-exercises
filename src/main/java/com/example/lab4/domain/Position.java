package com.example.lab4.domain;

import java.util.Objects;

public class Position {
    private boolean isOpen;

    public Position(boolean isOpen) {
        this.isOpen = isOpen;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public void open() {
        isOpen = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Position position = (Position) o;
        return isOpen == position.isOpen;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isOpen);
    }

    @Override
    public String toString() {
        return "Position{" +
                "isOpen=" + isOpen +
                '}';
    }
}
