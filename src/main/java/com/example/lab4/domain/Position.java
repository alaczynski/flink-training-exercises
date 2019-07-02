package com.example.lab4.domain;

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
    public String toString() {
        return "Position{" +
                "isOpen=" + isOpen +
                '}';
    }
}
