package com.example.lab4.domain;

import java.io.IOException;

public interface PositionRepository {
    Position get();
    void save(Position position);
}
