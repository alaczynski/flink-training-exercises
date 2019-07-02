package com.example.lab4.infra;

import com.example.lab4.domain.Position;
import com.example.lab4.domain.PositionRepository;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import java.io.IOException;

public class PositionFlinkRepository implements PositionRepository {
    private transient ValueState<Boolean> positionOpenState;

    public PositionFlinkRepository(RuntimeContext runtimeContext) {
        positionOpenState = runtimeContext
                .getState(new ValueStateDescriptor<>("positionOpen", Boolean.class));
    }

    @Override
    public Position get() {
        try {
            Boolean openState = positionOpenState.value();
            return openState == null ? new Position(false) : new Position(openState);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void save(Position position) {
        try {
            positionOpenState.update(position.isOpen());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
