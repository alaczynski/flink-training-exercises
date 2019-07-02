package com.example.lab4.infra;

import com.example.lab4.domain.Position;
import com.example.lab4.domain.TransactionEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.lab4.domain.TransactionType.BILATERAL;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class PositionFlinkRepositoryIntegrationTest {
    private KeyedOneInputStreamOperatorTestHarness<String, TransactionEvent, Position> testHarness;

    @Before
    public void setupTestHarness() throws Exception {
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(getKeyedProcessFunction()),
                (KeySelector<TransactionEvent, String>) TransactionEvent::getTransactionId,
                TypeInformation.of(String.class)
        );
        testHarness.setStateBackend(new FsStateBackend("file:///d:/tmp/flink/fs/checkpoints"));
        testHarness.open();
    }

    private static KeyedProcessFunction<String, TransactionEvent, Position> getKeyedProcessFunction() {
        return new KeyedProcessFunction<String, TransactionEvent, Position>() {
            private PositionFlinkRepository positionRepository;

            @Override
            public void open(Configuration parameters) {
                positionRepository = new PositionFlinkRepository(getRuntimeContext());
            }

            @Override
            public void processElement(TransactionEvent value, Context ctx, Collector<Position> out) {
                Position storedPosition = positionRepository.get();
                positionRepository.save(new Position(true));
                out.collect(storedPosition);
            }
        };
    }

    @Test
    public void shouldSuccessfullyStoreAndRetrieveValue() throws Exception {
        testHarness.processElement(new TransactionEvent("id-1", 0, BILATERAL), 1);
        testHarness.processElement(new TransactionEvent("id-1", 1, BILATERAL), 2);
        List<Position> positions = testHarness
                .extractOutputStreamRecords()
                .stream()
                .map((Function<StreamRecord<? extends Position>, Position>) StreamRecord::getValue)
                .collect(Collectors.toList());

        assertThat(positions).isEqualTo(asList(new Position(false), new Position(true)));
    }

}
