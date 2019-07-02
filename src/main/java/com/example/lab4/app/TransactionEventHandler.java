package com.example.lab4.app;

import com.example.lab4.domain.Position;
import com.example.lab4.domain.PositionRepository;
import com.example.lab4.domain.Report;
import com.example.lab4.domain.TransactionEvent;
import com.example.lab4.domain.TransactionEventService;
import com.example.lab4.infra.PositionFlinkRepository;

public class TransactionEventHandler {
    private final PositionRepository positionRepository;
    private final TransactionEventService transactionEventService = new TransactionEventService();

    public TransactionEventHandler(PositionRepository positionRepository) {
        this.positionRepository = positionRepository;
    }

    public Report handle(TransactionEvent transactionEvent) {
        Position position = positionRepository.get();
        Report report = transactionEventService.process(transactionEvent, position);
        positionRepository.save(position);
        return report;
    }
}
