package com.demo.transaction.model;

import java.math.BigDecimal;
import java.time.Instant;

public record Transaction(
    String id,
    String accountId,
    String targetAccount,
    BigDecimal amount,
    String currency,
    String status,
    Instant timestamp
) {}
