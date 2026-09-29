package com.demo.transaction.model;

import java.math.BigDecimal;

public record TransactionRequest(
    String accountId,
    BigDecimal amount,
    String currency,
    String targetAccount
) {}
