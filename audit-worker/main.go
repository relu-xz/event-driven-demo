package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"github.com/segmentio/kafka-go"
)

// TransactionEvaluated mapea el payload generado por el motor en Python
type TransactionEvaluated struct {
	ID              string   `json:"id"`
	AccountID       string   `json:"accountId"`
	TargetAccount   string   `json:"targetAccount"`
	Amount          float64  `json:"amount"`
	Currency        string   `json:"currency"`
	Status          string   `json:"status"`
	Timestamp       string   `json:"timestamp"`
	RiskScore       float64  `json:"riskScore"`
	RejectionReason *string  `json:"rejectionReason,omitempty"`
	EvaluatedAt     string   `json:"evaluatedAt"`
	EvaluatedBy     string   `json:"evaluatedBy"`
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func main() {
	// Logger estructurado JSON nativo de Go (slog)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	kafkaBroker := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	kafkaTopic := getEnv("INPUT_TOPIC", "tx-evaluated")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "audit-notification-group")

	slog.Info("Iniciando servicio de Auditoria y Notificaciones",
		"broker", kafkaBroker,
		"topic", kafkaTopic,
		"group_id", kafkaGroupID,
	)

	// Configuracion del lector de Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          kafkaTopic,
		GroupID:        kafkaGroupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Desactiva autocommit de fondo para confirmar manualmente
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	// Contexto cancelable vinculado a senales de terminacion del OS
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		// FetchMessage no hace commit automatico del offset
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Contexto cancelado. Cerrando loop de consumo ordenadamente...")
				break
			}
			slog.Error("Fallo al leer mensaje de Kafka", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Procesar payload
		var tx TransactionEvaluated
		if err := json.Unmarshal(msg.Value, &tx); err != nil {
			slog.Error("Error deserializando JSON de transaccion",
				"offset", msg.Offset,
				"partition", msg.Partition,
				"error", err,
			)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Ejecucion de logica de negocio
		processAuditAndNotification(&tx)

		// Commit manual de mensaje confirmado
		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("Fallo al confirmar offset a Kafka", "tx_id", tx.ID, "error", err)
		}
	}

	slog.Info("Servidor Go finalizado de forma segura.")
}

func processAuditAndNotification(tx *TransactionEvaluated) {
	// 1. Registro de Auditoria Inmutable
	slog.Info("[AUDITORIA] Evento registrado en ledger",
		"tx_id", tx.ID,
		"account_origin", tx.AccountID,
		"account_destination", tx.TargetAccount,
		"amount", fmt.Sprintf("%.2f %s", tx.Amount, tx.Currency),
		"status", tx.Status,
		"risk_score", tx.RiskScore,
		"evaluated_by", tx.EvaluatedBy,
	)

	// 2. Simulacion de Notificaciones
	if tx.Status == "APPROVED" {
		dispatchNotification(tx.AccountID, fmt.Sprintf(
			"Transferencia exitosa: Tu pago de %.2f %s a la cuenta %s fue procesado.",
			tx.Amount, tx.Currency, tx.TargetAccount,
		), "SUCCESS_ALERT")
	} else {
		reason := "Operacion denegada por seguridad"
		if tx.RejectionReason != nil {
			reason = *tx.RejectionReason
		}
		dispatchNotification(tx.AccountID, fmt.Sprintf(
			"ALERTA DE SEGURIDAD: Transaccion de %.2f %s bloqueada. Motivo: %s",
			tx.Amount, tx.Currency, reason,
		), "FRAUD_WARNING")
	}
}

func dispatchNotification(accountID, message, alertType string) {
	slog.Info("[NOTIFICACION DISPATCH]",
		"channel", "PUSH_NOTIFICATION",
		"target_account", accountID,
		"type", alertType,
		"content", message,
	)
}
