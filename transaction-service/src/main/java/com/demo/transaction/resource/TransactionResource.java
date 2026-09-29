package com.demo.transaction.resource;

import com.demo.transaction.model.Transaction;
import com.demo.transaction.model.TransactionRequest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

@Path("/transactions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TransactionResource {

    private static final Logger LOG = Logger.getLogger(TransactionResource.class);

    @Inject
    @Channel("transactions-out")
    Emitter<Transaction> transactionEmitter;

    @POST
    public Uni<Response> createTransaction(TransactionRequest request) {
        Transaction tx = new Transaction(
            UUID.randomUUID().toString(),
            request.accountId(),
            request.targetAccount(),
            request.amount(),
            request.currency() != null ? request.currency() : "USD",
            "PENDING",
            Instant.now()
        );

        LOG.infof("Recibida transaccion [%s] por monto %s %s. Despachando a Kafka...",
                tx.id(), tx.amount(), tx.currency());

        // emitter.send() retorna un CompletionStage que convertimos a Uni (no bloqueante)
        return Uni.createFrom().completionStage(() -> transactionEmitter.send(tx))
            .onItem().invoke(() -> LOG.infof("Transaccion [%s] aceptada por el broker Kafka", tx.id()))
            .onFailure().invoke(err -> LOG.errorf(err, "Fallo al enviar transaccion [%s] a Kafka", tx.id()))
            .replaceWith(() -> Response.status(Response.Status.ACCEPTED).entity(tx).build());
    }
}
