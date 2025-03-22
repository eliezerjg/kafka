package br.com.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PagamentoRequestConsumidor {

    @KafkaListener(
            topics = "${topicos.pagamento.request.topic}",
            groupId = "pagamento-request-1")
    public void consumeMessage(String message) {
        System.out.println("Mensagem recebida=========\n" + message);
    }
}
