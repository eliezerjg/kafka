package br.com.producer;

import br.com.config.ProdutorKafkaConfig;
import br.com.dto.PagamentoDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service("pagamentoRequestProducerService")
public class PagamentoRequestProducer {
    private static final Logger log = LoggerFactory.getLogger(PagamentoRequestProducer.class);

    @Value("${topicos.pagamento.request.topic}")
    private String topicoPagamentoRequest;

    private ProdutorKafkaConfig kafkaProdutorConfig;

    private ObjectMapper objectMapper;

    public PagamentoRequestProducer(ProdutorKafkaConfig kafkaProdutorConfig, ObjectMapper objectMapper){
         this.kafkaProdutorConfig = kafkaProdutorConfig;
         this.objectMapper = new ObjectMapper();
    }

    public String sendMessage(PagamentoDTO pagamento) throws JsonProcessingException {
        String conteudo = objectMapper.writeValueAsString(pagamento);
        kafkaProdutorConfig.kafkaTemplate().send(topicoPagamentoRequest, conteudo);
        return "Pagamento enviado para processamento";
    }

    @PostConstruct
    public void populate() {
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            log.info("Rodando tarefa de pagamento...");
            PagamentoDTO pagamento = new PagamentoDTO();
            pagamento.setDescricao("xxxxxx " + UUID.randomUUID());
            Random randomGenerator = new Random();
            pagamento.setNumero(1 + randomGenerator.nextInt());
            pagamento.setValor(new BigDecimal(randomGenerator.nextInt()));
            try {
                sendMessage(pagamento);
            } catch (JsonProcessingException e) {
                log.error("Erro ao enviar pagamento: ", e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }
}
