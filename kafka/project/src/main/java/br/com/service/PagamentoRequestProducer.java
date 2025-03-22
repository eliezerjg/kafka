package br.com.service;

import br.com.dto.PagamentoDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PagamentoRequestProducer {

    @Value("${topicos.pagamento.request.topic}")
    private String topicoPagamentoRequest;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public String sendMessage(PagamentoDTO pagamento) throws  JsonProcessingException {
        String orderAsMessage = objectMapper.writeValueAsString(pagamento);
        kafkaTemplate.send(topicoPagamentoRequest, orderAsMessage);
        return "Pagamento enviado";
    }
}
