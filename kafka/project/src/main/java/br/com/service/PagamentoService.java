package br.com.service;

import br.com.dto.PagamentoDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PagamentoService {
    @Autowired
    private PagamentoRequestProducer pagamentoProducer;

    public String criar(PagamentoDTO pagamento) {
        try {
            return pagamentoProducer.sendMessage(pagamento);
        } catch (JsonProcessingException e) {
            return "Ocorreu um erro na request " + e.getMessage();
        }
    }
}
