package cissa.valim.tutorial.microsservico.kafka.service;

import cissa.valim.tutorial.microsservico.kafka.data.PedidoData;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class SalvarPedidoService {

    @SneakyThrows
    @KafkaListener(topics = "SalvarPedido", groupId = "MicrosservicoSalvaPedido")
    private void executar(ConsumerRecord<String, String> record){
        log.info("Key = {}", record.key());
        log.info("Headers = {}", record.headers());
        log.info("Partition = {}", record.partition());

        String strDados = record.value();
        ObjectMapper mapper = new ObjectMapper();

        PedidoData pedido;
        pedido = mapper.readValue(strDados, PedidoData.class);

        log.info("Evento recebido = {}", pedido);
    }
}
