package com.lfp.unibus.websocket.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.lfp.unibus.common.KafkaService
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component
import org.springframework.web.util.UriComponents
import org.springframework.web.util.UriComponentsBuilder
import java.net.URI

@Component
abstract class KafkaHandler(
    internal val conversionService: ConversionService,
    internal val objectMapper: ObjectMapper,
    internal val kafkaService: KafkaService,
) {


}
