package com.lfp.unibus.common.data

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import kotlin.io.encoding.Base64

object TestUtils {
  val objectMapper: ObjectMapper = run {
    return@run JsonMapper.builder().findAndAddModules().build()
  }
  const val imageBase64 =
      "iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAEklEQVR4nGP8z4APMOGVHbHSAEEsAROxCnMTAAAAAElFTkSuQmCC"
  val imageDataUrl = DataUrl(data = Base64.decode(imageBase64))
}
