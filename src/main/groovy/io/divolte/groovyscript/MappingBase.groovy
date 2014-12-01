package io.divolte.groovyscript

import io.divolte.server.DslRecordMapping.ValueProducer
import io.divolte.server.SchemaMappingException

import java.net.URI

abstract class MappingBase extends Script {
  // The mapping property is set on the scripts binding by the 
  // initializing Java code. This is to avoid cyclic dependencies
  // between Java and Groovy code, as that introduces issues with 
  // IDE support.

  def mapping(Closure c) {
    c.delegate = mapping
    c.run()
  }

  def map(literal) {
    [
      'onto': { String fieldName -> mapping.map(fieldName, literal) }
    ]
  }

  def map(ValueProducer producer) {
    [
      'onto': { String fieldName -> mapping.map(fieldName, producer) }
    ]
  }

  def map(Closure<ValueProducer> producer) {
    [
      'onto': { String fieldName -> map producer.call() onto fieldName }
    ]
  }

  def when(ValueProducer<Boolean> producer) {
    [
      'apply': {
        Closure closure -> 
        mapping.when(producer, closure)
      }
    ]
  }

  def when(Closure<ValueProducer<Boolean>> producer) {
    [
      'apply': { Closure closure -> when producer.call() apply closure }
    ]
  }

  def match(String regex) {
    [
      'against': { ValueProducer<String> producer -> mapping.matcher(producer, regex) }
    ]
  }

  def int32 = Integer.TYPE
  def int64 = Long.TYPE
  def fp32 = Float.TYPE
  def fp64 = Double.TYPE
  def bool = Boolean.TYPE
  def uri = URI.class

  def parse(ValueProducer<String> producer) {
    [
      'to': { t -> switch(t) {
          case int32:
            mapping.toInt(producer)
            break
          case int64:
            mapping.toLong(producer)
            break
          case fp32:
            mapping.toFloat(producer)
            break
          case fp64:
            mapping.toDouble(producer)
            break
          case bool:
            mapping.toBoolean(producer)
            break
          case uri:
            mapping.parseUri(producer)
            break
          default:
            throw new SchemaMappingException("Cannot parse string into type: %s", t)
        }
      }
    ]
  }
}
