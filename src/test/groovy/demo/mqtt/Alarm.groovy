package demo.mqtt

import groovy.transform.Canonical

@Canonical
class Alarm {
    String name
    Integer code
    String description
}
