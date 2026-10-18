#pragma once

#include <Mesh.h>

#ifdef ENABLE_MQTT_OBSERVER
void mqtt_observer_begin(mesh::RTCClock* rtc);
void mqtt_observer_loop();
void mqtt_observer_forward_rx(const mesh::Packet* pkt, int len, float snr, float rssi);
void mqtt_observer_forward_tx(const mesh::Packet* pkt, int len);
#else
inline void mqtt_observer_begin(mesh::RTCClock*) { }
inline void mqtt_observer_loop() { }
inline void mqtt_observer_forward_rx(const mesh::Packet*, int, float, float) { }
inline void mqtt_observer_forward_tx(const mesh::Packet*, int) { }
#endif
