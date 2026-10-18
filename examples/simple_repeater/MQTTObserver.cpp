#include "MQTTObserver.h"

#ifdef ENABLE_MQTT_OBSERVER

#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <time.h>
#include <ESP.h>

#ifndef WIFI_SSID
  #define WIFI_SSID ""
#endif
#ifndef WIFI_PWD
  #define WIFI_PWD ""
#endif
#ifndef MQTT_BROKER_1
  #define MQTT_BROKER_1 ""
#endif
#ifndef MQTT_BROKER_2
  #define MQTT_BROKER_2 ""
#endif
#ifndef MQTT_BROKER_3
  #define MQTT_BROKER_3 ""
#endif
#ifndef MQTT_PORT
  #define MQTT_PORT 1883
#endif
#ifndef MQTT_TOPIC_BASE
  #define MQTT_TOPIC_BASE "meshcore/observer"
#endif
#ifndef MQTT_CLIENT_PREFIX
  #define MQTT_CLIENT_PREFIX "meshcore-observer"
#endif
#ifndef NTP_SERVER_1
  #define NTP_SERVER_1 "pool.ntp.org"
#endif
#ifndef NTP_SERVER_2
  #define NTP_SERVER_2 "time.google.com"
#endif
#ifndef NTP_SYNC_INTERVAL_SECS
  #define NTP_SYNC_INTERVAL_SECS 21600
#endif

namespace {
constexpr uint32_t MIN_VALID_UNIX_TIME = 1704067200UL;   // 1 Jan 2024 UTC
constexpr uint32_t MAX_REASONABLE_UNIX_TIME = 2208988800UL;  // 1 Jan 2040 UTC
constexpr unsigned long WIFI_RETRY_INTERVAL_MS = 10000;
constexpr unsigned long MQTT_RETRY_INTERVAL_MS = 5000;
constexpr unsigned long NTP_RETRY_INTERVAL_MS = 30000;

struct CollectorConn {
  const char* host = nullptr;
  uint16_t port = MQTT_PORT;
  WiFiClient net;
  PubSubClient mqtt;
  bool enabled = false;
  unsigned long last_attempt = 0;

  CollectorConn() : mqtt(net) { }
};

class MqttObserver {
  mesh::RTCClock* _rtc = nullptr;
  CollectorConn _collectors[3];
  unsigned long _last_wifi_attempt = 0;
  unsigned long _last_ntp_attempt = 0;
  unsigned long _last_ntp_success = 0;
  uint8_t _mesh_time_samples = 0;
  uint32_t _mesh_time_values[3] = {0};

  bool wifiReady() const {
    return WiFi.status() == WL_CONNECTED;
  }

  bool clockValid() const {
    return _rtc && _rtc->getCurrentTime() >= MIN_VALID_UNIX_TIME;
  }

  bool isNtpDue() const {
    if (_last_ntp_success == 0) return true;
    return (millis() - _last_ntp_success) >= (NTP_SYNC_INTERVAL_SECS * 1000UL);
  }

  void connectWifi() {
    if (wifiReady()) return;
    if (_last_wifi_attempt && (millis() - _last_wifi_attempt < WIFI_RETRY_INTERVAL_MS)) return;
    _last_wifi_attempt = millis();
    WiFi.disconnect(false, false);
    WiFi.begin(WIFI_SSID, WIFI_PWD);
  }

  void syncTimeFromNtp() {
    if (!_rtc || !wifiReady() || !isNtpDue()) return;
    if (_last_ntp_attempt && (millis() - _last_ntp_attempt < NTP_RETRY_INTERVAL_MS)) return;

    _last_ntp_attempt = millis();
    configTime(0, 0, NTP_SERVER_1, NTP_SERVER_2);
    time_t now = time(nullptr);
    if (now < (time_t)MIN_VALID_UNIX_TIME || now > (time_t)MAX_REASONABLE_UNIX_TIME) return;

    uint32_t ntp_secs = (uint32_t)now;
    if (ntp_secs > _rtc->getCurrentTime()) {
      _rtc->setCurrentTime(ntp_secs);
    }
    _last_ntp_success = millis();
    _mesh_time_samples = 0;
  }

  void maybeSyncFromMeshPacket(const mesh::Packet* pkt) {
    if (!_rtc || clockValid()) return;
    if (!pkt || pkt->payload_len < sizeof(uint32_t)) return;

    uint32_t sender_timestamp;
    memcpy(&sender_timestamp, pkt->payload, sizeof(sender_timestamp));
    if (sender_timestamp < MIN_VALID_UNIX_TIME || sender_timestamp > MAX_REASONABLE_UNIX_TIME) return;

    if (_mesh_time_samples < 3) {
      _mesh_time_values[_mesh_time_samples++] = sender_timestamp;
    }

    if (_mesh_time_samples >= 2) {
      uint32_t chosen = _mesh_time_values[0];
      for (uint8_t i = 1; i < _mesh_time_samples; i++) {
        if (_mesh_time_values[i] > chosen) chosen = _mesh_time_values[i];
      }
      _rtc->setCurrentTime(chosen + 1);
    }
  }

  void ensureMqttConnected(CollectorConn& c, uint8_t idx) {
    if (!c.enabled || !wifiReady()) return;
    if (c.mqtt.connected()) {
      c.mqtt.loop();
      return;
    }
    if (c.last_attempt && (millis() - c.last_attempt < MQTT_RETRY_INTERVAL_MS)) return;
    c.last_attempt = millis();

    char client_id[64];
    uint32_t chip = (uint32_t)(ESP.getEfuseMac() & 0xFFFFFFFFULL);
    snprintf(client_id, sizeof(client_id), "%s-%08lX-%u", MQTT_CLIENT_PREFIX, (unsigned long)chip, (unsigned)idx + 1);
    c.mqtt.connect(client_id);
  }

  void publish(const char* direction, const mesh::Packet* pkt, int len, bool has_radio, float snr, float rssi) {
    if (!pkt || !wifiReady()) return;

    uint8_t raw[MAX_TRANS_UNIT + 1];
    int raw_len = pkt->writeTo(raw);
    if (raw_len <= 0) return;

    uint32_t ts = _rtc ? _rtc->getCurrentTime() : (uint32_t)time(nullptr);

    char raw_topic[96];
    char meta_topic[96];
    snprintf(raw_topic, sizeof(raw_topic), "%s/%s/raw", MQTT_TOPIC_BASE, direction);
    snprintf(meta_topic, sizeof(meta_topic), "%s/%s/meta", MQTT_TOPIC_BASE, direction);

    char meta[220];
    if (has_radio) {
      snprintf(meta, sizeof(meta),
               "{\"ts\":%lu,\"len\":%d,\"wire_len\":%d,\"snr\":%.2f,\"rssi\":%.1f}",
               (unsigned long)ts, len, raw_len, snr, rssi);
    } else {
      snprintf(meta, sizeof(meta),
               "{\"ts\":%lu,\"len\":%d,\"wire_len\":%d}",
               (unsigned long)ts, len, raw_len);
    }

    for (uint8_t i = 0; i < 3; i++) {
      CollectorConn& c = _collectors[i];
      if (!c.enabled || !c.mqtt.connected()) continue;
      c.mqtt.publish(raw_topic, raw, raw_len, false);
      c.mqtt.publish(meta_topic, meta, false);
    }
  }

public:
  void begin(mesh::RTCClock* rtc) {
    _rtc = rtc;
    WiFi.mode(WIFI_STA);
    WiFi.setAutoReconnect(true);
    WiFi.persistent(false);
    connectWifi();

    const char* hosts[3] = { MQTT_BROKER_1, MQTT_BROKER_2, MQTT_BROKER_3 };
    for (uint8_t i = 0; i < 3; i++) {
      _collectors[i].host = hosts[i];
      _collectors[i].enabled = hosts[i] && hosts[i][0];
      if (_collectors[i].enabled) {
        _collectors[i].mqtt.setServer(hosts[i], MQTT_PORT);
        _collectors[i].mqtt.setBufferSize(1024);
        _collectors[i].mqtt.setKeepAlive(30);
      }
    }
  }

  void loop() {
    connectWifi();
    if (wifiReady()) {
      syncTimeFromNtp();
      for (uint8_t i = 0; i < 3; i++) {
        ensureMqttConnected(_collectors[i], i);
      }
    }
  }

  void forwardRx(const mesh::Packet* pkt, int len, float snr, float rssi) {
    maybeSyncFromMeshPacket(pkt);
    publish("rx", pkt, len, true, snr, rssi);
  }

  void forwardTx(const mesh::Packet* pkt, int len) {
    publish("tx", pkt, len, false, 0, 0);
  }
};

MqttObserver g_observer;
}

void mqtt_observer_begin(mesh::RTCClock* rtc) {
  g_observer.begin(rtc);
}

void mqtt_observer_loop() {
  g_observer.loop();
}

void mqtt_observer_forward_rx(const mesh::Packet* pkt, int len, float snr, float rssi) {
  g_observer.forwardRx(pkt, len, snr, rssi);
}

void mqtt_observer_forward_tx(const mesh::Packet* pkt, int len) {
  g_observer.forwardTx(pkt, len);
}

#endif
