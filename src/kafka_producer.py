#!/usr/bin/env python3
"""
Producer Kafka - Tarea 3 (Anexo 3 mejorado)
Genera datos simulados de sensores de calidad del aire (PM10/PM2.5 + temp/humidity)
Compatible con VM vboxuser/bigdata y con local sin Kafka (modo dry-run).

Uso:
  pip install kafka-python
  python3 src/kafka_producer.py              # produce infinito cada 1s
  python3 src/kafka_producer.py --once       # solo 5 mensajes para prueba
  python3 src/kafka_producer.py --dry-run    # sin Kafka, solo imprime
"""
import time
import json
import random
import argparse
import sys

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[WARN] kafka-python no instalado. Usando modo dry-run. Instalar con: pip install kafka-python")

def generate_sensor_data():
    # Simula sensor de calidad aire + temp/humedad (coherente con dataset batch)
    # Departamentos con mayor contaminación generan valores más altos ocasionalmente
    departamentos = ["Bogotá D.C.", "Antioquia", "Valle del Cauca", "Atlántico", "Santander"]
    dep = random.choice(departamentos)
    base_pm10 = random.gauss(35, 12)
    if dep in ["Bogotá D.C.", "Valle del Cauca"]:
        base_pm10 += random.uniform(5, 20)
    pm10 = max(5, round(base_pm10, 2))
    pm25 = max(3, round(pm10 * random.uniform(0.45, 0.65), 2))
    return {
        "sensor_id": random.randint(1, 10),
        "departamento": dep,
        "pm10": pm10,
        "pm25": pm25,
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())  # epoch seconds
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Envía 5 mensajes y termina")
    parser.add_argument("--dry-run", action="store_true", help="No conecta a Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap")
    parser.add_argument("--topic", default="sensor_data", help="Topic Kafka")
    args = parser.parse_args()

    dry_run = args.dry_run or not KAFKA_AVAILABLE

    producer = None
    if not dry_run:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[args.bootstrap],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"[INFO] Conectado a Kafka {args.bootstrap}, topic={args.topic}")
        except Exception as e:
            print(f"[ERROR] No se pudo conectar a Kafka: {e}")
            print("[INFO] Cambiando a dry-run")
            dry_run = True

    count = 0
    try:
        while True:
            data = generate_sensor_data()
            if dry_run:
                print(f"[DRY-RUN] {data}")
            else:
                future = producer.send(args.topic, value=data)
                # opcional: future.get(timeout=5)
                print(f"Sent: {data}")
            count += 1
            if args.once and count >= 5:
                print(f"[INFO] Modo --once: enviados {count} mensajes. Fin.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Productor detenido por usuario (Ctrl+C)")
    finally:
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    main()
