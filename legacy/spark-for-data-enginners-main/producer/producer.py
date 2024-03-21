from kafka import KafkaProducer
from datetime import datetime
import random
import uuid
import time 
import json
import asyncio

class MockData(object):

    @staticmethod
    def generate_id():
        return str(uuid.uuid4())

    @staticmethod
    def generate_user_id():
        return random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    @staticmethod
    def generate_agence():
        return random.choice(['Agência 1', 'Agência 2', 'Agência 3', 'Agência 4', 'Agência 5', 'Agência 6', 'Agência 7', 'Agência 8', 'Agência 9', 'Agência 10'])

    @staticmethod
    def generate_operation_value():
        return random.randint(1, 10000)

    @staticmethod
    def generate_operation_type():
        return random.choice(['Depósito', 'Saque'])

    @staticmethod
    def generate_date():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def generate_account_balance():
        return random.randint(1, 10000)

async def main():
    """
    Spark for Data Enginner - Impacta

    Sumário: Script responsável por gerar os dados e enviar para o Kafka 
    """
    
    kafka_bootstrap_server = "localhost:9092"
    topic_name = 'transactions' 
    key = None

    producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_server])

    while True:
        try:
            time.sleep(1)

            mock_data = {
                'id': MockData.generate_id(),
                'user_id': MockData.generate_user_id(),
                'agence': MockData.generate_agence(),
                'operation_value': MockData.generate_operation_value(),
                'operation_type': MockData.generate_operation_type(),
                'date': MockData.generate_date(),
                'account_balance': MockData.generate_account_balance()
            }

            raw_data = json.dumps(mock_data).encode('utf-8')

            print(f"Sending data of transaction: {mock_data['id']}")
            producer.send(topic_name, key=key, value=raw_data)
            producer.flush()

        except Exception as e:
            print(f"Error: {e}")
            break

if __name__ == "__main__":
    asyncio.run(main())