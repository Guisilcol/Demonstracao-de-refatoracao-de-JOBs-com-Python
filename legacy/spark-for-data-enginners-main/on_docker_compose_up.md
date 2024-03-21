# Passo-a-passo de comandos a serem executados ao inicializar os containers

## Container Kafka

### Ao subir a primeira vez os containers do "docker-compose.yml"

Caso tenha subido os serviços do arquivo "docker-compose.yml" pela primeira vez é necessário fazer a criação do tópico "transactions". 

### Criação do tópico "transactions"
```kafka-topics --bootstrap-server localhost:9092 --topic transactions --create --partitions 3 --replication-factor 1```

### Visualização das mensagens que chegam no tópico "transactions"
```kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions```

