# azure-queue-peek-and-query
An application to peek messages from Azure Service Bus queues, and query the resulting table

## Features

- Type the queue-name, and choose the environment (corresponding to the service bus connection strings added in the appsettings.json)
- Choose to peek from either the queue or the dead-letter queue
- Choose to view either the full message column or just the truncated form of it
- Can export the messages peeked as an excel file
- Can query the peeked messages using an SQL language (DuckDB SQL syntax: https://duckdb.org/docs/stable/sql/introduction.html)

## Application UI

![image](https://github.com/user-attachments/assets/34bc2a44-a71d-4836-8f1e-c71943bd3223)
