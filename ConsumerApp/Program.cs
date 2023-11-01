﻿using Confluent.Kafka;

namespace ConsumerApp
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            string bootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"; // Replace with your Kafka broker address
            string topic = "test"; // Replace with the Kafka topic you want to use

            // Producer configuration
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = "ClientGateway",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "5RXBAQHMCXYVY5X5",
                SaslPassword = "+zExfsPWK8S1rz69k3Eqnc0OGTtvdwOhWgxP0iPt1S3Z0AdIPUrz8NabEUKUMp2j",
            };

            // Consumer configuration
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "kafka-demo", // Replace with your consumer group name
                AutoOffsetReset = AutoOffsetReset.Earliest, // Set to 'Latest' or 'None' if needed
                ClientId = "ClientGateway",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "5RXBAQHMCXYVY5X5",
                SaslPassword = "+zExfsPWK8S1rz69k3Eqnc0OGTtvdwOhWgxP0iPt1S3Z0AdIPUrz8NabEUKUMp2j",
                ApiVersionRequestTimeoutMs = 1000,
            };

            //Create a Kafka producer
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                // Produce a message
                var message = new Message<Null, string> { Value = "Hello, Kafka!" };
                var deliveryResult = await producer.ProduceAsync(topic, message);

                Console.WriteLine($"Produced message to: {deliveryResult.TopicPartitionOffset}");
            }

            // Create a Kafka consumer
        }
    }
}