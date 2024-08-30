import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import dotenv from "dotenv";

dotenv.config();

const kafka = new Kafka({
  clientId: `push-data-service-${Date.now()}`,
  brokers: [process.env.KAFKA_BOOTSTRAP_SERVER_URL || "localhost:9092"],
});

const consumerData: Consumer = kafka.consumer({
  groupId: "push-data-service-group",
});

const run = async () => {
  await consumerData.connect();
  console.info("Connected to Kafka Broker.");
  await consumerData.subscribe({
    topic: process.env.SUBSCRIBE_TOPIC!,
    fromBeginning: false,
  });

  await consumerData.run({
    eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
      try {
        const payLoadParsed = JSON.parse(message.value?.toString() || "{}");
        console.log("Payload: ", payLoadParsed);
      } catch (e) {
        console.error("Error processing message:", e);
      }
    },
  });
};

run().catch((error) => console.error("run error:", error));

consumerData.on("consumer.crash", () => {
  console.log("Crash detected");
  process.exit(0);
});

consumerData.on("consumer.disconnect", () => {
  console.log("Disconnect detected");
  process.exit(0);
});

consumerData.on("consumer.stop", () => {
  console.log("Stop detected");
  process.exit(0);
});

const errorTypes: Array<NodeJS.Signals | "unhandledRejection"> = [
  "SIGINT",
  "SIGTERM",
  "unhandledRejection",
];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    console.log(`process.on ${type}`);
    console.error(e);
    process.exit(0);
  });
});
