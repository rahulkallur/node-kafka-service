"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const kafka = new kafkajs_1.Kafka({
    clientId: `push-data-service-${Date.now()}`,
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVER_URL || "localhost:9092"],
});
const consumerData = kafka.consumer({
    groupId: "push-data-service-group",
});
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumerData.connect();
    console.info("Connected to Kafka Broker.");
    yield consumerData.subscribe({
        topic: process.env.SUBSCRIBE_TOPIC,
        fromBeginning: false,
    });
    yield consumerData.run({
        eachMessage: (_a) => __awaiter(void 0, [_a], void 0, function* ({ topic, partition, message }) {
            var _b;
            try {
                const payLoadParsed = JSON.parse(((_b = message.value) === null || _b === void 0 ? void 0 : _b.toString()) || "{}");
                console.log("Payload: ", payLoadParsed);
            }
            catch (e) {
                console.error("Error processing message:", e);
            }
        }),
    });
});
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
const errorTypes = [
    "SIGINT",
    "SIGTERM",
    "unhandledRejection",
];
errorTypes.forEach((type) => {
    process.on(type, (e) => __awaiter(void 0, void 0, void 0, function* () {
        console.log(`process.on ${type}`);
        console.error(e);
        process.exit(0);
    }));
});
