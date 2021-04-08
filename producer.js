const { Kafka } = require('kafkajs');

const args = process.argv.slice(2);
if (args.length != 1) {
    console.error('Usage: producer.js PRODUCER_NAME');
    process.exit();
}
const PRODUCER_NAME = args[0];
const TOPIC_NAME = 'test-topic';
const KAFKA_BROKERS = ['localhost:9092'];

const LEVELS = [ 'NOTHING', 'ERROR', 'WARN', 'INFO', 'DEBUG' ];

const customLogCreator = logLevel => ({ namespace, level, label, log }) => {
    console.log(`[${LEVELS[level]}] [${namespace}] ${log.message}`);
};

const kafka = new Kafka({
    clientId: PRODUCER_NAME,
    brokers: KAFKA_BROKERS,
    logCreator: customLogCreator,
});

const producer = kafka.producer();

const run = async () => {
    await producer.connect();

    setInterval(async () => {
        const data = `${PRODUCER_NAME} send at ${(new Date()).toLocaleString()}`;
        const message = {
            //key: Date.now().toString(),
            value: data,
        };
        console.log(message);
        await producer.send({
            topic: TOPIC_NAME,
            messages: [message],
        });
    }, 3000);
}

run().catch(console.error);
