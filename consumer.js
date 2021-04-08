const { Kafka } = require('kafkajs');

const args = process.argv.slice(2);
if (args.length != 1) {
    console.error('Usage: consumer.js CONSUMER_NAME');
    process.exit();
}
const CONSUMER_NAME = args[0];
const TOPIC_NAME = 'test-topic';
const KAFKA_BROKERS = ['localhost:9092'];

const LEVELS = [ 'NOTHING', 'ERROR', 'WARN', 'INFO', 'DEBUG' ];

const customLogCreator = logLevel => ({ namespace, level, label, log }) => {
    console.log(`[${LEVELS[level]}] [${namespace}] ${log.message}`);
};

const kafka = new Kafka({
    clientId: CONSUMER_NAME,
    brokers: KAFKA_BROKERS,
    logCreator: customLogCreator,
});

const consumer = kafka.consumer({ groupId: 'my-consumers-group' });

const run = async () => {
    await consumer.connect();

    process.on('SIGINT', async () => {
        console.log('Received SIGINT. Disconnecting');
        await consumer.disconnect();
    });

    await consumer.subscribe({
        topic: TOPIC_NAME,
        fromBeginning: true,
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    });
};

run().catch(console.error);
