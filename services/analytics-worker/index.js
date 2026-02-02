const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');

// 1. Connect to MongoDB (Required for this worker to function)
mongoose.connect('mongodb://localhost:27017/urlshortener')
  .then(() => console.log('Worker Connected to MongoDB'))
  .catch(err => console.error('Mongo Error', err));

// 2. Define Schema (Must match the one in url-service)
const URLSchema = new mongoose.Schema({
  originalUrl: String,
  shortId: String,
  clicks: { type: Number, default: 0 }
});
const URL = mongoose.model('URL', URLSchema);

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'analytics-group' });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'click-events', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const { shortId } = JSON.parse(message.value.toString());
        
        // Update Click Count
        await URL.updateOne(
          { shortId },
          { $inc: { clicks: 1 } }
        );
        
        console.log(`Processed click for ${shortId}`);
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
}
run();