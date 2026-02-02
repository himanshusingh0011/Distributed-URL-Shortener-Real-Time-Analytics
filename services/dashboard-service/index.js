const express = require('express');
const { Kafka } = require('kafkajs');
const app = express();

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'dashboard-group' });
app.use(express.static('public')); // Serve the HTML file

app.get('/events', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  await consumer.connect();
  await consumer.subscribe({ topic: 'click-events' });

  await consumer.run({
    eachMessage: async ({ message }) => {
      res.write(`data: ${message.value.toString()}\n\n`);
    },
  });
});

app.listen(4000, () => console.log('Dashboard stream on 4000'));