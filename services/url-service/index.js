const express = require('express');
const { Kafka } = require('kafkajs');
const Redis = require('ioredis');
const mongoose = require('mongoose');
const { nanoid } = require('nanoid');

// Add this right after your imports
mongoose.connect('mongodb://localhost:27017/urlshortener')
  .then(() => console.log('Connected to MongoDB'));

const app = express();
app.use(express.json());

const redis = new Redis(6379, 'localhost');
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();

// URL Schema
const URLSchema = new mongoose.Schema({
  originalUrl: String,
  shortId: String,
  clicks: { type: Number, default: 0 }
});
const URL = mongoose.model('URL', URLSchema);

app.post('/shorten', async (req, res) => {
  const { originalUrl } = req.body;
  const shortId = nanoid(8);
  
  await URL.create({ originalUrl, shortId });
  await redis.set(shortId, originalUrl); // Cache for fast redirect
  
  res.json({ shortUrl: `http://localhost:3000/${shortId}` });
});

app.get('/:shortId', async (req, res) => {
  const { shortId } = req.params;
  let url = await redis.get(shortId);

  if (!url) {
    const doc = await URL.findOne({ shortId });
    if (!doc) return res.sendStatus(404);
    url = doc.originalUrl;
  }

  // Fire-and-forget click event to Kafka
  await producer.connect();
  await producer.send({
    topic: 'click-events',
    messages: [{ value: JSON.stringify({ shortId, timestamp: Date.now() }) }],
  });

  res.redirect(url);
});

app.listen(3000, () => console.log('URL Service running on 3000'));