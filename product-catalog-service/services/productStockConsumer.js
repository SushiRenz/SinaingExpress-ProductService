const amqp = require('amqplib');
const mongoose = require('mongoose');
const Product = require('../models/Product');

const RABBITMQ_URL = 'amqp://localhost'; // Or your RabbitMQ URL
const QUEUE = 'order_placed';

async function startConsumer() {
  await mongoose.connect('mongodb://localhost:27017/sinaing_express_products');

  const conn = await amqp.connect(RABBITMQ_URL);
  const channel = await conn.createChannel();
  await channel.assertQueue(QUEUE, { durable: true });

  console.log('Waiting for order_placed messages...');

  channel.consume(QUEUE, async (msg) => {
    if (msg !== null) {
      try {
        const order = JSON.parse(msg.content.toString());
        // order.items: [{ productId, quantity }]
        for (const item of order.items) {
          await Product.findByIdAndUpdate(
            item.productId,
            { $inc: { stock: -item.quantity } },
            { new: true }
          );
        }
        channel.ack(msg);
        console.log('Stock updated for order:', order._id || '[no id]');
      } catch (err) {
        console.error('Error processing order:', err);
        channel.nack(msg, false, false); // Discard message
      }
    }
  });
}

startConsumer().catch(console.error);