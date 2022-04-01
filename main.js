'use strict';

require('dotenv').config();
const { start_subscription, stop_subscription } = require('./service/user_service');

const main = async () => {
  await start_subscription();
};

process.on('SIGTERM', async () => {
  console.log('PROCESS killed - SIGTERM');
  await stop_subscription();
  exit(1);
});
process.on('SIGINT', async () => {
  console.log('PROCESS killed - SIGINT');
  await stop_subscription();
  exit(1);
});

main().catch((err) => {
  console.error('ERROR occurred', err);
});
