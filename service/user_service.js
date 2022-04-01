'use strict';

const { EventHubConsumerClient, earliestEventPosition } = require('@azure/event-hubs');
const { ContainerClient } = require('@azure/storage-blob');
const { BlobCheckpointStore } = require('@azure/eventhubs-checkpointstore-blob');

const connectionString = process.env.AZURE_EVENTHUB_CONNECTION_STRING;
const partition_key = process.env.AZURE_EVENTHUB_PARTITION_KEY;
const eventHubName = process.env.AZURE_EVENTHUB_NAME;
const storageConnectionString = process.env.AZURE_BLOB_CONNECTION_STRING;
const containerName = process.env.AZURE_BLOB_CONTAINER_NAME;
const consumerGroup = '$Default'; // name of the default consumer group
var consumerClient = null;
var subscription = null;

const start_subscription = async () => {
  // Create a consumer client for the event hub by specifying the checkpoint store.
  const containerClient = new ContainerClient(storageConnectionString, containerName);
  const checkpointStore = new BlobCheckpointStore(containerClient);
  consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName, checkpointStore);
  const props = await consumerClient.getEventHubProperties();
  console.log(props.name);
  console.log(props.partitionIds);
  console.log('Start subscription.....');
  subscription = consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(`No events received within wait time. Waiting for next interval`);
          return;
        }
        events.forEach((event) => {
          console.log(`Received event: '${event.body}' from partition: '${context.partitionId}' and consumer group: '${context.consumerGroup}'`);
          console.dir(event.body);
        });
        // Update the checkpoint.
        await context.updateCheckpoint(events[events.length - 1]);
        return;
      },
      processError: async (error, context) => {
        console.error(`Error : ${err}`);
      },
    },
    { startPosition: earliestEventPosition }
  );
};

const stop_subscription = async () => {
  await subscription.close();
  await consumerClient.close();
  console.log('All subscriptions closed and cleaned up. Good job!');
};

module.exports = {
  start_subscription,
  stop_subscription,
};
