
import axios from 'axios';
import { ServiceBusAdministrationClient, ServiceBusClient } from '@azure/service-bus';
import { CatFact } from './models/catFact';
import { CatFactExtended } from './models/catFactExtended';

const API_URL = 'https://catfact.ninja/facts';

// Replace with your API and Azure Service Bus details
const SERVICE_BUS_CONNECTION_STRING = process.env.SERVICEBUS_CONNECTION_STRING || '<REPLACE_WITH_YOUR_CONNECTION_STRING>';
const SERVICE_BUS_QUEUE_NAME = process.env.QUEUE_NAME || '<REPLACE_WITH_YOUR_QUEUE_NAME>';

const serviceBusClient = new ServiceBusClient(SERVICE_BUS_CONNECTION_STRING);
const serviceBusQueueSender = serviceBusClient.createSender(SERVICE_BUS_QUEUE_NAME);

/**
 * Fetches data from an API in batches.
 * @param batchSize The size of each batch.
 * @param page The page number to fetch.
 * @returns The fetched data.
 */
async function fetchDataInBatches(batchSize: number, page: number) {
  const response = await axios.get(`${API_URL}?limit=${batchSize}&page=${page}`);
  return response.data.data;
}

/**
 * Transforms the data by adding a dateFetched property to each item.
 * @param data The data to transform.
 * @returns The transformed data.
 */
function transformData(data: Array<CatFact>): Array<CatFactExtended> {
  return data.map((catFact) => ({ ...catFact, dateFetched: new Date() } as CatFactExtended));
}

/**
 * Creates a queue in Azure Service Bus if it does not already exist.
 */
async function createQueueIfNotExists() {
  const serviceBusAdministrationClient = new ServiceBusAdministrationClient(SERVICE_BUS_CONNECTION_STRING);

  try {
    await serviceBusAdministrationClient.getQueue(SERVICE_BUS_QUEUE_NAME);
  } catch (error: any) {
    if (error.code === "MessageEntityNotFoundError") {
      const createQueueResponse = await serviceBusAdministrationClient.createQueue(SERVICE_BUS_QUEUE_NAME);
      console.log("Created queue with name - ", createQueueResponse.name);
    } else {
      console.error('Unknown error:', error);
    }
  }
}

/**
 * Sends the transformed data to an Azure Service Bus.
 * @param data The data to send.
 */
async function sendDataToServiceBus(data: any) {
  await serviceBusQueueSender.sendMessages({ body: JSON.stringify(data) });
}

/**
 * Fetches, transforms, and sends data in batches to Azure Service Bus.
 */
async function main() {
  await createQueueIfNotExists();

  let page = 1;
  const batchSize = 200;

  while (true) {
    const data = await fetchDataInBatches(batchSize, page);
    if (data.length === 0) break;

    const transformedData = transformData(data);
    await sendDataToServiceBus(transformedData);

    page++;
  }

  await serviceBusClient.close();
}

main()
  .catch(console.error)
  .finally(async () => {
    await serviceBusQueueSender.close();
  });