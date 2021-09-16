using System;
using System.Text;
using Azure.Storage.Blobs;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;

namespace event_hub_consumer
{
    class Program
    {
        private const string ehubNamespaceConnectionString = "Endpoint=sb://evhubtransactions.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Jh4cCZjKQriC0Sv3tx+EsedD+PZmgokENizkjO3QXzI=";

        private const string eventHubName = "eventransaction";

        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=blockblobbatch;AccountKey=0A/NKYALAOOAgWSNHCwkS2sV581RH7v9o+V0evu6Xmy5c/MS9m7/+h3vw+noGQbESpm3A7n802FL69uX5ZBDow==;EndpointSuffix=core.windows.net";

        private const string blobContainerName = "evhubcache";

        static BlobContainerClient storageClient;

        static EventProcessorClient processor;

        static async Task Main(string[] args)
        {

            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;


            while(true) 
            {
                await processor.StartProcessingAsync();
                await Task.Delay(TimeSpan.FromSeconds(30));
                await processor.StopProcessingAsync();            
            }
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
