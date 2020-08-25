using System;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace attachments
{
    class Program
    {
        private static string cosmosAccount = "<Your_Azure_Cosmos_account_URI>";
        private static string cosmosKey = "<Your_Azure_Cosmos_account_PRIMARY_KEY>";
        private static string cosmosDatabaseName = "<Your_Azure_Cosmos_database>";
        private static string cosmosCollectionName = "<Your_Azure_Cosmos_collection>";
        private static string storageConnectionString = "<Your_Azure_Storage_connection_string>";
        private static string storageContainerName = "<Your_Azure_Storage_container_name>";
        private static DocumentClient cosmosClient = new DocumentClient(new Uri(cosmosAccount), cosmosKey);
        private static BlobServiceClient storageClient = new BlobServiceClient(storageConnectionString);
        private static BlobContainerClient storageContainerClient = storageClient.GetBlobContainerClient(storageContainerName);

        static void Main(string[] args)
        {
            RunScenarioAsync().Wait();
        }

        private static async Task RunScenarioAsync()
        {
            await InitializeAsync();

            while (true)
            {
                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine("Attachments Demo");
                Console.WriteLine("--------------------------------------------------------------------- ");
                Console.WriteLine("");
                Console.WriteLine("Press for demo scenario:\n");
                Console.WriteLine("1 - Upload Attachments");
                Console.WriteLine("2 - Download Attachments");
                Console.WriteLine("3 - Delete Attachments");
                Console.WriteLine("4 - Upload Blobs");
                Console.WriteLine("5 - Download Blobs");
                Console.WriteLine("6 - Delete Blobs");
                Console.WriteLine("7 - Copy Attachments to Blobs");
                Console.WriteLine("--------------------------------------------------------------------- ");

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D1:
                        await UploadAttachmentsAsync();
                        break;
                    case ConsoleKey.D2:
                        await DownloadAttachmentsAsync();
                        break;
                    case ConsoleKey.D3:
                        await DeleteAttachmentsAsync();
                        break;
                    case ConsoleKey.D4:
                        await UploadBlobsAsync();
                        break;
                    case ConsoleKey.D5:
                        await DownloadBlobsAsync();
                        break;
                    case ConsoleKey.D6:
                        await DeleteBlobsAsync();
                        break;
                    case ConsoleKey.D7:
                        await CopyAttachmentsToBlobsAsync();
                        break;
                    default:
                        Console.WriteLine("Select choice");
                        break;
                }
            }
        }

        private static async Task InitializeAsync()
        {
            Console.WriteLine("Initializing Cosmos Database: {0} ...", cosmosDatabaseName);

            await cosmosClient.CreateDatabaseIfNotExistsAsync(new Database { Id = cosmosDatabaseName });

            Console.WriteLine("Initializing Cosmos Container: {0} ...", cosmosCollectionName);

            DocumentCollection collection = await cosmosClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(cosmosDatabaseName),
                new DocumentCollection
                {
                    Id = cosmosCollectionName,
                    PartitionKey = new PartitionKeyDefinition { Paths = { "/id" } }
                },
                new RequestOptions
                {
                    OfferThroughput = 10000
                }
            );
        }
        private static async Task UploadAttachmentsAsync()
        {
            DirectoryInfo sourceDirectory = new DirectoryInfo("source");

            Console.WriteLine("Upserting parent documents ...");

            IList<Task> createParentDocumentTasks = new List<Task>();

            foreach (FileInfo file in sourceDirectory.EnumerateFiles())
            {
                createParentDocumentTasks.Add(
                    cosmosClient.UpsertDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(cosmosDatabaseName, cosmosCollectionName),
                        new { id = file.Name }
                    )
                );
            }

            await Task.WhenAll(createParentDocumentTasks);

            Console.WriteLine("Uploading attachments ...");

            IList<Task> uploadAttachmentTasks = new List<Task>();
            int totalCount = 0;

            foreach (FileInfo file in sourceDirectory.EnumerateFiles())
            {
                Console.WriteLine("Scheduled task to upload file: {0}", file.Name);
                uploadAttachmentTasks.Add(
                    cosmosClient.CreateAttachmentAsync(
                        UriFactory.CreateDocumentUri(cosmosDatabaseName, cosmosCollectionName, file.Name),
                        new FileStream(file.FullName, FileMode.Open, FileAccess.Read),
                        new MediaOptions { ContentType = "application/octet-stream", Slug = file.Name },
                        new RequestOptions { PartitionKey = new PartitionKey(file.Name) }
                    )
                );
                totalCount++;
            }

            await Task.WhenAll(uploadAttachmentTasks);

            Console.WriteLine("Finished uploading {0} attachments", totalCount);
        }

        private async static Task DownloadAttachmentsAsync()
        {
            Console.WriteLine("Clearing target directory ...");

            DirectoryInfo targetDirectory = new DirectoryInfo("target");

            foreach (FileInfo file in targetDirectory.EnumerateFiles())
            {
                file.Delete();
            }

            Console.WriteLine("Downloading attachments ...");

            int totalCount = 0;
            string docContinuation = null;

            do
            {
                FeedResponse<dynamic> response = await cosmosClient.ReadDocumentFeedAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDatabaseName, cosmosCollectionName),
                    new FeedOptions
                    {
                        MaxItemCount = -1,
                        RequestContinuation = docContinuation,
                        MaxDegreeOfParallelism = -1
                    });
                docContinuation = response.ResponseContinuation;

                foreach (Document document in response)
                {
                    string attachmentContinuation = null;
                    do
                    {
                        FeedResponse<Attachment> attachments = await cosmosClient.ReadAttachmentFeedAsync(
                            document.SelfLink,
                            new FeedOptions
                            {
                                PartitionKey = new PartitionKey(document.Id),
                                RequestContinuation = attachmentContinuation
                            }
                        );
                        attachmentContinuation = attachments.ResponseContinuation;

                        foreach (var attachment in attachments)
                        {
                            MediaResponse content = await cosmosClient.ReadMediaAsync(attachment.MediaLink);

                            byte[] buffer = new byte[content.ContentLength];
                            await content.Media.ReadAsync(buffer, 0, buffer.Length);
                            using (FileStream targetFileStream = File.OpenWrite(targetDirectory.FullName + "\\" + attachment.Id))
                            {
                                await targetFileStream.WriteAsync(buffer, 0, buffer.Length);
                            }

                            Console.WriteLine("Downloaded attachment: {0}", document.Id);
                            totalCount++;
                        }

                    } while (!string.IsNullOrEmpty(attachmentContinuation));
                }
            }
            while (!string.IsNullOrEmpty(docContinuation));

            Console.WriteLine("Finished downloading {0} attachments", totalCount);
        }

        private async static Task DeleteAttachmentsAsync()
        {
            Console.WriteLine("Deleting attachments ...");

            int totalCount = 0;
            string docContinuation = null;

            do
            {
                FeedResponse<dynamic> response = await cosmosClient.ReadDocumentFeedAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDatabaseName, cosmosCollectionName),
                    new FeedOptions
                    {
                        MaxItemCount = -1,
                        RequestContinuation = docContinuation,
                        MaxDegreeOfParallelism = -1
                    });
                docContinuation = response.ResponseContinuation;

                foreach (Document document in response)
                {
                    string attachmentContinuation = null;
                    PartitionKey docPartitionKey = new PartitionKey(document.Id);

                    do
                    {
                        FeedResponse<Attachment> attachments = await cosmosClient.ReadAttachmentFeedAsync(
                            document.SelfLink,
                            new FeedOptions
                            {
                                PartitionKey = docPartitionKey,
                                RequestContinuation = attachmentContinuation
                            }
                        );
                        attachmentContinuation = attachments.ResponseContinuation;

                        foreach (var attachment in attachments)
                        {
                            await cosmosClient.DeleteAttachmentAsync(
                                attachment.SelfLink,
                                new RequestOptions { PartitionKey = docPartitionKey }
                            );

                            Console.WriteLine("Deleted attachment: {0}", document.Id);
                            totalCount++;
                        }

                    } while (!string.IsNullOrEmpty(attachmentContinuation));
                }
            }
            while (docContinuation != null);

            Console.WriteLine("Finished deleting {0} attachments", totalCount);
        }

        private static async Task UploadBlobsAsync()
        {
            DirectoryInfo sourceDirectory = new DirectoryInfo("source");

            Console.WriteLine("Uploading blobs ...");

            IList<Task> uploadBlobTasks = new List<Task>();
            int totalCount = 0;

            foreach (FileInfo file in sourceDirectory.EnumerateFiles())
            {
                Console.WriteLine("Scheduled task to upload file: {0}", file.Name);
                uploadBlobTasks.Add(
                    storageContainerClient.GetBlobClient(file.Name).UploadAsync(
                        new FileStream(file.FullName, FileMode.Open, FileAccess.Read),
                        true
                    )
                );
                totalCount++;
            }

            await Task.WhenAll(uploadBlobTasks);

            Console.WriteLine("Finished uploading {0} blobs", totalCount);
        }

        private async static Task DownloadBlobsAsync()
        {
            Console.WriteLine("Clearing target directory ...");

            DirectoryInfo targetDirectory = new DirectoryInfo("target");

            foreach (FileInfo file in targetDirectory.EnumerateFiles())
            {
                file.Delete();
            }

            Console.WriteLine("Downloading blobs ...");

            int totalCount = 0;

            await foreach (BlobItem blobItem in storageContainerClient.GetBlobsAsync())
            {
                BlobDownloadInfo content = await storageContainerClient.GetBlobClient(blobItem.Name).DownloadAsync();

                using (FileStream targetFileStream = File.OpenWrite(targetDirectory.FullName + "\\" + blobItem.Name))
                {
                    await content.Content.CopyToAsync(targetFileStream);
                }

                Console.WriteLine("Downloaded blob: {0}", blobItem.Name);
                totalCount++;
            }

            Console.WriteLine("Finished downloading {0} attachments", totalCount);
        }

        private async static Task DeleteBlobsAsync()
        {
            Console.WriteLine("Deleting blobs ...");

            int totalCount = 0;

            await foreach (BlobItem blobItem in storageContainerClient.GetBlobsAsync())
            {
                await storageContainerClient.GetBlobClient(blobItem.Name).DeleteAsync();
                Console.WriteLine("Deleted blob: {0}", blobItem.Name);
                totalCount++;
            }

            Console.WriteLine("Finished deleting {0} blobs", totalCount);
        }

        private async static Task CopyAttachmentsToBlobsAsync()
        {
            Console.WriteLine("Copying Azure Cosmos DB attachments to Azure Blob storage...");

            int totalCount = 0;
            string docContinuation = null;

            // Iterate through each document (item in v3+) in the Azure Cosmos DB collection (container in v3+) to look for attachments.
            do
            {
                FeedResponse<dynamic> response = await cosmosClient.ReadDocumentFeedAsync(
                    UriFactory.CreateDocumentCollectionUri(cosmosDatabaseName, cosmosCollectionName),
                    new FeedOptions
                    {
                        MaxItemCount = -1,
                        RequestContinuation = docContinuation,
                        MaxDegreeOfParallelism = -1
                    });
                docContinuation = response.ResponseContinuation;

                foreach (Document document in response)
                {
                    string attachmentContinuation = null;

                    // Iterate through each attachment within the document (if any).
                    do
                    {
                        FeedResponse<Attachment> attachments = await cosmosClient.ReadAttachmentFeedAsync(
                            document.SelfLink,
                            new FeedOptions
                            {
                                PartitionKey = new PartitionKey(document.Id),
                                RequestContinuation = attachmentContinuation
                            }
                        );
                        attachmentContinuation = attachments.ResponseContinuation;

                        foreach (var attachment in attachments)
                        {
                            // Download the attachment in to local memory.
                            MediaResponse content = await cosmosClient.ReadMediaAsync(attachment.MediaLink);

                            byte[] buffer = new byte[content.ContentLength];
                            await content.Media.ReadAsync(buffer, 0, buffer.Length);

                            // Upload the locally buffered attachment to blob storage
                            string blobId = String.Concat(document.Id,"-",attachment.Id);

                            Azure.Response<BlobContentInfo> uploadedBob = await storageContainerClient.GetBlobClient(blobId).UploadAsync(
                                new MemoryStream(buffer, writable: false),
                                true
                            );

                            Console.WriteLine("Copied attachment ... Document Id: {0} , Attachment Id: {1}, Blob Id: {2}", document.Id, attachment.Id, blobId);
                            totalCount++;
                        }

                    } while (!string.IsNullOrEmpty(attachmentContinuation));
                }
            }
            while (!string.IsNullOrEmpty(docContinuation));

            Console.WriteLine("Finished copying {0} attachments to blob storage", totalCount);
        }
    }
}
