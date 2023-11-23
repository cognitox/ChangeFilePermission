using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;

namespace ChangeFilePermission
{
    internal class Program
    {
        private static string accountName { get; set; } = ""; //Stroage account name
        private static string accountKey { get; set; } = "";  //your storage account key;
        private static string fileSystemName { get; set; } = ""; //Container name
        private static string folderPath { get; set; } = ""; //path of the parent folder

        private static string userObjectId { get; set; } = ""; //User email or service principal id

        static void Main(string[] args)
        {
                 
            
            // Get the directory client object for the specified folder path
            var directoryClient = GetDataLakeServiceClient();


            // Set the updated access control list for the folder
            UpdateACLsRecursively(directoryClient, false);

            //updates ACL for a single directory
            //UpdateDirectoryACLs(directoryClient);


        }


        /// <summary>
        /// Caution
        /// Authorization with Shared Key is not recommended as it may be less secure.For optimal security, disable authorization via Shared Key for your storage account, as described in Prevent Shared Key authorization for an Azure Storage account.
        /// Use of access keys and connection strings should be limited to initial proof of concept apps or development prototypes that don't access production or sensitive data. Otherwise, the token-based authentication classes 
        /// available in the Azure SDK should always be preferred when authenticating to Azure resources.
        ///  Microsoft recommends that clients use either Microsoft Entra ID or a shared access signature (SAS) to authorize access to data in Azure Storage. For more information, see Authorize operations for data access.
        /// </summary>
        /// <returns></returns>
        public static DataLakeServiceClient GetDataLakeServiceClient()
        {
            StorageSharedKeyCredential sharedKeyCredential =
            new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = $"https://{accountName}.dfs.core.windows.net";

            DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient(
                new Uri(dfsUri),
                sharedKeyCredential);

            return dataLakeServiceClient;
        }


        public static void UpdateACLsRecursively(DataLakeServiceClient serviceClient, bool isDefaultScope)
        {
            DataLakeDirectoryClient directoryClient = serviceClient.GetFileSystemClient(fileSystemName).GetDirectoryClient(folderPath);

            List<PathAccessControlItem> accessControlListUpdate =  new List<PathAccessControlItem>()
            {
                new PathAccessControlItem(AccessControlType.User, RolePermissions.Read | RolePermissions.Write | 
                RolePermissions.Execute, isDefaultScope,
                entityId: userObjectId)
            };

            try
            {
                //Update ACLs
                //var result = directoryClient.UpdateAccessControlRecursive(accessControlListUpdate, null);
               

                directoryClient.SetPermissions(owner: userObjectId);
                var paths = directoryClient.GetPaths(true);

                foreach (var path in paths) { 
                    Console.WriteLine(path.Name);
                    var newdirectoryClient = serviceClient.GetFileSystemClient(fileSystemName).GetDirectoryClient(path.Name);

                    newdirectoryClient.SetPermissions(owner: userObjectId);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");
                throw;
            }

        }

        public static void UpdateDirectoryACLs(DataLakeServiceClient serviceClient)
        {
            
            DataLakeDirectoryClient directoryClient = serviceClient.GetFileSystemClient(fileSystemName).GetDirectoryClient(folderPath);


            PathAccessControl directoryAccessControl = directoryClient.GetAccessControl();

            List<PathAccessControlItem> accessControlListUpdate
                = (List<PathAccessControlItem>)directoryAccessControl.AccessControlList;

            int index = -1;

            foreach (var item in accessControlListUpdate)
            {
                if (item.AccessControlType == AccessControlType.Other)
                {
                    index = accessControlListUpdate.IndexOf(item);
                    break;
                }
            }

            if (index > -1)
            {
                accessControlListUpdate[index] = new PathAccessControlItem(AccessControlType.Other,
                RolePermissions.Read |
                RolePermissions.Execute);

                directoryClient.SetAccessControlList(accessControlListUpdate);
            }

        }
    }


    
}