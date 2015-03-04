using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Xml;
using DeployReports.Ssrs;
using Newtonsoft.Json;

namespace DeployReports
{
    class Program
    {
        static void Main(string[] args)
        {
            var configFilePath = args[0];
            var s = File.ReadAllText(configFilePath);
            var config = JsonConvert.DeserializeObject<dynamic>(s);

            var itemsPath = args[1];

            using (var rs = new ReportingService2010())
            {
                rs.Url = ((string)config.Url).TrimEnd('/') + "/ReportService2010.asmx";
                rs.Credentials = new NetworkCredential((string)config.UserName, (string)config.Password);

                Console.WriteLine("Start publish to {0}...", config.Url);

                var root = (string)config.RootFolder;
                CreateFolders(rs, root);

                var dataSourcesRoot = Path.Combine(root, (string)config.DataSource.Path).Replace("\\", "/");
                CreateFolders(rs, dataSourcesRoot);

                var dataSources = Directory.GetFiles(itemsPath, "*.rds");
                foreach (var dataSource in dataSources)
                {
                    Console.WriteLine("Publishing \"{0}\" to {1}...", Path.GetFileNameWithoutExtension(dataSource),
                        dataSourcesRoot);
                    
                    PublishDataSource(rs, dataSourcesRoot, dataSource, config);
                    
                    Console.WriteLine("Done");
                    Console.WriteLine();
                }

                var modelsRoot = Path.Combine(root, (string)config.Model.Path).Replace("\\", "/");
                CreateFolders(rs, modelsRoot);

                var models = Directory.GetFiles(itemsPath, "*.smdl");
                foreach (var modelFile in models)
                {
                    Console.WriteLine("Publishing \"{0}\" to {1}...", Path.GetFileNameWithoutExtension(modelFile),
                        modelsRoot);
                    var model = PublishModel(rs, modelsRoot, Path.GetFileNameWithoutExtension(modelFile),
                        modelFile, Path.ChangeExtension(modelFile, ".dsv"));
                    UpdateItemDataSource(rs, model.Path, dataSourcesRoot, modelsRoot);

                    Console.WriteLine("Done");
                    Console.WriteLine();
                }

                var reportsRoot = Path.Combine(root, (string)config.Report.Path).Replace("\\", "/");
                CreateFolders(rs, reportsRoot);

                var reports = Directory.GetFiles(itemsPath, "*.rdl");
                foreach (var reportFile in reports)
                {
                    Console.WriteLine("Publishing \"{0}\" to {1}...", Path.GetFileNameWithoutExtension(reportFile),
                        reportsRoot);
                    
                    var report = PublishReport(rs, reportsRoot, reportFile);
                    UpdateItemDataSource(rs, report.Path, dataSourcesRoot, modelsRoot);

                    Console.WriteLine("Done");
                    Console.WriteLine();
                }
            }
            
            Console.WriteLine();
            Console.WriteLine("Publish completed!");
            Console.ReadLine();
        }

        private static void UpdateItemDataSource(ReportingService2010 rs, string itemPath, string dataSourcesRoot, string modelsRoot)
        {
            var models = rs.ListChildren(modelsRoot, false)
                .ToDictionary(_ => _.Name, _ => _, StringComparer.OrdinalIgnoreCase);

            var dataSources = rs.GetItemDataSources(itemPath);
            var newDataSources = new List<DataSource>();
            foreach (var ds in dataSources)
            {
                if (models.ContainsKey(ds.Name))
                {
                    newDataSources.Add(new DataSource
                    {
                        Name = ds.Name,
                        Item = new DataSourceReference
                        {
                            Reference = modelsRoot + "/" + ds.Name
                        }
                    });
                }
                else if (ds.Item is InvalidDataSourceReference)
                {
                    newDataSources.Add(new DataSource
                    {
                        Name = ds.Name,
                        Item = new DataSourceReference
                        {
                            Reference = dataSourcesRoot + "/" + ds.Name
                        }
                    });
                }
                else
                {
                    newDataSources.Add(ds);
                }
            }
            rs.SetItemDataSources(itemPath, newDataSources.ToArray());
        }

        private static CatalogItem PublishModel(ReportingService2010 rs, string modelsRoot, string name, string localModelFile,
            string localDataSourceViewFile)
        {
            var absoluteModelPath = modelsRoot + "/" + name;
            if (rs.GetItemType(absoluteModelPath) == "Model")
            {
                rs.DeleteItem(absoluteModelPath);
            }

            var xml = new XmlDocument();
            xml.Load(localModelFile);
            var xml2 = new XmlDocument();
            xml2.Load(localDataSourceViewFile);
            Debug.Assert(xml.DocumentElement != null, "xml.DocumentElement != null");
            Debug.Assert(xml2.DocumentElement != null, "xml2.DocumentElement != null");
            xml.DocumentElement.AppendChild(xml.ImportNode(xml2.DocumentElement, true));

            using (var ms = new MemoryStream())
            using (var wr = XmlWriter.Create(ms))
            {
                xml.WriteTo(wr);
                wr.Flush();
                Warning[] warnings;
                var item = rs.CreateCatalogItem("Model", name, modelsRoot, false, ms.ToArray(), null, out warnings);

                if (warnings != null && warnings.Length > 0)
                {
                    foreach (var warning in warnings)
                    {
                        Console.WriteLine("[{0}] {1} {2}", warning.Severity, warning.Code, warning.Message);
                    }
                }

                return item;
            }
        }

        private static CatalogItem PublishDataSource(ReportingService2010 rs, string dataSourcesRoot, string localDataSourceFile, dynamic config)
        {
            var name = Path.GetFileNameWithoutExtension(localDataSourceFile);

            var definition = new DataSourceDefinition
            {
                CredentialRetrieval = CredentialRetrievalEnum.Store,
                ConnectString = (string)config.DataSource.ConnectionString,
                Enabled = true,
                EnabledSpecified = true,
                Extension = "SQL",
                WindowsCredentials = false,
                UserName = (string)config.DataSource.Username,
                Password = (string)config.DataSource.Password
            };

            var item = rs.CreateDataSource(name,
                dataSourcesRoot, true, definition, null);

            return item;
        }

        private static void CreateFolders(ReportingService2010 rs, string path)
        {
            var folders = path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var parentFolder = "";
            foreach (var folder in folders)
            {
                var itemType = rs.GetItemType(parentFolder + "/" + folder);
                if (itemType == "Folder")
                {
                    // Folder already exists
                    parentFolder += "/" + folder;
                    continue;
                }

                var item = rs.CreateFolder(folder, parentFolder == "" ? "/" : parentFolder, null);
                parentFolder = item.Path;
            }
        }

        private static CatalogItem PublishReport(ReportingService2010 rs, string reportsRoot, string localReportFile)
        {
            var reportName = Path.GetFileNameWithoutExtension(localReportFile);
            Warning[] warnings;

            var item = rs.CreateCatalogItem("Report", reportName, reportsRoot, true, File.ReadAllBytes(localReportFile), null, out warnings);

            if (warnings != null && warnings.Length > 0)
            {
                foreach (var warning in warnings)
                {
                    Console.WriteLine("[{0}] {1} {2}", warning.Severity, warning.Code, warning.Message);
                }
            }

            return item;
        }

        private static void PrintChildren(ReportingService2010 rs, string path, int depth)
        {
            var items = rs.ListChildren(path, false);

            foreach (var item in items)
            {
                var prefix = "";
                if (depth > 0)
                {
                    prefix = new string(' ', depth * 2) + "|-";
                }
                Console.WriteLine(prefix + item.Name + ":" + item.TypeName);

                if (item.TypeName == "Folder")
                {
                    PrintChildren(rs, item.Path, depth + 1);
                }
            }
        }
    }
}
