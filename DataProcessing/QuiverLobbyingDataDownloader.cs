/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataSource;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// QuiverLobbyingDataDownloader implementation.
    /// </summary>
    public class QuiverLobbyingDataDownloader : IDisposable
    {
        public const string VendorName = "quiver";
        public const string VendorDataName = "lobbying";
        
        private readonly string _destinationFolder;
        private readonly string _universeFolder;
        private readonly string _processedDataDirectory;
        private readonly string _clientKey;
        private readonly string _dataFolder = Globals.DataFolder;
        private readonly bool _canCreateUniverseFiles;
        private readonly int _maxRetries = 5;
        
        private readonly JsonSerializerSettings _jsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        /// <summary>
        /// Control the rate of download per unit of time.
        /// </summary>
        private readonly RateGate _indexGate;

        /// <summary>
        /// Creates a new instance of <see cref="QuiverLobbying"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="processedDataDirectory">The folder where the data will be read from</param>
        /// <param name="apiKey">The Vendor API key</param>
        public QuiverLobbyingDataDownloader(string destinationFolder, string processedDataDirectory, string apiKey = null)
        {
            _destinationFolder = Path.Combine(destinationFolder, VendorName, VendorDataName);
            _universeFolder = Path.Combine(_destinationFolder, "universe");
            _processedDataDirectory = Path.Combine(processedDataDirectory, VendorName, VendorDataName);

            _clientKey = apiKey ?? Config.Get("quiver-auth-token");
            _canCreateUniverseFiles = Directory.Exists(Path.Combine(_dataFolder, "equity", "usa", "map_files"));

            _indexGate = new RateGate(100, TimeSpan.FromSeconds(60));

            Directory.CreateDirectory(_universeFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <param name="processDate">date of data to be processed</param>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run(DateTime processDate)
        {
            var stopwatch = Stopwatch.StartNew();
            Log.Trace($"QuiverLobbyingDataDownloader.Run(): Start downloading/processing QuiverQuant Lobbying data");

            var today = DateTime.UtcNow.Date;
            try
            {
                if (processDate >= today || processDate == DateTime.MinValue)
                {
                    Log.Trace($"Encountered data from invalid date: {processDate:yyyy-MM-dd} - Skipping");
                    return false;
                }

                var quiverLobbyingData = HttpRequester($"live/lobbying?date_from={processDate:yyyyMMdd}&date_to={processDate:yyyyMMdd}").SynchronouslyAwaitTaskResult();
                if (string.IsNullOrWhiteSpace(quiverLobbyingData))
                {
                    // We've already logged inside HttpRequester
                    return false;
                }

                var lobbyingByDate = JsonConvert.DeserializeObject<List<RawLobbying>>(quiverLobbyingData, _jsonSerializerSettings);

                var recentLobbies = JsonConvert.DeserializeObject<List<RawLobbying>>(result, _jsonSerializerSettings);
                var csvContents = new List<string>();
                                            
                var lobbyingByTicker = new Dictionary<string, List<string>>();
                var universeCsvContents = new List<string>();

                var mapFileProvider = new LocalZipMapFileProvider();
                mapFileProvider.Initialize(new DefaultDataProvider());

                foreach (var lobbying in lobbyingByDate)
                {
                    var ticker = lobbying.Ticker.ToUpperInvariant();

                    if (!lobbyingByTicker.TryGetValue(ticker, out var _))
                    {
                        lobbyingByTicker.Add(ticker, new List<string>());
                    }
                    
                    // subtracting one day as the start time of the data. since the Date attribute in the JSON is the consolidated time of the data
                    var dateTime = lobby.Date.AddDays(-1);

                    var date = $"{dateTime:yyyyMMdd}";
                    var issue = lobby.Issue == null ? null : lobby.Issue.Replace("\n", " ").Replace(",", " ").Trim();
                    var specificIssue = lobby.SpecificIssue == null ? null : lobby.SpecificIssue.Replace("\n", " ").Replace(",", " ").Trim();

                    var curRow = $"{lobby.Client},{issue},{specificIssue},{lobby.Amount}";
                    lobbyingByTicker[ticker].Add($"{date},{curRow}");

                    var sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, true, mapFileProvider, date);
                    universeCsvContents.Add($"{sid},{ticker},{curRow}");
                }

                if (!_canCreateUniverseFiles)
                {
                    return false;
                }
                else if (universeCsvContents.Any())
                {
                    SaveContentToFile(_universeFolder, $"{processDate:yyyyMMdd}", universeCsvContents);
                }

                lobbyingByTicker.DoForEach(kvp => SaveContentToFile(_destinationFolder, kvp.Key, kvp.Value));
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"QuiverLobbyingDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }

        /// <summary>
        /// Sends a GET request for the provided URL
        /// </summary>
        /// <param name="url">URL to send GET request for</param>
        /// <returns>Content as string</returns>
        /// <exception cref="Exception">Failed to get data after exceeding retries</exception>
        private async Task<string> HttpRequester(string url)
        {
            for (var retries = 1; retries <= _maxRetries; retries++)
            {
                try
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri("https://api.quiverquant.com/beta/");
                        client.DefaultRequestHeaders.Clear();

                        // You must supply your API key in the HTTP header,
                        // otherwise you will receive a 403 Forbidden response
                        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _clientKey);

                        // Responses are in JSON: you need to specify the HTTP header Accept: application/json
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                        
                        // Makes sure we don't overrun Quiver rate limits accidentally
                        _indexGate.WaitToProceed();

                        var response = await client.GetAsync(Uri.EscapeUriString(url));
                        if (response.StatusCode == HttpStatusCode.NotFound)
                        {
                            Log.Error($"QuiverLobbyingDataDownloader.HttpRequester(): Files not found at url: {Uri.EscapeUriString(url)}");
                            response.DisposeSafely();
                            return string.Empty;
                        }

                        if (response.StatusCode == HttpStatusCode.Unauthorized)
                        {
                            var finalRequestUri = response.RequestMessage.RequestUri; // contains the final location after following the redirect.
                            response = client.GetAsync(finalRequestUri).Result; // Reissue the request. The DefaultRequestHeaders configured on the client will be used, so we don't have to set them again.
                        }

                        response.EnsureSuccessStatusCode();

                        var result =  await response.Content.ReadAsStringAsync();
                        response.DisposeSafely();

                        return result;
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, $"QuiverLobbyingDataDownloader.HttpRequester(): Error at HttpRequester. (retry {retries}/{_maxRetries})");
                    Thread.Sleep(1000);
                }
            }

            throw new Exception($"Request failed with no more retries remaining (retry {_maxRetries}/{_maxRetries})");
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="name">file name</param>
        /// <param name="contents">Contents to write</param>
        private void SaveContentToFile(string destinationFolder, string name, IEnumerable<string> contents)
        {
            name = name.ToLowerInvariant();
            var finalPath = Path.Combine(destinationFolder, $"{name}.csv");
            string filePath;

            if (destinationFolder.Contains("universe"))
            {
                filePath = Path.Combine(_processedDataDirectory, "universe", $"{name}.csv");
            }
            else
            {
                filePath = Path.Combine(_processedDataDirectory, $"{name}.csv");
            }

            var finalFileExists = File.Exists(filePath);

            var lines = new HashSet<string>(contents);
            if (finalFileExists)
            {
                foreach (var line in File.ReadAllLines(filePath))
                {
                    lines.Add(line);
                }
            }

            var finalLines = destinationFolder.Contains("universe") ? 
                lines.OrderBy(x => x.Split(',').First()).ToList() :
                lines
                .OrderBy(x => DateTime.ParseExact(x.Split(',').First(), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal))
                .ToList();

            File.WriteAllLines(finalPath, finalLines);
        }

        private class RawLobbying : QuiverLobbying
        {
            /// <summary>
            /// The date of the data being consolidated and received
            /// </summary>
            [JsonProperty(PropertyName = "Date")]
            [JsonConverter(typeof(DateTimeJsonConverter), "yyyy-MM-dd")]
            public DateTime Date { get; set; }

            /// <summary>
            /// The ticker/symbol for the company
            /// </summary>
            [JsonProperty(PropertyName = "Ticker")]
            public string Ticker { get; set; }
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            _indexGate?.Dispose();
        }
    }
}
